import csv
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime


# ============================================================
# CONFIGURATION
# ============================================================

BASE_URL = "https://api.mightpulse.com/v1"

FIRST_KINGDOM = 1
LAST_KINGDOM = 2481

ALLIANCES_PER_KINGDOM = 100

# Stay below the documented 60 requests/minute limit.
MIN_SECONDS_BETWEEN_REQUESTS = 1.05

# Stop before the documented 5,000 requests/day limit.
SAFE_DAILY_LIMIT = 4900

# Persistent storage inside the Docker volume.
DATA_DIR = "/app/data"

PROGRESS_FILE = os.path.join(
    DATA_DIR,
    "kingshot_progress.json"
)

KEY_STATE_FILE = os.path.join(
    DATA_DIR,
    "kingshot_key_state.json"
)

OUTPUT_FILE = os.path.join(
    DATA_DIR,
    "kingshot_players.csv"
)

ERROR_FILE = os.path.join(
    DATA_DIR,
    "kingshot_errors.log"
)

DISCORD_WEBHOOK_URL = os.getenv(
    "DISCORD_WEBHOOK_URL",
    ""
)


# ============================================================
# API KEYS
# ============================================================

API_KEYS = [
    key.strip()
    for key in os.getenv(
        "API_KEYS",
        ""
    ).split(",")
    if key.strip()
]


# ============================================================
# GLOBAL STATE
# ============================================================

current_key_index = 0

requests_today = []

day_started = time.time()

last_request_time = 0.0

current_kingdom = FIRST_KINGDOM


# ============================================================
# DIRECTORY
# ============================================================

os.makedirs(DATA_DIR, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

def log_error(message):
    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    text = (
        f"[{timestamp}] "
        f"{message}"
    )

    print(text)

    try:
        with open(
            ERROR_FILE,
            "a",
            encoding="utf-8"
        ) as f:
            f.write(text + "\n")
    except Exception:
        pass


# ============================================================
# DISCORD
# ============================================================

def send_discord(message):
    if not DISCORD_WEBHOOK_URL:
        return

    payload = json.dumps(
        {
            "content": message
        }
    ).encode("utf-8")

    request = urllib.request.Request(
        DISCORD_WEBHOOK_URL,
        data=payload,
        headers={
            "Content-Type": "application/json"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=30
        ):
            pass

    except Exception as e:
        log_error(
            f"Discord webhook error: {e}"
        )


# ============================================================
# KEY STATE
# ============================================================

def initialize_key_state():
    global requests_today

    requests_today = [0] * len(API_KEYS)


def save_key_state():
    state = {
        "current_key_index": current_key_index,
        "requests_today": requests_today,
        "day_started": day_started
    }

    try:
        with open(
            KEY_STATE_FILE,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                state,
                f,
                indent=2
            )
    except Exception as e:
        log_error(
            f"Could not save key state: {e}"
        )


def load_key_state():
    global current_key_index
    global requests_today
    global day_started

    if not os.path.exists(
        KEY_STATE_FILE
    ):
        initialize_key_state()
        return

    try:
        with open(
            KEY_STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:
            state = json.load(f)

        saved_index = int(
            state.get(
                "current_key_index",
                0
            )
        )

        saved_counts = state.get(
            "requests_today",
            []
        )

        saved_day = float(
            state.get(
                "day_started",
                time.time()
            )
        )

        if 0 <= saved_index < len(API_KEYS):
            current_key_index = saved_index
        else:
            current_key_index = 0

        if len(saved_counts) == len(API_KEYS):
            requests_today = [
                int(x)
                for x in saved_counts
            ]
        else:
            initialize_key_state()

        day_started = saved_day

    except Exception as e:
        log_error(
            f"Could not load key state: {e}"
        )

        initialize_key_state()


def reset_daily_state():
    global requests_today
    global day_started
    global current_key_index

    requests_today = [
        0
        for _ in API_KEYS
    ]

    day_started = time.time()

    current_key_index = 0

    save_key_state()

    print()
    print(
        "New daily API period detected."
    )
    print(
        "API key counters have been reset."
    )
    print()


# ============================================================
# PROGRESS
# ============================================================

def save_progress(next_kingdom):
    try:
        with open(
            PROGRESS_FILE,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                {
                    "next_kingdom": next_kingdom
                },
                f,
                indent=2
            )
    except Exception as e:
        log_error(
            f"Could not save progress: {e}"
        )


def load_progress():
    if not os.path.exists(
        PROGRESS_FILE
    ):
        return FIRST_KINGDOM

    try:
        with open(
            PROGRESS_FILE,
            "r",
            encoding="utf-8"
        ) as f:
            state = json.load(f)

        kingdom = int(
            state.get(
                "next_kingdom",
                FIRST_KINGDOM
            )
        )

        if kingdom < FIRST_KINGDOM:
            return FIRST_KINGDOM

        if kingdom > LAST_KINGDOM:
            return LAST_KINGDOM + 1

        return kingdom

    except Exception as e:
        log_error(
            f"Could not load progress: {e}"
        )

        return FIRST_KINGDOM


# ============================================================
# RATE LIMITING
# ============================================================

def wait_for_rate_limit():
    global last_request_time

    elapsed = (
        time.time()
        - last_request_time
    )

    if elapsed < MIN_SECONDS_BETWEEN_REQUESTS:
        time.sleep(
            MIN_SECONDS_BETWEEN_REQUESTS
            - elapsed
        )

    last_request_time = time.time()


# ============================================================
# API KEY SWITCHING
# ============================================================

def switch_to_next_key(reason):
    global current_key_index

    old_key = current_key_index + 1

    if (
        current_key_index + 1
        >= len(API_KEYS)
    ):
        message = (
            "All configured API keys "
            "have reached their limit. "
            f"Stopped at kingdom "
            f"{current_kingdom}."
        )

        print()
        print(message)

        send_discord(
            f"Player Scanner stopped.\n"
            f"Reason: all API keys exhausted.\n"
            f"Last kingdom: {current_kingdom}"
        )

        save_key_state()
        save_progress(current_kingdom)

        raise SystemExit(0)

    current_key_index += 1

    print()
    print(
        f"Switching API key "
        f"{old_key} -> "
        f"{current_key_index + 1}"
    )
    print(
        f"Reason: {reason}"
    )
    print()

    send_discord(
        f"Player Scanner switched API key "
        f"{old_key} -> "
        f"{current_key_index + 1}.\n"
        f"Kingdom: {current_kingdom}"
    )

    save_key_state()


# ============================================================
# API REQUEST
# ============================================================

def api_get(path):
    global requests_today

    while True:

        if not API_KEYS:
            print(
                "ERROR: No API keys configured."
            )

            send_discord(
                "Player Scanner stopped: "
                "no API keys configured."
            )

            raise SystemExit(1)

        # Reset our local 24-hour timer.
        if (
            time.time() - day_started
            >= 86400
        ):
            reset_daily_state()

        # Stay below the official daily limit.
        if (
            requests_today[
                current_key_index
            ]
            >= SAFE_DAILY_LIMIT
        ):
            switch_to_next_key(
                "local daily safety limit"
            )

            continue

        wait_for_rate_limit()

        key = API_KEYS[
            current_key_index
        ]

        url = (
            BASE_URL
            + path
        )

        request = urllib.request.Request(
            url,
            headers={
                "Authorization":
                    f"Bearer {key}",
                "Accept":
                    "application/json",
                "User-Agent":
                    "PlayerScanner/1.0"
            },
            method="GET"
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=90
            ) as response:

                body = (
                    response
                    .read()
                    .decode("utf-8")
                )

                requests_today[
                    current_key_index
                ] += 1

                save_key_state()

                return json.loads(body)

        except urllib.error.HTTPError as e:

            requests_today[
                current_key_index
            ] += 1

            save_key_state()

            if e.code == 401:

                switch_to_next_key(
                    "API key returned 401"
                )

                continue

            if e.code == 429:

                switch_to_next_key(
                    "API key returned 429"
                )

                continue

            body = ""

            try:
                body = (
                    e.read()
                    .decode(
                        "utf-8",
                        errors="replace"
                    )
                )
            except Exception:
                pass

            log_error(
                f"HTTP {e.code}: {path}"
            )

            if body:
                log_error(
                    body[:1000]
                )

            return None

        except Exception as e:

            log_error(
                f"Request failed: "
                f"{path} | {e}"
            )

            # Wait before retrying.
            time.sleep(5)

            return None


# ============================================================
# CSV
# ============================================================

CSV_FIELDS = [
    "kid",
    "aid",
    "alliance_tag",
    "alliance_name",
    "uid",
    "governor_id",
    "nick_name",
    "power",
    "town_center_level",
    "kills",
    "alliance_rank",
    "alliance_rank_label",
]


def ensure_csv():
    if os.path.exists(
        OUTPUT_FILE
    ):
        return

    with open(
        OUTPUT_FILE,
        "w",
        newline="",
        encoding="utf-8-sig"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=CSV_FIELDS
        )

        writer.writeheader()


def append_players(players):

    if not players:
        return

    with open(
        OUTPUT_FILE,
        "a",
        newline="",
        encoding="utf-8-sig"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=CSV_FIELDS,
            extrasaction="ignore"
        )

        writer.writerows(players)


# ============================================================
# KINGDOM
# ============================================================

def get_alliance_list(kid):

    path = (
        f"/kingdoms/{kid}/ranks"
        f"?board=alliance_power"
        f"&limit={ALLIANCES_PER_KINGDOM}"
    )

    data = api_get(path)

    if not data:
        return []

    boards = data.get(
        "boards",
        []
    )

    if not boards:
        return []

    return boards[0].get(
        "rows",
        []
    )


def get_alliance_roster(
    kid,
    tag
):

    encoded_tag = urllib.parse.quote(
        tag,
        safe=""
    )

    path = (
        f"/alliances/{kid}/"
        f"{encoded_tag}"
        f"?include=info,roster"
    )

    data = api_get(path)

    if not data:
        return []

    return data.get(
        "members",
        []
    )


def process_kingdom(kid):

    global current_kingdom

    current_kingdom = kid

    print()
    print("=" * 70)
    print(
        f"Kingdom {kid}"
    )
    print("=" * 70)

    alliances = get_alliance_list(
        kid
    )

    if not alliances:
        print(
            "No alliances returned."
        )

        return 0

    print(
        f"Found {len(alliances)} alliances."
    )

    output_rows = []

    seen_governor_ids = set()

    for number, alliance in enumerate(
        alliances,
        start=1
    ):

        tag = alliance.get(
            "abbr"
        )

        if not tag:
            continue

        aid = alliance.get(
            "aid"
        )

        alliance_name = alliance.get(
            "name",
            ""
        )

        member_count = alliance.get(
            "member_count",
            0
        )

        print(
            f"[{number}/"
            f"{len(alliances)}] "
            f"{tag} "
            f"({member_count} members)"
        )

        members = get_alliance_roster(
            kid,
            tag
        )

        for member in members:

            governor_id = member.get(
                "governor_id"
            )

            if governor_id is None:
                continue

            governor_id = str(
                governor_id
            )

            if governor_id in seen_governor_ids:
                continue

            seen_governor_ids.add(
                governor_id
            )

            output_rows.append(
                {
                    "kid":
                        member.get(
                            "kid",
                            kid
                        ),

                    "aid":
                        member.get(
                            "aid",
                            aid
                        ),

                    "alliance_tag":
                        tag,

                    "alliance_name":
                        alliance_name,

                    "uid":
                        member.get(
                            "uid",
                            ""
                        ),

                    "governor_id":
                        governor_id,

                    "nick_name":
                        member.get(
                            "nick_name",
                            ""
                        ),

                    "power":
                        member.get(
                            "power",
                            ""
                        ),

                    "town_center_level":
                        member.get(
                            "town_center_level",
                            ""
                        ),

                    "kills":
                        member.get(
                            "kills",
                            ""
                        ),

                    "alliance_rank":
                        member.get(
                            "alliance_rank",
                            ""
                        ),

                    "alliance_rank_label":
                        member.get(
                            "alliance_rank_label",
                            ""
                        ),
                }
            )

    append_players(
        output_rows
    )

    print()
    print(
        f"Kingdom {kid} complete."
    )

    print(
        f"Players collected: "
        f"{len(output_rows)}"
    )

    print(
        f"Current API key: "
        f"{current_key_index + 1}/"
        f"{len(API_KEYS)}"
    )

    print(
        f"Requests with this key: "
        f"{requests_today[current_key_index]}"
    )

    send_discord(
        f"Kingdom {kid} completed.\n"
        f"Players collected: "
        f"{len(output_rows)}\n"
        f"API key: "
        f"{current_key_index + 1}/"
        f"{len(API_KEYS)}"
    )

    return len(output_rows)


# ============================================================
# STARTUP
# ============================================================

if not API_KEYS:

    print()
    print(
        "ERROR: No API keys were provided."
    )
    print(
        "Set the API_KEYS environment variable."
    )
    print()

    send_discord(
        "Player Scanner could not start: "
        "no API keys configured."
    )

    raise SystemExit(1)


initialize_key_state()
load_key_state()

ensure_csv()

current_kingdom = load_progress()

print()
print("=" * 70)
print(
    "Kingshot Player Scanner"
)
print("=" * 70)
print(
    f"Kingdoms: "
    f"{FIRST_KINGDOM} - "
    f"{LAST_KINGDOM}"
)
print(
    f"Starting kingdom: "
    f"{current_kingdom}"
)
print(
    f"API keys configured: "
    f"{len(API_KEYS)}"
)
print(
    f"Daily safety limit per key: "
    f"{SAFE_DAILY_LIMIT}"
)
print("=" * 70)

send_discord(
    f"Player Scanner started.\n"
    f"Starting kingdom: {current_kingdom}\n"
    f"Target: {LAST_KINGDOM}\n"
    f"API keys: {len(API_KEYS)}"
)


# ============================================================
# MAIN LOOP
# ============================================================

try:

    for kid in range(
        current_kingdom,
        LAST_KINGDOM + 1
    ):

        current_kingdom = kid

        try:

            process_kingdom(
                kid
            )

            # Save NEXT kingdom.
            save_progress(
                kid + 1
            )

        except SystemExit:
            raise

        except Exception as e:

            log_error(
                f"Unexpected error "
                f"in kingdom {kid}: "
                f"{e}"
            )

            # Retry this kingdom
            # when the container restarts.
            save_progress(
                kid
            )

            send_discord(
                f"Player Scanner encountered "
                f"an error in kingdom {kid}.\n"
                f"Progress saved. "
                f"The kingdom will be retried."
            )

            raise


except KeyboardInterrupt:

    print()
    print(
        "Scanner stopped manually."
    )

    save_progress(
        current_kingdom
    )

    save_key_state()


print()
print("=" * 70)
print(
    "All requested kingdoms processed."
)
print("=" * 70)

send_discord(
    "Player Scanner finished "
    "all requested kingdoms."
)