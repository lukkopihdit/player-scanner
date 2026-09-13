import asyncio
import io
import json
import os
import time
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Any

import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks

BASE_URL = "https://api.mightpulse.com/v1"
KINGDOM_ID = 810
SCAN_INTERVAL_SECONDS = 3600
REQUEST_SPACING_SECONDS = 1.05
DAILY_SAFE_LIMIT = 4900
DB_PATH = os.getenv("DB_PATH", "/data/scanner.db")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
NOTIFY_CHANNEL_ID = int(os.getenv("NOTIFY_CHANNEL_ID", "0") or 0)
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)

api_keys = [x.strip() for x in os.getenv("MIGHTPULSE_API_KEYS", "").split(",") if x.strip()]

ROSTER_FIELDS = {
    "nick_name": "name",
    "town_center_level": "level",
    "power": "power",
    "kills": "kills",
    "alliance_tag": "alliance",
    "alliance_rank": "rank",
    "alliance_rank_label": "rank_label",
    "online": "online",
    "last_active_at": "last_active_at",
    "uid": "uid",
    "fid": "fid",
    "avatar_url": "avatar_url",
    "kid": "kid",
}

CHANGE_CHOICES = [
    app_commands.Choice(name="All changes", value="all"),
    app_commands.Choice(name="Name", value="name"),
    app_commands.Choice(name="Town Center level", value="level"),
    app_commands.Choice(name="Power", value="power"),
    app_commands.Choice(name="Kills", value="kills"),
    app_commands.Choice(name="Alliance", value="alliance"),
    app_commands.Choice(name="Alliance rank", value="rank"),
    app_commands.Choice(name="Online status", value="online"),
    app_commands.Choice(name="Last active", value="last_active_at"),
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fmt_value(value: Any) -> str:
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "Online" if value else "Offline"
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def clean_field(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


class RateLimiter:
    def __init__(self, spacing: float):
        self.spacing = spacing
        self.lock = asyncio.Lock()
        self.next_start = 0.0

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            wait_for = max(0.0, self.next_start - now)
            self.next_start = max(now, self.next_start) + self.spacing
        if wait_for:
            await asyncio.sleep(wait_for)


class KeyPool:
    def __init__(self, keys: list[str]):
        self.keys = keys
        self.index = 0
        self.counts = [0] * len(keys)
        self.lock = asyncio.Lock()

    async def current(self) -> tuple[int, str]:
        async with self.lock:
            return self.index, self.keys[self.index]

    async def increment(self, index: int):
        async with self.lock:
            if 0 <= index < len(self.counts):
                self.counts[index] += 1

    async def rotate(self, reason: str) -> bool:
        async with self.lock:
            if self.index + 1 >= len(self.keys):
                return False
            old = self.index + 1
            self.index += 1
            print(f"Switching API key {old} -> {self.index + 1}: {reason}", flush=True)
            return True

    async def reset_if_needed(self):
        # The service documents a daily limit but not its exact reset timestamp.
        # We intentionally keep counters for the current process lifetime and
        # rotate when a real 429 or the safety threshold is encountered.
        return

    async def count(self) -> int:
        async with self.lock:
            return sum(self.counts)


class ApiClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.keys = KeyPool(api_keys)
        self.limiter = RateLimiter(REQUEST_SPACING_SECONDS)

    async def get(self, path: str) -> dict[str, Any] | None:
        if not self.keys.keys:
            raise RuntimeError("No MIGHTPULSE_API_KEYS configured")

        while True:
            index, key = await self.keys.current()
            if self.keys.counts[index] >= DAILY_SAFE_LIMIT:
                if not await self.keys.rotate("local safety limit"):
                    raise RuntimeError("All API keys reached the local daily safety limit")
                continue

            await self.limiter.wait()
            url = BASE_URL + path
            try:
                async with self.session.get(
                    url,
                    headers={
                        "Authorization": f"Bearer {key}",
                        "Accept": "application/json",
                        "User-Agent": "PlayerScannerBot/1.0",
                    },
                    timeout=aiohttp.ClientTimeout(total=100),
                ) as response:
                    await self.keys.increment(index)
                    if response.status == 429:
                        if not await self.keys.rotate("HTTP 429"):
                            raise RuntimeError("All API keys are rate limited")
                        continue
                    if response.status == 401:
                        if not await self.keys.rotate("HTTP 401"):
                            raise RuntimeError("All API keys returned 401")
                        continue
                    if response.status == 404:
                        body = await response.text()
                        print(f"404: {path} -> {body[:300]}", flush=True)
                        return None
                    if response.status >= 400:
                        body = await response.text()
                        print(f"HTTP {response.status}: {path} -> {body[:500]}", flush=True)
                        return None
                    return await response.json()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"Request error for {path}: {exc}", flush=True)
                return None


    async def get_player_full(self, identifier: str, id_type: str | None = None) -> dict[str, Any] | None:
        encoded = urllib.parse.quote(identifier.strip(), safe="")
        query = "?include=base,heroes,ranks,gov_gear"
        if id_type:
            query += "&id_type=" + urllib.parse.quote(id_type, safe="")
        return await self.get(f"/players/{encoded}{query}")


class Database:
    def __init__(self, path: str):
        self.path = path

    async def init(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS alliances (
                    aid INTEGER PRIMARY KEY,
                    kid INTEGER NOT NULL,
                    abbr TEXT NOT NULL,
                    name TEXT NOT NULL,
                    power INTEGER,
                    member_count INTEGER,
                    leader_name TEXT,
                    power_rank INTEGER,
                    last_seen TEXT NOT NULL,
                    data_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS players (
                    governor_id TEXT PRIMARY KEY,
                    uid INTEGER,
                    fid INTEGER,
                    nick_name TEXT,
                    town_center_level INTEGER,
                    power INTEGER,
                    kills INTEGER,
                    alliance_aid INTEGER,
                    alliance_tag TEXT,
                    alliance_name TEXT,
                    alliance_rank INTEGER,
                    alliance_rank_label TEXT,
                    online INTEGER,
                    last_active_at REAL,
                    avatar_url TEXT,
                    kid INTEGER,
                    data_json TEXT NOT NULL,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    governor_id TEXT NOT NULL,
                    nick_name TEXT,
                    alliance_tag TEXT,
                    change_type TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    alliance_count INTEGER NOT NULL,
                    player_count INTEGER NOT NULL,
                    changes_count INTEGER NOT NULL,
                    error_count INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_changes_created ON changes(created_at);
                CREATE INDEX IF NOT EXISTS idx_changes_type ON changes(change_type);
                CREATE INDEX IF NOT EXISTS idx_changes_player ON changes(governor_id);
                """
            )
            await db.commit()

    async def player_snapshot(self, governor_id: str):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM players WHERE governor_id=?", (governor_id,))
            return await cur.fetchone()

    async def upsert_alliance(self, row: dict[str, Any]):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT INTO alliances
                (aid,kid,abbr,name,power,member_count,leader_name,power_rank,last_seen,data_json)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(aid) DO UPDATE SET
                    kid=excluded.kid, abbr=excluded.abbr, name=excluded.name,
                    power=excluded.power, member_count=excluded.member_count,
                    leader_name=excluded.leader_name, power_rank=excluded.power_rank,
                    last_seen=excluded.last_seen, data_json=excluded.data_json""",
                (
                    row.get("aid"), row.get("kid"), row.get("abbr"), row.get("name", ""),
                    row.get("score"), row.get("member_count"), row.get("leader_name"),
                    row.get("rank"), utc_now(), json.dumps(row, ensure_ascii=False),
                ),
            )
            await db.commit()

    async def record_player(self, member: dict[str, Any], alliance: dict[str, Any], changes: list[dict[str, Any]]):
        governor_id = member.get("governor_id")
        if governor_id is None:
            return
        governor_id = str(governor_id)
        old = await self.player_snapshot(governor_id)
        now = utc_now()
        alliance_aid = alliance.get("aid")
        alliance_tag = alliance.get("abbr", "")
        player_data = dict(member)
        player_data["alliance"] = dict(alliance)

        if old:
            for api_field, change_type in ROSTER_FIELDS.items():
                new_value = member.get(api_field)
                old_value = old[api_field]
                if clean_field(old_value) != clean_field(new_value):
                    change = {
                        "governor_id": governor_id,
                        "nick_name": member.get("nick_name") or old["nick_name"],
                        "alliance_tag": alliance_tag,
                        "change_type": change_type,
                        "old_value": fmt_value(old_value),
                        "new_value": fmt_value(new_value),
                        "created_at": now,
                    }
                    changes.append(change)

        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT INTO players
                (governor_id,uid,fid,nick_name,town_center_level,power,kills,
                 alliance_aid,alliance_tag,alliance_name,alliance_rank,alliance_rank_label,
                 online,last_active_at,avatar_url,kid,data_json,first_seen,last_seen)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(governor_id) DO UPDATE SET
                  uid=excluded.uid, fid=excluded.fid, nick_name=excluded.nick_name,
                  town_center_level=excluded.town_center_level, power=excluded.power,
                  kills=excluded.kills, alliance_aid=excluded.alliance_aid,
                  alliance_tag=excluded.alliance_tag, alliance_name=excluded.alliance_name,
                  alliance_rank=excluded.alliance_rank, alliance_rank_label=excluded.alliance_rank_label,
                  online=excluded.online, last_active_at=excluded.last_active_at,
                  avatar_url=excluded.avatar_url, kid=excluded.kid,
                  data_json=excluded.data_json, last_seen=excluded.last_seen""",
                (
                    governor_id, member.get("uid"), member.get("fid"), member.get("nick_name"),
                    member.get("town_center_level"), member.get("power"), member.get("kills"),
                    alliance_aid, alliance_tag, alliance.get("name"), member.get("alliance_rank"),
                    member.get("alliance_rank_label"),
                    1 if member.get("online") else 0 if member.get("online") is not None else None,
                    member.get("last_active_at"), member.get("avatar_url"), member.get("kid", KINGDOM_ID),
                    json.dumps(player_data, ensure_ascii=False),
                    old["first_seen"] if old else now,
                    now,
                ),
            )
            if changes:
                for c in changes:
                    await db.execute(
                        """INSERT INTO changes
                        (governor_id,nick_name,alliance_tag,change_type,old_value,new_value,created_at)
                        VALUES (?,?,?,?,?,?,?)""",
                        (c["governor_id"], c["nick_name"], c["alliance_tag"], c["change_type"], c["old_value"], c["new_value"], c["created_at"]),
                    )
            await db.commit()

    async def fetch_changes(self, change_type: str, since_hours: int, limit: int):
        since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        query = "SELECT * FROM changes WHERE created_at >= ?"
        params: list[Any] = [since]
        if change_type != "all":
            query += " AND change_type = ?"
            params.append(change_type)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(query, params)
            return await cur.fetchall()

    async def get_player(self, governor_id: str):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM players WHERE governor_id=?", (governor_id,))
            return await cur.fetchone()

    async def get_player_by_name(self, name: str):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM players WHERE nick_name = ? COLLATE NOCASE ORDER BY last_seen DESC LIMIT 1",
                (name,),
            )
            return await cur.fetchone()

    async def get_alliances(self, limit: int = 200):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM alliances WHERE kid=? ORDER BY power_rank ASC, power DESC LIMIT ?",
                (KINGDOM_ID, limit),
            )
            return await cur.fetchall()

    async def get_alliance_players(self, tag: str, limit: int = 1000):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM players WHERE alliance_tag=? ORDER BY town_center_level DESC, power DESC LIMIT ?",
                (tag, limit),
            )
            return await cur.fetchall()

    async def status(self):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM players")
            players = (await cur.fetchone())[0]
            cur = await db.execute("SELECT COUNT(*) FROM alliances")
            alliances = (await cur.fetchone())[0]
            cur = await db.execute("SELECT COUNT(*) FROM changes")
            changes = (await cur.fetchone())[0]
            cur = await db.execute("SELECT finished_at FROM scans ORDER BY id DESC LIMIT 1")
            last_scan = await cur.fetchone()
            return players, alliances, changes, (last_scan[0] if last_scan else None)

    async def add_scan(self, started: str, finished: str, alliance_count: int, player_count: int, changes_count: int, error_count: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO scans(started_at,finished_at,alliance_count,player_count,changes_count,error_count) VALUES(?,?,?,?,?,?)",
                (started, finished, alliance_count, player_count, changes_count, error_count),
            )
            await db.commit()


class Scanner:
    def __init__(self, db: Database):
        self.db = db
        self.session: aiohttp.ClientSession | None = None
        self.api: ApiClient | None = None
        self.scan_lock = asyncio.Lock()
        self.bot: commands.Bot | None = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        self.api = ApiClient(self.session)

    async def close(self):
        if self.session:
            await self.session.close()

    async def scan_once(self) -> tuple[int, int, int, int]:
        if not self.api:
            raise RuntimeError("Scanner API client not started")
        async with self.scan_lock:
            started = utc_now()
            errors = 0
            changes: list[dict[str, Any]] = []
            alliances = await self._alliances()
            if not alliances:
                raise RuntimeError("No alliances were returned for kingdom 810")

            for alliance in alliances:
                try:
                    await self.db.upsert_alliance(alliance)
                except Exception as exc:
                    errors += 1
                    print(f"Database alliance error: {exc}", flush=True)

            # The limiter spaces request starts, while aiohttp allows the
            # freshness waits to overlap rather than serializing all 100 rosters.
            tasks_ = [self._process_alliance(a, changes) for a in alliances]
            results = await asyncio.gather(*tasks_, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    errors += 1
                    print(f"Alliance scan error: {result}", flush=True)
                else:
                    errors += result

            finished = utc_now()
            player_count = await self._player_count()
            await self.db.add_scan(started, finished, len(alliances), player_count, len(changes), errors)
            await self._notify_changes(changes)
            return len(alliances), player_count, len(changes), errors

    async def _alliances(self):
        data = await self.api.get(f"/kingdoms/{KINGDOM_ID}/ranks?board=alliance_power&limit=100")
        if not data:
            return []
        boards = data.get("boards", [])
        rows = boards[0].get("rows", []) if boards else []
        # Deduplicate on alliance ID because tags can be similar/case-varied.
        seen = set()
        result = []
        for row in rows:
            aid = row.get("aid")
            if aid in seen:
                continue
            seen.add(aid)
            result.append(row)
        return result

    async def _process_alliance(self, alliance: dict[str, Any], changes: list[dict[str, Any]]) -> int:
        tag = alliance.get("abbr")
        if not tag:
            return 1
        encoded = __import__("urllib.parse", fromlist=["quote"]).quote(tag, safe="")
        data = await self.api.get(f"/alliances/{KINGDOM_ID}/{encoded}?include=info,roster")
        if not data:
            return 1
        members = data.get("members", [])
        local_errors = 0
        for member in members:
            try:
                local_changes: list[dict[str, Any]] = []
                await self.db.record_player(member, alliance, local_changes)
                changes.extend(local_changes)
            except Exception as exc:
                local_errors += 1
                print(f"Player database error: {exc}", flush=True)
        return local_errors

    async def _player_count(self) -> int:
        async with aiosqlite.connect(self.db.path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM players WHERE kid=?", (KINGDOM_ID,))
            return (await cur.fetchone())[0]

    async def _notify_changes(self, changes: list[dict[str, Any]]):
        if not self.bot or not NOTIFY_CHANNEL_ID:
            return
        level_increases = []
        for c in changes:
            if c["change_type"] != "level":
                continue
            try:
                old = int(c["old_value"])
                new = int(c["new_value"])
            except (TypeError, ValueError):
                continue
            if new > old:
                level_increases.append((c, old, new))

        channel = self.bot.get_channel(NOTIFY_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(NOTIFY_CHANNEL_ID)
            except Exception as exc:
                print(f"Could not fetch notification channel: {exc}", flush=True)
                return

        def level_label(level: int | None) -> str:
            if level is None:
                return "Unknown"
            try:
                level = int(level)
            except (TypeError, ValueError):
                return str(level)

            if level <= 30:
                return f"Lv. {level}"

            # Lv. 31-34 are the final normal levels after Lv. 30.
            if level <= 34:
                return f"30-{level - 30}"

            # Lv. 35 onward uses the Truegold (TG) naming scheme.
            # Each TG tier starts at Lv. 35 and has four sub-levels.
            offset = level - 35
            tg = (offset // 5) + 1
            sub = offset % 5
            if sub == 0:
                return f"TG {tg}"
            return f"TG {tg}-{sub}"

        for c, old, new in level_increases:
            try:
                new_level = int(new)
            except (TypeError, ValueError):
                new_level = 0

            if new_level >= 31:
                title = "Kingdom 810 truegold spending detected"
                old_display = level_label(old)
                new_display = level_label(new)
            else:
                title = "Kingdom 810 level increase"
                old_display = f"Lv. {old}"
                new_display = f"Lv. {new}"

            msg = (
                f"**{title}**\n"
                f"Player: `{c['nick_name']}`\n"
                f"Governor ID: `{c['governor_id']}`\n"
                f"Alliance: `{c['alliance_tag']}`\n"
                f"Town Center: **{old_display} → {new_display}**"
            )
            try:
                await channel.send(msg)
            except Exception as exc:
                print(f"Notification error: {exc}", flush=True)



class ScannerBot(commands.Bot):
    def __init__(self, scanner: Scanner):
        intents = discord.Intents.none()
        super().__init__(command_prefix="!", intents=intents)
        self.scanner = scanner
        scanner.bot = self

    async def setup_hook(self):
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)

            # Keep commands guild-only. Earlier versions registered the same
            # commands both globally and to this guild, which makes Discord
            # display every command twice.
            #
            # First copy the locally registered commands to the guild, then
            # clear the global command set and sync it. This also removes any
            # old global copies that were created by previous versions.
            self.tree.clear_commands(guild=guild)
            self.tree.copy_global_to(guild=guild)
            self.tree.clear_commands(guild=None)

            await self.tree.sync()
            synced = await self.tree.sync(guild=guild)
            print(
                f"Synced {len(synced)} slash commands to guild {GUILD_ID} "
                "(global duplicates removed)",
                flush=True,
            )
        else:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} global slash commands", flush=True)
        hourly_scan.start()

    async def on_ready(self):
        print(f"Logged in as {self.user} (ID: {self.user.id})", flush=True)
        print(f"Monitoring kingdom {KINGDOM_ID} every hour", flush=True)


async def run_scan_and_report():
    try:
        alliances, players, changes, errors = await scanner.scan_once()
        print(
            f"Scan complete: {alliances} alliances, {players} stored players, "
            f"{changes} changes, {errors} errors",
            flush=True,
        )
    except Exception as exc:
        print(f"Scan failed: {exc}", flush=True)


@tasks.loop(seconds=SCAN_INTERVAL_SECONDS)
async def hourly_scan():
    await run_scan_and_report()


@hourly_scan.before_loop
async def before_hourly_scan():
    await bot.wait_until_ready()
    # Run one scan immediately at startup; subsequent scans are hourly.
    await run_scan_and_report()


scanner = Scanner(Database(DB_PATH))
bot = ScannerBot(scanner)


@bot.tree.command(name="status", description="Show scanner status for kingdom 810")
async def status(interaction: discord.Interaction):
    players, alliances, changes, last_scan = await scanner.db.status()
    embed = discord.Embed(title="Player Scanner Status", color=discord.Color.blurple())
    embed.add_field(name="Kingdom", value="810", inline=True)
    embed.add_field(name="Tracked alliances", value=str(alliances), inline=True)
    embed.add_field(name="Tracked players", value=str(players), inline=True)
    embed.add_field(name="Recorded changes", value=str(changes), inline=True)
    embed.add_field(name="Interval", value="1 hour", inline=True)
    embed.add_field(name="Last scan", value=last_scan or "Not yet scanned", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="scan", description="Run an immediate scan of the top 100 alliances in kingdom 810")
async def scan(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    result = await scanner.scan_once()
    await interaction.followup.send(
        f"Scan finished. Alliances: {result[0]}, players: {result[1]}, changes: {result[2]}, errors: {result[3]}.",
        ephemeral=True,
    )


@bot.tree.command(name="changes", description="Show recorded changes")
@app_commands.describe(change_type="Which type of change to show", since_hours="Only changes from the last N hours", limit="Maximum number of results")
@app_commands.choices(change_type=CHANGE_CHOICES)
async def changes(
    interaction: discord.Interaction,
    change_type: app_commands.Choice[str] | None = None,
    since_hours: app_commands.Range[int, 1, 720] = 24,
    limit: app_commands.Range[int, 1, 25] = 10,
):
    await interaction.response.defer(ephemeral=True)
    rows = await scanner.db.fetch_changes(change_type.value if change_type else "all", since_hours, limit)
    if not rows:
        await interaction.followup.send(
            f"No changes found in the last {since_hours} hours.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title=f"Kingdom 810 changes · last {since_hours}h",
        color=discord.Color.orange(),
    )
    lines = []
    for row in rows:
        lines.append(
            f"**{row['nick_name'] or 'Unknown'}** · `{row['governor_id']}` · `{row['alliance_tag'] or '?'}`\n"
            f"`{row['change_type']}`: `{row['old_value']}` → `{row['new_value']}`\n"
            f"<t:{int(datetime.fromisoformat(row['created_at']).timestamp())}:R>"
        )
    embed.description = "\n\n".join(lines)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="player", description="Show the stored data for a tracked player")
@app_commands.describe(governor_id="The player's governor ID")
async def player(interaction: discord.Interaction, governor_id: str):
    row = await scanner.db.get_player(governor_id.strip())
    if not row:
        await interaction.response.send_message("Player not found in the stored top-100-alliance data.", ephemeral=True)
        return
    embed = discord.Embed(title=row["nick_name"] or "Unknown player", color=discord.Color.green())
    embed.add_field(name="Governor ID", value=str(row["governor_id"]), inline=True)
    embed.add_field(name="UID", value=str(row["uid"]), inline=True)
    embed.add_field(name="Town Center", value=str(row["town_center_level"]), inline=True)
    embed.add_field(name="Power", value=str(row["power"]), inline=True)
    embed.add_field(name="Kills", value=str(row["kills"]), inline=True)
    embed.add_field(name="Alliance", value=f"{row['alliance_tag']} · {row['alliance_name']}", inline=False)
    embed.add_field(name="Rank", value=f"{row['alliance_rank']} · {row['alliance_rank_label']}", inline=True)
    embed.add_field(name="Online", value="Yes" if row["online"] else "No", inline=True)
    if row["avatar_url"]:
        embed.set_thumbnail(url=row["avatar_url"])
    embed.set_footer(text=f"Last seen: {row['last_seen']}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="deepscan", description="Fetch all available data for a player by name, governor ID, or UID")
@app_commands.describe(identifier="Player name, governor ID, or UID", id_type="Use UID when the identifier is an internal UID")
@app_commands.choices(
    id_type=[
        app_commands.Choice(name="Governor ID", value="governor_id"),
        app_commands.Choice(name="UID", value="uid"),
    ]
)
async def deepscan(
    interaction: discord.Interaction,
    identifier: str,
    id_type: app_commands.Choice[str] | None = None,
):
    await interaction.response.defer(ephemeral=True)

    identifier = identifier.strip()
    resolved_type = id_type.value if id_type else None

    # Numeric identifiers are treated as governor IDs by default.
    # For names, search the stored scanner database first.
    lookup_id = identifier
    player_row = None

    if resolved_type == "uid":
        # The API can resolve UID directly.
        pass
    elif identifier.isdigit():
        player_row = await scanner.db.get_player(identifier)
    else:
        player_row = await scanner.db.get_player_by_name(identifier)
        if player_row:
            lookup_id = str(player_row["governor_id"])
            resolved_type = "governor_id"

    data = await scanner.api.get_player_full(lookup_id, resolved_type)

    if not data or not data.get("player"):
        await interaction.followup.send(
            "Player not found. Use the player's exact name, governor ID, or choose UID as the identifier type.",
            ephemeral=True,
        )
        return

    player = data.get("player", {})
    heroes = data.get("heroes")
    ranks = data.get("ranks")
    gov_gear = data.get("gov_gear")

    # Build a readable JSON document containing every section returned by the API.
    full_payload = {
        "ok": data.get("ok"),
        "uid": data.get("uid"),
        "governor_id": data.get("governor_id"),
        "id_type": data.get("id_type"),
        "include": data.get("include"),
        "fresh": data.get("fresh"),
        "cached_at": data.get("cached_at"),
        "age_seconds": data.get("age_seconds"),
        "player": player,
        "heroes": heroes,
        "ranks": ranks,
        "gov_gear": gov_gear,
    }

    pretty = json.dumps(
        full_payload,
        ensure_ascii=False,
        indent=2,
        default=str,
    )

    filename_id = str(player.get("governor_id") or data.get("governor_id") or identifier)
    filename = f"deepscan_{filename_id}.json"

    file = discord.File(
        io.BytesIO(pretty.encode("utf-8")),
        filename=filename,
    )

    embed = discord.Embed(
        title=f"Deep Scan · {player.get('nick_name') or 'Unknown'}",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Governor ID", value=str(player.get("governor_id", data.get("governor_id", ""))), inline=True)
    embed.add_field(name="UID", value=str(data.get("uid", player.get("uid", ""))), inline=True)
    embed.add_field(name="Kingdom", value=str(player.get("kid", "")), inline=True)
    embed.add_field(name="Town Center", value=str(player.get("town_center_level", "")), inline=True)
    embed.add_field(name="Power", value=str(player.get("power", "")), inline=True)
    alliance = player.get("alliance") or {}
    if isinstance(alliance, dict):
        embed.add_field(name="Alliance", value=f"{alliance.get('abbr', '')} · {alliance.get('name', '')}".strip(" ·"), inline=False)
    else:
        embed.add_field(name="Alliance", value=str(alliance), inline=False)

    sections = []
    if heroes is not None:
        sections.append(f"heroes: {'yes' if heroes else 'empty'}")
    if ranks is not None:
        sections.append("ranks: yes")
    if gov_gear is not None:
        sections.append("gov_gear: yes")
    if sections:
        embed.add_field(name="API sections", value=", ".join(sections), inline=False)

    age = data.get("age_seconds")
    if age is not None:
        embed.set_footer(text=f"API record age: {int(age)} seconds")

    await interaction.followup.send(
        embed=embed,
        file=file,
        ephemeral=True,
    )


@bot.tree.command(name="alliances", description="List the tracked alliance tags in Kingdom 810")
async def alliances(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    rows = await scanner.db.get_alliances(200)
    if not rows:
        await interaction.followup.send(
            "No alliances are stored yet. Run `/scan` first.",
            ephemeral=True,
        )
        return

    tags = []
    seen = set()
    for row in rows:
        tag = (row["abbr"] or "").strip().replace("\n", " ").replace("\r", " ")
        if tag and tag not in seen:
            seen.add(tag)
            tags.append(tag)

    content = ", ".join(tags)

    if len(content) <= 1900:
        await interaction.followup.send(
            content,
            ephemeral=True,
        )
        return

    file = discord.File(
        io.BytesIO(content.encode("utf-8")),
        filename="alliances_810.txt",
    )

    await interaction.followup.send(
        content=f"**{len(tags)} tracked alliance tags in Kingdom 810.**",
        file=file,
        ephemeral=True,
    )


@bot.tree.command(name="alliance", description="Export all stored members of one tracked alliance")
@app_commands.describe(tag="Exact case-sensitive alliance tag")
async def alliance(interaction: discord.Interaction, tag: str):
    rows = await scanner.db.get_alliance_players(tag, 200)
    if not rows:
        await interaction.response.send_message(
            "No stored players found for that exact alliance tag.",
            ephemeral=True,
        )
        return

    # Requested output format: id, name, level, kingdom
    lines = [
        f"{row['governor_id'] or ''}, {row['nick_name'] or ''}, "
        f"{row['town_center_level'] if row['town_center_level'] is not None else ''}, 810"
        for row in rows
    ]
    content = "\n".join(lines) + "\n"

    file = discord.File(
        io.BytesIO(content.encode("utf-8")),
        filename=f"alliance_{tag}_810.txt",
    )

    await interaction.response.send_message(
        content=(
            f"**{tag} · Kingdom 810**\n"
            f"{len(rows)} members. Attached in the requested format: "
            "`id, name, level, kingdom`"
        ),
        file=file,
        ephemeral=True,
    )


@bot.tree.command(name="alliancefile", description="Export every tracked alliance as separate CSV member files")
async def alliancefile(interaction: discord.Interaction):
    """Send CSV files for every tracked alliance, with at most 399 players per file."""
    await interaction.response.defer(ephemeral=True)

    alliances = await scanner.db.get_alliances(100)
    if not alliances:
        await interaction.followup.send(
            "No tracked alliances are stored yet. Run `/scan` first.",
            ephemeral=True,
        )
        return

    header = "ID,Name,TC Level,Kingdom,Power,Power Updated,Combat Power,Combat Power Updated\n"
    max_players_per_file = 399

    def csv_value(value):
        if value is None:
            return ""
        text = str(value)
        if any(ch in text for ch in [',', '"', '\n', '\r']):
            text = '"' + text.replace('"', '""') + '"'
        return text

    def member_csv_line(member):
        return ",".join([
            csv_value(member["governor_id"]),
            csv_value(member["nick_name"]),
            csv_value(member["town_center_level"]),
            "810",
            csv_value(member["power"]),
            "",
            "",
            "",
        ])

    def safe_tag(tag: str) -> str:
        cleaned = "".join(
            ch if ch.isalnum() or ch in "-_" else "_"
            for ch in tag
        )
        return cleaned or "alliance"

    prepared: list[tuple[str, discord.File, int]] = []

    for row in alliances:
        tag = (row["abbr"] or "").strip()
        if not tag:
            continue

        members = await scanner.db.get_alliance_players(tag, 1000)
        total_chunks = max(1, (len(members) + max_players_per_file - 1) // max_players_per_file)
        filename_tag = safe_tag(tag)

        for chunk_index in range(total_chunks):
            chunk = members[
                chunk_index * max_players_per_file:
                (chunk_index + 1) * max_players_per_file
            ]
            lines = [header.rstrip("\n")] + [member_csv_line(member) for member in chunk]
            content = "\n".join(lines) + "\n"

            if total_chunks == 1:
                filename = f"alliance_{filename_tag}_810.csv"
            else:
                filename = f"alliance_{filename_tag}_{chunk_index + 1}_810.csv"

            file = discord.File(
                io.BytesIO(content.encode("utf-8-sig")),
                filename=filename,
            )
            prepared.append((
                tag if total_chunks == 1 else f"{tag} ({chunk_index + 1}/{total_chunks})",
                file,
                len(chunk),
            ))

    if not prepared:
        await interaction.followup.send(
            "No alliance files could be created.",
            ephemeral=True,
        )
        return

    total = len(prepared)
    sent = 0

    for start in range(0, total, 10):
        batch = prepared[start:start + 10]
        files = [item[1] for item in batch]
        names = ", ".join(item[0] for item in batch)

        await interaction.followup.send(
            content=(
                f"**Kingdom 810 alliance CSV files**\n"
                f"Maximum {max_players_per_file} players per file.\n"
                f"Batch {start // 10 + 1}: {names}"
            ),
            files=files,
            ephemeral=True,
        )
        sent += len(batch)

    await interaction.followup.send(
        f"Finished. Created {sent} CSV alliance files. Each file contains at most {max_players_per_file} players.",
        ephemeral=True,
    )


@bot.tree.command(name="alliancefilemerged", description="Export top alliances separately and merge the rest into CSV files")
@app_commands.describe(top_count="Number of top alliances to keep as individual files (1-99)")
async def alliancefilemerged(
    interaction: discord.Interaction,
    top_count: app_commands.Range[int, 1, 99],
):
    """Create CSV files with at most 399 players in every file."""
    await interaction.response.defer(ephemeral=True)

    alliances = await scanner.db.get_alliances(100)
    if not alliances:
        await interaction.followup.send(
            "No tracked alliances are stored yet. Run `/scan` first.",
            ephemeral=True,
        )
        return

    alliances = list(alliances[:100])
    top = alliances[:top_count]
    remainder = alliances[top_count:]

    header = "ID,Name,TC Level,Kingdom,Power,Power Updated,Combat Power,Combat Power Updated\n"
    max_players_per_file = 399

    def safe_tag(tag: str) -> str:
        cleaned = "".join(
            ch if ch.isalnum() or ch in "-_" else "_"
            for ch in tag
        )
        return cleaned or "alliance"

    def csv_value(value):
        if value is None:
            return ""
        text = str(value)
        if any(ch in text for ch in [',', '"', '\n', '\r']):
            text = '"' + text.replace('"', '""') + '"'
        return text

    def member_csv_line(member):
        return ",".join([
            csv_value(member["governor_id"]),
            csv_value(member["nick_name"]),
            csv_value(member["town_center_level"]),
            "810",
            csv_value(member["power"]),
            "",
            "",
            "",
        ])

    files: list[discord.File] = []
    file_labels: list[str] = []

    # Top alliances stay separate. If one exceeds 399 players, it is split
    # into multiple files, each with the exact same CSV format.
    for row in top:
        tag = (row["abbr"] or "").strip()
        if not tag:
            continue

        members = await scanner.db.get_alliance_players(tag, 1000)
        total_chunks = max(1, (len(members) + max_players_per_file - 1) // max_players_per_file)

        for chunk_index in range(total_chunks):
            chunk = members[
                chunk_index * max_players_per_file:
                (chunk_index + 1) * max_players_per_file
            ]
            lines = [header.rstrip("\n")] + [member_csv_line(member) for member in chunk]
            content = "\n".join(lines) + "\n"

            if total_chunks == 1:
                filename = f"alliance_{safe_tag(tag)}_810.csv"
                label = tag
            else:
                filename = f"alliance_{safe_tag(tag)}_{chunk_index + 1}_810.csv"
                label = f"{tag} ({chunk_index + 1}/{total_chunks})"

            files.append(
                discord.File(
                    io.BytesIO(content.encode("utf-8-sig")),
                    filename=filename,
                )
            )
            file_labels.append(label)

    # All remaining alliances are merged together, then split every 399 rows.
    # This keeps the merged files in the exact same CSV format as individual files.
    if remainder:
        merged_rows: list[str] = []

        for row in remainder:
            tag = (row["abbr"] or "").strip()
            if not tag:
                continue

            members = await scanner.db.get_alliance_players(tag, 1000)
            merged_rows.extend(member_csv_line(member) for member in members)

        total_chunks = max(1, (len(merged_rows) + max_players_per_file - 1) // max_players_per_file)

        for chunk_index in range(total_chunks):
            chunk = merged_rows[
                chunk_index * max_players_per_file:
                (chunk_index + 1) * max_players_per_file
            ]
            lines = [header.rstrip("\n")] + chunk
            content = "\n".join(lines) + "\n"

            if total_chunks == 1:
                filename = "alliances_rest_810.csv"
                label = "REST"
            else:
                filename = f"alliances_rest_{chunk_index + 1}_810.csv"
                label = f"REST ({chunk_index + 1}/{total_chunks})"

            files.append(
                discord.File(
                    io.BytesIO(content.encode("utf-8-sig")),
                    filename=filename,
                )
            )
            file_labels.append(label)

    if not files:
        await interaction.followup.send(
            "No alliance files could be created.",
            ephemeral=True,
        )
        return

    total_files = len(files)
    sent = 0

    for start in range(0, total_files, 10):
        batch = files[start:start + 10]
        labels = file_labels[start:start + 10]

        await interaction.followup.send(
            content=(
                f"**Kingdom 810 alliance CSV files**\n"
                f"Top {top_count} alliances individually; remaining "
                f"{len(remainder)} alliances merged.\n"
                f"Maximum {max_players_per_file} players per file.\n"
                f"Batch {start // 10 + 1}: {', '.join(labels)}"
            ),
            files=batch,
            ephemeral=True,
        )
        sent += len(batch)

    await interaction.followup.send(
        f"Finished. Created {sent} CSV files. Every file contains at most {max_players_per_file} players.",
        ephemeral=True,
    )


@bot.tree.command(name="webhooktest", description="Send a test message to the notification channel")
async def webhooktest(interaction: discord.Interaction):
    if not NOTIFY_CHANNEL_ID:
        await interaction.response.send_message("NOTIFY_CHANNEL_ID is not configured.", ephemeral=True)
        return
    channel = bot.get_channel(NOTIFY_CHANNEL_ID)
    if channel is None:
        channel = await bot.fetch_channel(NOTIFY_CHANNEL_ID)
    await channel.send("Player Scanner notification test successful.")
    await interaction.response.send_message("Test message sent.", ephemeral=True)


async def main():
    if not DISCORD_TOKEN:
        raise SystemExit("DISCORD_TOKEN is not configured")
    if not api_keys:
        raise SystemExit("MIGHTPULSE_API_KEYS is not configured")

    await scanner.db.init()
    await scanner.start()
    try:
        await bot.start(DISCORD_TOKEN)
    finally:
        await scanner.close()


if __name__ == "__main__":
    asyncio.run(main())
