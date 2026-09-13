import asyncio
import io
import json
import math
import os
import time
import urllib.parse
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime, timezone, timedelta
from typing import Any

import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks

BASE_URL = "https://api.mightpulse.com/v1"
PUBLIC_BASE_URL = "https://mightpulse.com/api"
KINGDOM_ID = 810
SCAN_INTERVAL_SECONDS = 3600
REQUEST_SPACING_SECONDS = 1.05
DAILY_SAFE_LIMIT = 4900
MAX_CSV_PLAYERS = 399
REPORT_HOUR_UTC = int(os.getenv("REPORT_HOUR_UTC", "0"))
RANK_ALERT_THRESHOLD = int(os.getenv("RANK_ALERT_THRESHOLD", "5"))
REPORT_CHANNEL_ID = int(os.getenv("REPORT_CHANNEL_ID", "0") or 0)
DAILY_REPORT_CHANNEL_ID = int(os.getenv("DAILY_REPORT_CHANNEL_ID", str(REPORT_CHANNEL_ID)) or 0)
WEEKLY_REPORT_CHANNEL_ID = int(os.getenv("WEEKLY_REPORT_CHANNEL_ID", str(REPORT_CHANNEL_ID)) or 0)
DB_PATH = os.getenv("DB_PATH", "/data/scanner.db")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
NOTIFY_CHANNEL_ID = int(os.getenv("NOTIFY_CHANNEL_ID", "0") or 0)
KVK_OPPONENT_CHANNEL_ID = int(os.getenv("KVK_OPPONENT_CHANNEL_ID", "0") or 0)
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

BOARD_OPTIONS = [
    ("Alliance Power", "alliance_power"),
    ("Alliance Kills", "alliance_kills"),
    ("Personal Power", "personal_power"),
    ("Kills", "kills"),
    ("Town Center", "town_center"),
    ("Rebel Conquest", "rebel_conquest"),
    ("Single Hero", "single_hero"),
    ("Hero Total", "hero_total"),
    ("Troop Power", "troop_power"),
    ("Building Power", "building_power"),
    ("Research Power", "research_power"),
    ("Hero No Equip", "hero_no_equip"),
    ("Hero Equip", "hero_equip"),
    ("Governor Gear", "gov_gear"),
    ("Governor Charm", "gov_charm"),
    ("Pet Power", "pet_power"),
    ("Island Prosperity", "island_prosperity"),
    ("Migrant Score", "migrant_score"),
    ("Mystic Trial", "mystic_trial"),
    ("Coliseum", "coliseum"),
    ("Forest of Life", "forest_of_life"),
    ("Crystal Cave", "crystal_cave"),
    ("Knowledge Nexus", "knowledge_nexus"),
    ("Molten Fort", "molten_fort"),
    ("Radiant Spire", "radiant_spire"),
    ("Master Power", "master_power"),
]

CHANGE_CHOICES = [
    app_commands.Choice(name="All changes", value="all"),
    app_commands.Choice(name="Name", value="name"),
    app_commands.Choice(name="Town Center level", value="level"),
    app_commands.Choice(name="Power", value="power"),
    app_commands.Choice(name="Kills", value="kills"),
    app_commands.Choice(name="Alliance", value="alliance"),
    app_commands.Choice(name="Alliance rank", value="rank"),
    app_commands.Choice(name="Alliance rank label", value="rank_label"),
    app_commands.Choice(name="Online status", value="online"),
    app_commands.Choice(name="Last active", value="last_active_at"),
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def compact_number(value: Any) -> str:
    if value is None:
        return "0"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)
    a = abs(n)
    if a >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}B"
    if a >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if a >= 1_000:
        return f"{n / 1_000:.1f}K"
    return f"{n:.0f}"


def fmt_value(value: Any) -> str:
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "Online" if value else "Offline"
    return str(value)


def clean_field(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def level_label(level: int | None) -> str:
    if level is None:
        return "Unknown"
    try:
        level = int(level)
    except (TypeError, ValueError):
        return str(level)
    if level <= 30:
        return f"Lv. {level}"
    if level <= 34:
        return f"30-{level - 30}"
    offset = level - 35
    tg = (offset // 5) + 1
    sub = offset % 5
    return f"TG {tg}" if sub == 0 else f"TG {tg}-{sub}"


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
            try:
                async with self.session.get(
                    BASE_URL + path,
                    headers={
                        "Authorization": f"Bearer {key}",
                        "Accept": "application/json",
                        "User-Agent": "PlayerScannerBot/1.0",
                    },
                    timeout=aiohttp.ClientTimeout(total=100),
                ) as response:
                    await self.keys.increment(index)
                    if response.status in (429, 401):
                        if not await self.keys.rotate(f"HTTP {response.status}"):
                            raise RuntimeError(f"All API keys rejected/rate-limited (HTTP {response.status})")
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

    async def get_player_full(self, identifier: str, id_type: str | None = None):
        encoded = urllib.parse.quote(identifier.strip(), safe="")
        query = "?include=base,heroes,ranks,gov_gear"
        if id_type:
            query += "&id_type=" + urllib.parse.quote(id_type, safe="")
        return await self.get(f"/players/{encoded}{query}")

    async def get_player_base(self, identifier: str, id_type: str = "uid"):
        encoded = urllib.parse.quote(identifier.strip(), safe="")
        query = "?include=base"
        if id_type:
            query += "&id_type=" + urllib.parse.quote(id_type, safe="")
        return await self.get(f"/players/{encoded}{query}")

    async def get_public_kingdom(self, kingdom_id: int) -> dict[str, Any] | None:
        """Fetch the public kingdom page data only for fields unavailable in the documented API."""
        try:
            async with self.session.get(
                f"{PUBLIC_BASE_URL}/kingdoms/{kingdom_id}?players=100&alliances=100",
                headers={
                    "Accept": "application/json",
                    "User-Agent": "PlayerScannerBot/1.0",
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status >= 400:
                    body = await response.text()
                    print(
                        f"Public kingdom API HTTP {response.status}: {kingdom_id} -> {body[:500]}",
                        flush=True,
                    )
                    return None
                return await response.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Public kingdom API error for {kingdom_id}: {exc}", flush=True)
            return None


class Database:
    def __init__(self, path: str):
        self.path = path

    async def init(self):
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS alliances (
                    aid INTEGER PRIMARY KEY, kid INTEGER NOT NULL, abbr TEXT NOT NULL, name TEXT NOT NULL,
                    power INTEGER, member_count INTEGER, leader_name TEXT, power_rank INTEGER,
                    last_seen TEXT NOT NULL, data_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS alliance_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, aid INTEGER, kid INTEGER, abbr TEXT, name TEXT,
                    power INTEGER, member_count INTEGER, power_rank INTEGER, leader_name TEXT, captured_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_alliance_snapshots_aid_time ON alliance_snapshots(aid, captured_at);
                CREATE TABLE IF NOT EXISTS players (
                    governor_id TEXT PRIMARY KEY, uid INTEGER, fid INTEGER, nick_name TEXT,
                    town_center_level INTEGER, power INTEGER, kills INTEGER,
                    alliance_aid INTEGER, alliance_tag TEXT, alliance_name TEXT,
                    alliance_rank INTEGER, alliance_rank_label TEXT, online INTEGER,
                    last_active_at REAL, avatar_url TEXT, kid INTEGER, data_json TEXT NOT NULL,
                    first_seen TEXT NOT NULL, last_seen TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS player_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, governor_id TEXT NOT NULL, uid INTEGER, nick_name TEXT,
                    town_center_level INTEGER, power INTEGER, kills INTEGER, alliance_aid INTEGER,
                    alliance_tag TEXT, alliance_rank INTEGER, online INTEGER, last_active_at REAL,
                    kid INTEGER, captured_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_player_snapshots_player_time ON player_snapshots(governor_id, captured_at);
                CREATE INDEX IF NOT EXISTS idx_player_snapshots_time ON player_snapshots(captured_at);
                CREATE TABLE IF NOT EXISTS changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, governor_id TEXT NOT NULL, nick_name TEXT,
                    alliance_tag TEXT, change_type TEXT NOT NULL, old_value TEXT, new_value TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_changes_created ON changes(created_at);
                CREATE INDEX IF NOT EXISTS idx_changes_type ON changes(change_type);
                CREATE INDEX IF NOT EXISTS idx_changes_player ON changes(governor_id);
                CREATE TABLE IF NOT EXISTS scans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT NOT NULL, finished_at TEXT NOT NULL,
                    alliance_count INTEGER NOT NULL, player_count INTEGER NOT NULL,
                    changes_count INTEGER NOT NULL, error_count INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS watches_players (
                    governor_id TEXT PRIMARY KEY, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS watches_alliances (
                    aid INTEGER PRIMARY KEY, abbr TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS report_runs (
                    report_type TEXT PRIMARY KEY, last_sent_at TEXT
                );
                """
            )
            await db.commit()

    async def player_snapshot(self, governor_id: str):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM players WHERE governor_id=?", (governor_id,))
            return await cur.fetchone()

    async def player_by_name(self, name: str):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM players WHERE nick_name = ? COLLATE NOCASE ORDER BY last_seen DESC LIMIT 1",
                (name,),
            )
            return await cur.fetchone()

    async def get_player(self, governor_id: str):
        return await self.player_snapshot(governor_id)

    async def get_players_by_governor_ids(self, governor_ids: list[str]):
        if not governor_ids:
            return {}
        placeholders = ",".join("?" for _ in governor_ids)
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                f"SELECT * FROM players WHERE governor_id IN ({placeholders})",
                [str(x) for x in governor_ids],
            )
            rows = await cur.fetchall()
            return {str(row["governor_id"]): row for row in rows}

    async def get_alliances(self, limit: int = 100):
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

    async def record_alliance_snapshot(self, alliance: dict[str, Any]):
        now = utc_now()
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO alliance_snapshots(aid,kid,abbr,name,power,member_count,power_rank,leader_name,captured_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (alliance.get("aid"), alliance.get("kid", KINGDOM_ID), alliance.get("abbr"), alliance.get("name"),
                 alliance.get("score"), alliance.get("member_count"), alliance.get("rank"), alliance.get("leader_name"), now),
            )
            await db.commit()

    async def upsert_alliance(self, row: dict[str, Any]):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT INTO alliances
                (aid,kid,abbr,name,power,member_count,leader_name,power_rank,last_seen,data_json)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(aid) DO UPDATE SET
                  kid=excluded.kid,abbr=excluded.abbr,name=excluded.name,power=excluded.power,
                  member_count=excluded.member_count,leader_name=excluded.leader_name,power_rank=excluded.power_rank,
                  last_seen=excluded.last_seen,data_json=excluded.data_json""",
                (row.get("aid"), row.get("kid", KINGDOM_ID), row.get("abbr"), row.get("name", ""), row.get("score"),
                 row.get("member_count"), row.get("leader_name"), row.get("rank"), utc_now(), json.dumps(row, ensure_ascii=False)),
            )
            await db.commit()

    async def record_player(self, member: dict[str, Any], alliance: dict[str, Any], changes: list[dict[str, Any]], seen_ids: set[str]):
        governor_id = member.get("governor_id")
        if governor_id is None:
            return
        governor_id = str(governor_id)
        seen_ids.add(governor_id)
        old = await self.player_snapshot(governor_id)
        now = utc_now()
        alliance_aid = alliance.get("aid")
        alliance_tag = alliance.get("abbr", "")
        if old:
            for api_field, change_type in ROSTER_FIELDS.items():
                old_value = old[api_field]
                new_value = member.get(api_field)
                if clean_field(old_value) != clean_field(new_value):
                    changes.append({
                        "governor_id": governor_id,
                        "nick_name": member.get("nick_name") or old["nick_name"],
                        "alliance_tag": alliance_tag,
                        "change_type": change_type,
                        "old_value": fmt_value(old_value),
                        "new_value": fmt_value(new_value),
                        "created_at": now,
                    })

        data = dict(member)
        data["alliance"] = dict(alliance)
        # A new database record is a baseline, not a join event. A reappearance
        # after a recorded departure is a real join back into tracked territory.
        if old is not None:
            async with aiosqlite.connect(self.path) as check_db:
                cur = await check_db.execute(
                    "SELECT 1 FROM changes WHERE governor_id=? AND change_type='left_tracked' AND created_at>? LIMIT 1",
                    (governor_id, old["last_seen"]),
                )
                if await cur.fetchone():
                    changes.append({
                        "governor_id": governor_id,
                        "nick_name": member.get("nick_name"),
                        "alliance_tag": alliance_tag,
                        "change_type": "joined_tracked",
                        "old_value": "Not tracked",
                        "new_value": alliance_tag,
                        "created_at": now,
                    })
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT INTO players
                (governor_id,uid,fid,nick_name,town_center_level,power,kills,alliance_aid,alliance_tag,
                 alliance_name,alliance_rank,alliance_rank_label,online,last_active_at,avatar_url,kid,data_json,first_seen,last_seen)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(governor_id) DO UPDATE SET
                  uid=excluded.uid,fid=excluded.fid,nick_name=excluded.nick_name,
                  town_center_level=excluded.town_center_level,power=excluded.power,kills=excluded.kills,
                  alliance_aid=excluded.alliance_aid,alliance_tag=excluded.alliance_tag,alliance_name=excluded.alliance_name,
                  alliance_rank=excluded.alliance_rank,alliance_rank_label=excluded.alliance_rank_label,
                  online=excluded.online,last_active_at=excluded.last_active_at,avatar_url=excluded.avatar_url,
                  kid=excluded.kid,data_json=excluded.data_json,last_seen=excluded.last_seen""",
                (governor_id, member.get("uid"), member.get("fid"), member.get("nick_name"), member.get("town_center_level"),
                 member.get("power"), member.get("kills"), alliance_aid, alliance_tag, alliance.get("name"),
                 member.get("alliance_rank"), member.get("alliance_rank_label"),
                 1 if member.get("online") else 0 if member.get("online") is not None else None,
                 member.get("last_active_at"), member.get("avatar_url"), member.get("kid", KINGDOM_ID),
                 json.dumps(data, ensure_ascii=False), old["first_seen"] if old else now, now),
            )
            await db.execute(
                "INSERT INTO player_snapshots(governor_id,uid,nick_name,town_center_level,power,kills,alliance_aid,alliance_tag,alliance_rank,online,last_active_at,kid,captured_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (governor_id, member.get("uid"), member.get("nick_name"), member.get("town_center_level"), member.get("power"),
                 member.get("kills"), alliance_aid, alliance_tag, member.get("alliance_rank"),
                 1 if member.get("online") else 0 if member.get("online") is not None else None,
                 member.get("last_active_at"), member.get("kid", KINGDOM_ID), now),
            )
            for c in changes[-len(changes):]:
                pass
            if changes:
                # Only insert changes generated for this player in this invocation.
                # The caller supplies a shared list, so we detect by governor_id and created_at.
                for c in [x for x in changes if x["governor_id"] == governor_id and x["created_at"] == now]:
                    await db.execute(
                        "INSERT INTO changes(governor_id,nick_name,alliance_tag,change_type,old_value,new_value,created_at) VALUES(?,?,?,?,?,?,?)",
                        (c["governor_id"], c["nick_name"], c["alliance_tag"], c["change_type"], c["old_value"], c["new_value"], c["created_at"]),
                    )
            await db.commit()

    async def record_departures(self, seen_ids: set[str], scan_time: str):
        changes: list[dict[str, Any]] = []
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM players WHERE kid=?", (KINGDOM_ID,))
            players = await cur.fetchall()
            for row in players:
                if str(row["governor_id"]) in seen_ids:
                    continue
                cur2 = await db.execute(
                    "SELECT 1 FROM changes WHERE governor_id=? AND change_type='left_tracked' AND created_at>? LIMIT 1",
                    (str(row["governor_id"]), row["last_seen"]),
                )
                if await cur2.fetchone():
                    continue
                # Only classify a departure when the whole scan was successful.
                changes.append({
                    "governor_id": str(row["governor_id"]),
                    "nick_name": row["nick_name"],
                    "alliance_tag": row["alliance_tag"],
                    "change_type": "left_tracked",
                    "old_value": row["alliance_tag"],
                    "new_value": "Not in tracked top 100",
                    "created_at": scan_time,
                })
            for c in changes:
                await db.execute(
                    "INSERT INTO changes(governor_id,nick_name,alliance_tag,change_type,old_value,new_value,created_at) VALUES(?,?,?,?,?,?,?)",
                    (c["governor_id"], c["nick_name"], c["alliance_tag"], c["change_type"], c["old_value"], c["new_value"], c["created_at"]),
                )
            await db.commit()
        return changes

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

    async def player_history(self, governor_id: str, since_hours: int, limit: int = 100):
        since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM player_snapshots WHERE governor_id=? AND captured_at>=? ORDER BY captured_at ASC LIMIT ?",
                (governor_id, since, limit),
            )
            return await cur.fetchall()

    async def power_changes(self, since_hours: int, limit: int = 25, alliance_tag: str | None = None):
        since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            q = """SELECT governor_id, nick_name, alliance_tag, MAX(power)-MIN(power) AS growth,
                    MIN(power) AS old_power, MAX(power) AS new_power
                    FROM player_snapshots WHERE kid=? AND captured_at>=?"""
            params: list[Any] = [KINGDOM_ID, since]
            if alliance_tag:
                q += " AND alliance_tag=?"
                params.append(alliance_tag)
            q += " GROUP BY governor_id, nick_name, alliance_tag ORDER BY growth DESC LIMIT ?"
            params.append(limit)
            cur = await db.execute(q, params)
            return await cur.fetchall()

    async def truegold_changes(self, since_hours: int, limit: int, alliance_tag: str | None = None):
        since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            q = "SELECT * FROM changes WHERE change_type='level' AND created_at>=?"
            params: list[Any] = [since]
            if alliance_tag:
                q += " AND alliance_tag=?"
                params.append(alliance_tag)
            q += " ORDER BY id DESC LIMIT ?"
            params.append(limit * 3)
            cur = await db.execute(q, params)
            rows = await cur.fetchall()
        result = []
        for row in rows:
            try:
                old, new = int(row["old_value"]), int(row["new_value"])
            except (TypeError, ValueError):
                continue
            if new >= 35 and new > old:
                result.append(row)
            if len(result) >= limit:
                break
        return result

    async def inactive(self, days: int, limit: int, alliance_tag: str | None = None):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            q = "SELECT * FROM players WHERE kid=? AND (last_active_at IS NULL OR last_active_at<?)"
            params: list[Any] = [KINGDOM_ID, cutoff]
            if alliance_tag:
                q += " AND alliance_tag=?"
                params.append(alliance_tag)
            q += " ORDER BY last_active_at ASC LIMIT ?"
            params.append(limit)
            cur = await db.execute(q, params)
            return await cur.fetchall()

    async def current_online(self, online: bool, alliance_tag: str | None, limit: int):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            q = "SELECT * FROM players WHERE kid=? AND online=?"
            params: list[Any] = [KINGDOM_ID, 1 if online else 0]
            if alliance_tag:
                q += " AND alliance_tag=?"
                params.append(alliance_tag)
            q += " ORDER BY power DESC LIMIT ?"
            params.append(limit)
            cur = await db.execute(q, params)
            return await cur.fetchall()

    async def alliance_stats(self, tag: str):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM alliances WHERE kid=? AND abbr=? ORDER BY last_seen DESC LIMIT 1", (KINGDOM_ID, tag))
            alliance = await cur.fetchone()
            cur = await db.execute("SELECT * FROM players WHERE kid=? AND alliance_tag=?", (KINGDOM_ID, tag))
            players = await cur.fetchall()
            return alliance, players

    async def alliance_history(self, aid: int, since_hours: int, limit: int = 100):
        since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM alliance_snapshots WHERE aid=? AND captured_at>=? ORDER BY captured_at ASC LIMIT ?",
                (aid, since, limit),
            )
            return await cur.fetchall()

    async def compare_alliance_snapshots(self, aid: int):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM alliance_snapshots WHERE aid=? ORDER BY captured_at DESC LIMIT 2", (aid,))
            return await cur.fetchall()

    async def watched_players(self):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM watches_players")
            return await cur.fetchall()

    async def watched_alliances(self):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM watches_alliances")
            return await cur.fetchall()

    async def add_watch_player(self, governor_id: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("INSERT OR IGNORE INTO watches_players(governor_id,created_at) VALUES(?,?)", (governor_id, utc_now()))
            await db.commit()

    async def remove_watch_player(self, governor_id: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("DELETE FROM watches_players WHERE governor_id=?", (governor_id,))
            await db.commit()

    async def add_watch_alliance(self, aid: int, abbr: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("INSERT OR REPLACE INTO watches_alliances(aid,abbr,created_at) VALUES(?,?,?)", (aid, abbr, utc_now()))
            await db.commit()

    async def remove_watch_alliance(self, aid: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("DELETE FROM watches_alliances WHERE aid=?", (aid,))
            await db.commit()

    async def watch_status(self):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM watches_players")
            p = (await cur.fetchone())[0]
            cur = await db.execute("SELECT COUNT(*) FROM watches_alliances")
            a = (await cur.fetchone())[0]
            return p, a

    async def report_sent(self, report_type: str):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT last_sent_at FROM report_runs WHERE report_type=?", (report_type,))
            row = await cur.fetchone()
            return row[0] if row else None

    async def mark_report(self, report_type: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO report_runs(report_type,last_sent_at) VALUES(?,?) ON CONFLICT(report_type) DO UPDATE SET last_sent_at=excluded.last_sent_at",
                (report_type, utc_now()),
            )
            await db.commit()

    async def status(self):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM players WHERE kid=?", (KINGDOM_ID,))
            players = (await cur.fetchone())[0]
            cur = await db.execute("SELECT COUNT(*) FROM alliances WHERE kid=?", (KINGDOM_ID,))
            alliances = (await cur.fetchone())[0]
            cur = await db.execute("SELECT COUNT(*) FROM changes")
            changes = (await cur.fetchone())[0]
            cur = await db.execute("SELECT finished_at,alliance_count,player_count,changes_count,error_count FROM scans ORDER BY id DESC LIMIT 1")
            last_scan = await cur.fetchone()
            return players, alliances, changes, last_scan

    async def scan_counts(self):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM player_snapshots")
            snapshots = (await cur.fetchone())[0]
            cur = await db.execute("SELECT COUNT(*) FROM alliance_snapshots")
            alliance_snapshots = (await cur.fetchone())[0]
            return snapshots, alliance_snapshots

    async def alliance_rank_movements(self, threshold: int = 5):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""SELECT aid,abbr,old_rank,new_rank FROM (
                SELECT aid,abbr,
                       (SELECT power_rank FROM alliance_snapshots x WHERE x.aid=s.aid ORDER BY x.id DESC LIMIT 1 OFFSET 1) AS old_rank,
                       (SELECT power_rank FROM alliance_snapshots y WHERE y.aid=s.aid ORDER BY y.id DESC LIMIT 1) AS new_rank
                FROM alliance_snapshots s GROUP BY aid,abbr
            ) WHERE old_rank IS NOT NULL AND new_rank IS NOT NULL AND ABS(old_rank-new_rank)>=?""", (threshold,))
            return await cur.fetchall()

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
        self.last_errors = 0
        self.last_request_started = 0

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
            seen_ids: set[str] = set()
            alliances = await self._alliances()
            if not alliances:
                raise RuntimeError("No alliances were returned for kingdom 810")

            for alliance in alliances:
                try:
                    await self.db.upsert_alliance(alliance)
                    await self.db.record_alliance_snapshot(alliance)
                except Exception as exc:
                    errors += 1
                    print(f"Alliance database error: {exc}", flush=True)

            results = await asyncio.gather(
                *[self._process_alliance(a, changes, seen_ids) for a in alliances],
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    errors += 1
                    print(f"Alliance scan error: {result}", flush=True)
                else:
                    errors += result

            # Only classify missing players as departures when all 100 alliance
            # roster requests succeeded. This avoids false "left" events after an API hiccup.
            successful_alliances = errors == 0
            if successful_alliances:
                changes.extend(await self.db.record_departures(seen_ids, utc_now()))

            finished = utc_now()
            players, _, _, _ = await self.db.status()
            await self.db.add_scan(started, finished, len(alliances), players, len(changes), errors)
            self.last_errors = errors
            await self._notify_changes(changes)
            await self._notify_watches(changes)
            if errors == 0:
                try:
                    await notify_rank_alerts(await self.db.alliance_rank_movements(RANK_ALERT_THRESHOLD))
                except Exception as exc:
                    print(f"Rank alert error: {exc}", flush=True)
            return len(alliances), players, len(changes), errors

    async def _alliances(self):
        data = await self.api.get(f"/kingdoms/{KINGDOM_ID}/ranks?board=alliance_power&limit=100")
        if not data:
            return []
        boards = data.get("boards", [])
        rows = boards[0].get("rows", []) if boards else []
        seen = set()
        result = []
        for row in rows:
            aid = row.get("aid")
            if aid in seen:
                continue
            seen.add(aid)
            result.append(row)
        return result

    async def discover(self):
        if not self.api:
            raise RuntimeError("Scanner API client not started")
        kingdom = await self.api.get(f"/kingdoms/{KINGDOM_ID}") or {}
        power = await self.api.get(f"/kingdoms/{KINGDOM_ID}/ranks?board=alliance_power&limit=100") or {}
        kills = await self.api.get(f"/kingdoms/{KINGDOM_ID}/ranks?board=alliance_kills&limit=100") or {}
        power_rows = (power.get("boards", [{}])[0].get("rows", []) if power.get("boards") else [])
        kill_rows = (kills.get("boards", [{}])[0].get("rows", []) if kills.get("boards") else [])
        power_ids = {r.get("aid") for r in power_rows}
        kill_ids = {r.get("aid") for r in kill_rows}
        extra = [r for r in kill_rows if r.get("aid") not in power_ids]
        count = kingdom.get("alliance_count", len(power_rows))
        return count, len(power_rows), len(kill_rows), extra

    async def _process_alliance(self, alliance: dict[str, Any], changes: list[dict[str, Any]], seen_ids: set[str]) -> int:
        tag = alliance.get("abbr")
        if not tag:
            return 1
        encoded = urllib.parse.quote(tag, safe="")
        data = await self.api.get(f"/alliances/{KINGDOM_ID}/{encoded}?include=info,roster")
        if not data:
            return 1
        members = data.get("members", [])
        before = len(changes)
        for member in members:
            try:
                await self.db.record_player(member, alliance, changes, seen_ids)
            except Exception as exc:
                print(f"Player database error: {exc}", flush=True)
                return 1
        return 0 if len(changes) >= before else 0

    async def _notify_changes(self, changes: list[dict[str, Any]]):
        if not self.bot or not NOTIFY_CHANNEL_ID:
            return
        channel = self.bot.get_channel(NOTIFY_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(NOTIFY_CHANNEL_ID)
            except Exception as exc:
                print(f"Could not fetch notification channel: {exc}", flush=True)
                return

        for c in changes:
            if c["change_type"] != "level":
                continue
            try:
                old, new = int(c["old_value"]), int(c["new_value"])
            except (TypeError, ValueError):
                continue
            if new <= old:
                continue
            if new >= 31:
                title = "Kingdom 810 truegold spending detected"
                old_display, new_display = level_label(old), level_label(new)
            else:
                title = "Kingdom 810 level increase"
                old_display, new_display = f"Lv. {old}", f"Lv. {new}"
            player_name = c.get("nick_name") or "Unknown"
            alliance_tag = c.get("alliance_tag") or "?"
            display_name = f"[{alliance_tag}]{player_name}"
            msg = (
                f"**{title}**\n"
                f"Player: `{display_name}`\n"
                f"Town Center: **{old_display} → {new_display}**"
            )
            try:
                await channel.send(msg)
            except Exception as exc:
                print(f"Notification error: {exc}", flush=True)

    async def _notify_watches(self, changes: list[dict[str, Any]]):
        if not self.bot or not NOTIFY_CHANNEL_ID or not changes:
            return
        watched_players = {str(r["governor_id"]) for r in await self.db.watched_players()}
        watched_alliance_rows = await self.db.watched_alliances()
        watched_alliances = {str(r["abbr"]) for r in watched_alliance_rows}
        selected = []
        for c in changes:
            if str(c["governor_id"]) in watched_players or str(c["alliance_tag"]) in watched_alliances:
                selected.append(c)
        if not selected:
            return
        channel = self.bot.get_channel(NOTIFY_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(NOTIFY_CHANNEL_ID)
            except Exception:
                return
        unique = {(c["governor_id"], c["change_type"], c["created_at"], c["new_value"]) for c in selected}
        for key in unique:
            c = next(x for x in selected if (x["governor_id"], x["change_type"], x["created_at"], x["new_value"]) == key)
            await channel.send(
                f"**Watched player change**\nPlayer: `{c['nick_name']}` · `{c['governor_id']}`\n"
                f"Alliance: `{c['alliance_tag']}`\n{c['change_type']}: `{c['old_value']}` → `{c['new_value']}`"
            )


class ScannerBot(commands.Bot):
    def __init__(self, scanner: Scanner):
        super().__init__(command_prefix="!", intents=discord.Intents.none())
        self.scanner = scanner
        scanner.bot = self
        self.report_loop_started = False

    async def setup_hook(self):
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)

            # Keep all commands guild-only. Clear and re-sync both scopes so
            # old global registrations from earlier versions are removed.
            self.tree.clear_commands(guild=guild)
            self.tree.copy_global_to(guild=guild)
            self.tree.clear_commands(guild=None)

            # Remove any previously registered global commands.
            global_synced = await self.tree.sync()
            synced = await self.tree.sync(guild=guild)

            print(
                f"Global command cleanup synced {len(global_synced)} commands; "
                f"synced {len(synced)} slash commands to guild {GUILD_ID}.",
                flush=True,
            )
            for command in synced:
                options = []
                for option in getattr(command, "options", []):
                    option_name = getattr(option, "name", "?")
                    option_type = getattr(option, "type", "?")
                    options.append(f"{option_name}:{option_type}")
                print(
                    f"Registered command /{command.name}"
                    + (f" ({', '.join(options)})" if options else ""),
                    flush=True,
                )
        else:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} global slash commands", flush=True)
        hourly_scan.start()
        daily_weekly_report.start()

    async def on_ready(self):
        print(f"Logged in as {self.user} (ID: {self.user.id})", flush=True)
        print(f"Monitoring kingdom {KINGDOM_ID} every hour", flush=True)


async def run_scan_and_report():
    try:
        result = await scanner.scan_once()
        print(f"Scan complete: {result[0]} alliances, {result[1]} players, {result[2]} changes, {result[3]} errors", flush=True)
    except Exception as exc:
        print(f"Scan failed: {exc}", flush=True)


@tasks.loop(seconds=SCAN_INTERVAL_SECONDS)
async def hourly_scan():
    await run_scan_and_report()


@hourly_scan.before_loop
async def before_hourly_scan():
    await bot.wait_until_ready()
    await run_scan_and_report()


async def notify_rank_alerts(rows):
    if not NOTIFY_CHANNEL_ID:
        return
    channel = bot.get_channel(NOTIFY_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(NOTIFY_CHANNEL_ID)
        except Exception:
            return
    for row in rows:
        await channel.send(f"**Kingdom 810 alliance rank movement**\n{row['abbr']}: **#{row['old_rank']} → #{row['new_rank']}**")


async def send_report(report_type: str):
    channel_id = DAILY_REPORT_CHANNEL_ID if report_type == "daily" else WEEKLY_REPORT_CHANNEL_ID
    if not channel_id:
        return
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except Exception as exc:
            print(f"Report channel error: {exc}", flush=True)
            return

    hours = 24 if report_type == "daily" else 24 * 7
    changes = await scanner.db.fetch_changes("all", hours, 500)
    levelups = [c for c in changes if c["change_type"] == "level"]
    namechanges = [c for c in changes if c["change_type"] == "name"]
    alliance_changes = [c for c in changes if c["change_type"] == "alliance"]
    joins = [c for c in changes if c["change_type"] == "joined_tracked"]
    departures = [c for c in changes if c["change_type"] == "left_tracked"]
    power = await scanner.db.power_changes(hours, 5)
    alliances = await scanner.db.get_alliances(100)

    embed = discord.Embed(
        title=f"Kingdom 810 {'Daily' if report_type == 'daily' else 'Weekly'} Report",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Tracked alliances", value=str(len(alliances)), inline=True)
    players, _, _, last_scan = await scanner.db.status()
    embed.add_field(name="Tracked players", value=str(players), inline=True)
    embed.add_field(name="Level increases", value=str(len(levelups)), inline=True)
    embed.add_field(name="Name changes", value=str(len(namechanges)), inline=True)
    embed.add_field(name="Alliance changes", value=str(len(alliance_changes)), inline=True)
    embed.add_field(name="Joined tracked", value=str(len(joins)), inline=True)
    embed.add_field(name="Left tracked", value=str(len(departures)), inline=True)
    if power:
        growth = "\n".join(f"{i+1}. {r['nick_name']} · {compact_number(r['growth'])}" for i, r in enumerate(power))
    else:
        growth = "No recorded power growth."
    embed.add_field(name="Top power growth", value=growth[:1024], inline=False)
    if last_scan:
        embed.set_footer(text=f"Last scan: {last_scan[0]}")
    await channel.send(embed=embed)
    await scanner.db.mark_report(report_type)


@tasks.loop(minutes=1)
async def daily_weekly_report():
    now = datetime.now(timezone.utc)
    # Reports are scheduled for exactly 00:05 UTC by default.
    if now.hour != REPORT_HOUR_UTC or now.minute != 5:
        return

    today = now.date().isoformat()
    daily_last = await scanner.db.report_sent("daily")
    if daily_last and daily_last[:10] == today:
        pass
    else:
        await send_report("daily")

    # Weekly report is sent every Monday at the same configured UTC hour.
    if now.weekday() == 0:
        weekly_last = await scanner.db.report_sent("weekly")
        if not weekly_last or weekly_last[:10] != today:
            await send_report("weekly")


@daily_weekly_report.before_loop
async def before_report_loop():
    await bot.wait_until_ready()


scanner = Scanner(Database(DB_PATH))
bot = ScannerBot(scanner)


@bot.tree.command(name="status", description="Show scanner status for kingdom 810")
async def status(interaction: discord.Interaction):
    players, alliances, changes, last_scan = await scanner.db.status()
    watched_p, watched_a = await scanner.db.watch_status()
    embed = discord.Embed(title="Kingdom 810 Scanner Status", color=discord.Color.blurple())
    embed.add_field(name="Tracked alliances", value=str(alliances), inline=True)
    embed.add_field(name="Tracked players", value=str(players), inline=True)
    embed.add_field(name="Recorded changes", value=str(changes), inline=True)
    embed.add_field(name="Watched players", value=str(watched_p), inline=True)
    embed.add_field(name="Watched alliances", value=str(watched_a), inline=True)
    embed.add_field(name="Scan interval", value="1 hour", inline=True)
    if last_scan:
        embed.add_field(name="Last scan", value=f"{last_scan[0]}\nAlliances: {last_scan[1]} · Players: {last_scan[2]} · Changes: {last_scan[3]} · Errors: {last_scan[4]}", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="scan", description="Run an immediate scan of the top 100 alliances")
async def scan(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    result = await scanner.scan_once()
    await interaction.followup.send(f"Scan finished. Alliances: {result[0]}, players: {result[1]}, changes: {result[2]}, errors: {result[3]}.", ephemeral=True)


@bot.tree.command(name="changes", description="Show recorded changes")
@app_commands.describe(change_type="Which change type", since_hours="Look back this many hours", limit="Maximum results")
@app_commands.choices(change_type=CHANGE_CHOICES)
async def changes(interaction: discord.Interaction, change_type: app_commands.Choice[str] | None = None, since_hours: app_commands.Range[int, 1, 720] = 24, limit: app_commands.Range[int, 1, 25] = 10):
    await interaction.response.defer(ephemeral=True)
    rows = await scanner.db.fetch_changes(change_type.value if change_type else "all", since_hours, limit)
    if not rows:
        await interaction.followup.send(f"No changes found in the last {since_hours} hours.", ephemeral=True)
        return
    embed = discord.Embed(title=f"Kingdom 810 Changes · {since_hours}h", color=discord.Color.orange())
    embed.description = "\n\n".join(f"**{r['nick_name'] or 'Unknown'}** · `{r['governor_id']}` · `{r['alliance_tag'] or '?'}`\n`{r['change_type']}`: `{r['old_value']}` → `{r['new_value']}`" for r in rows)
    await interaction.followup.send(embed=embed, ephemeral=True)




async def player_autocomplete(interaction: discord.Interaction, current: str):
    text = current.strip().lower()
    async with aiosqlite.connect(scanner.db.path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT governor_id,nick_name FROM players WHERE kid=? AND (LOWER(nick_name) LIKE ? OR governor_id LIKE ?) ORDER BY power DESC LIMIT 25",
            (KINGDOM_ID, f"%{text}%", f"%{current.strip()}%"),
        )
        rows = await cur.fetchall()
    return [app_commands.Choice(name=f"{r['nick_name']} · {r['governor_id']}", value=str(r['governor_id'])) for r in rows]


async def alliance_autocomplete(interaction: discord.Interaction, current: str):
    text = current.strip().lower()
    rows = await scanner.db.get_alliances(100)
    return [app_commands.Choice(name=f"{r['abbr']} · #{r['power_rank']}", value=r['abbr']) for r in rows if text in str(r['abbr']).lower()][:25]

async def resolve_player(identifier: str):
    identifier = identifier.strip()
    if identifier.isdigit():
        row = await scanner.db.get_player(identifier)
        return str(identifier), row
    row = await scanner.db.player_by_name(identifier)
    if row:
        return str(row["governor_id"]), row
    return identifier, None


@bot.tree.command(name="player", description="Show a tracked player's stored information")
@app_commands.describe(identifier="Governor ID or exact player name")
@app_commands.autocomplete(identifier=player_autocomplete)
async def player(interaction: discord.Interaction, identifier: str):
    row_id, row = await resolve_player(identifier)
    if not row:
        await interaction.response.send_message("Player not found in the tracked database.", ephemeral=True)
        return
    embed = discord.Embed(title=row["nick_name"] or "Unknown player", color=discord.Color.green())
    embed.add_field(name="Governor ID", value=str(row["governor_id"]), inline=True)
    embed.add_field(name="UID", value=str(row["uid"]), inline=True)
    embed.add_field(name="Kingdom", value=str(row["kid"]), inline=True)
    embed.add_field(name="Town Center", value=level_label(row["town_center_level"]), inline=True)
    embed.add_field(name="Power", value=compact_number(row["power"]), inline=True)
    embed.add_field(name="Kills", value=compact_number(row["kills"]), inline=True)
    embed.add_field(name="Alliance", value=f"{row['alliance_tag']} · {row['alliance_name']}", inline=False)
    embed.add_field(name="Online", value="Yes" if row["online"] else "No", inline=True)
    if row["avatar_url"]:
        embed.set_thumbnail(url=row["avatar_url"])
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="deepscan", description="Fetch all available data for a player")
@app_commands.describe(identifier="Player name, governor ID, or UID", id_type="Identifier type for numeric IDs")
@app_commands.autocomplete(identifier=player_autocomplete)
@app_commands.choices(id_type=[app_commands.Choice(name="Governor ID", value="governor_id"), app_commands.Choice(name="UID", value="uid")])
async def deepscan(interaction: discord.Interaction, identifier: str, id_type: app_commands.Choice[str] | None = None):
    await interaction.response.defer(ephemeral=True)
    lookup, row = await resolve_player(identifier)
    resolved = id_type.value if id_type else ("governor_id" if identifier.isdigit() or row else None)
    if row and not id_type:
        lookup = str(row["governor_id"])
    data = await scanner.api.get_player_full(lookup, resolved)
    if not data or not data.get("player"):
        await interaction.followup.send("Player not found or API request failed.", ephemeral=True)
        return
    pretty = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    file = discord.File(io.BytesIO(pretty.encode("utf-8")), filename=f"deepscan_{data.get('governor_id', lookup)}.json")
    player_data = data.get("player", {})
    embed = discord.Embed(title=f"Deep Scan · {player_data.get('nick_name', 'Unknown')}", color=discord.Color.blurple())
    embed.add_field(name="Governor ID", value=str(data.get("governor_id", "")), inline=True)
    embed.add_field(name="UID", value=str(data.get("uid", "")), inline=True)
    embed.add_field(name="Kingdom", value=str(player_data.get("kid", "")), inline=True)
    embed.add_field(name="Town Center", value=level_label(player_data.get("town_center_level")), inline=True)
    embed.add_field(name="Power", value=compact_number(player_data.get("power")), inline=True)
    await interaction.followup.send(embed=embed, file=file, ephemeral=True)


@bot.tree.command(name="recent", description="Show the most recent changes")
@app_commands.describe(limit="Maximum results")
async def recent(interaction: discord.Interaction, limit: app_commands.Range[int, 1, 25] = 10):
    await interaction.response.defer(ephemeral=True)
    rows = await scanner.db.fetch_changes("all", 24, limit)
    if not rows:
        await interaction.followup.send("No changes in the last 24 hours.", ephemeral=True)
        return
    await interaction.followup.send("\n".join(f"{r['nick_name']} · `{r['change_type']}` · `{r['old_value']}` → `{r['new_value']}`" for r in rows), ephemeral=True)


@bot.tree.command(name="levelups", description="Show Town Center level increases")
@app_commands.describe(hours="Look back this many hours", limit="Maximum results")
async def levelups(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 720] = 24, limit: app_commands.Range[int, 1, 25] = 20):
    await interaction.response.defer(ephemeral=True)
    rows = await scanner.db.fetch_changes("level", hours, limit * 2)
    rows = [r for r in rows if str(r["new_value"]).isdigit() and int(r["new_value"]) > int(r["old_value"])]
    rows = rows[:limit]
    if not rows:
        await interaction.followup.send("No level increases found.", ephemeral=True)
        return
    embed = discord.Embed(title=f"Kingdom 810 Level Ups · {hours}h", color=discord.Color.green())
    embed.description = "\n".join(f"**{r['nick_name']}** · {r['alliance_tag']} · `{level_label(int(r['old_value']))} → {level_label(int(r['new_value']))}`" for r in rows)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="namechanges", description="Show player name changes")
@app_commands.describe(hours="Look back this many hours", limit="Maximum results")
async def namechanges(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 720] = 24, limit: app_commands.Range[int, 1, 25] = 20):
    rows = await scanner.db.fetch_changes("name", hours, limit)
    if not rows:
        await interaction.response.send_message("No name changes found.", ephemeral=True)
        return
    await interaction.response.send_message("\n".join(f"{r['governor_id']} · `{r['old_value']}` → `{r['new_value']}` · {r['alliance_tag']}" for r in rows), ephemeral=True)


@bot.tree.command(name="alliancechanges", description="Show players who changed alliance")
@app_commands.describe(hours="Look back this many hours", limit="Maximum results")
async def alliancechanges(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 720] = 24, limit: app_commands.Range[int, 1, 25] = 20):
    rows = await scanner.db.fetch_changes("alliance", hours, limit)
    if not rows:
        await interaction.response.send_message("No alliance changes found.", ephemeral=True)
        return
    await interaction.response.send_message("\n".join(f"{r['nick_name']} · `{r['old_value']}` → `{r['new_value']}`" for r in rows), ephemeral=True)


@bot.tree.command(name="moves", description="Show player movements between alliances")
@app_commands.describe(hours="Look back this many hours", limit="Maximum results")
async def moves(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 720] = 24, limit: app_commands.Range[int, 1, 25] = 25):
    await alliancechanges(interaction, hours, limit)


@bot.tree.command(name="joined", description="Show players who joined tracked alliances")
@app_commands.describe(hours="Look back this many hours", limit="Maximum results")
async def joined(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 720] = 24, limit: app_commands.Range[int, 1, 25] = 25):
    rows = await scanner.db.fetch_changes("joined_tracked", hours, limit)
    await interaction.response.send_message("\n".join(f"{r['nick_name']} · `{r['governor_id']}` · {r['alliance_tag']}" for r in rows) or "No tracked joins found.", ephemeral=True)


@bot.tree.command(name="left", description="Show players who left tracked alliances")
@app_commands.describe(hours="Look back this many hours", limit="Maximum results")
async def left(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 720] = 24, limit: app_commands.Range[int, 1, 25] = 25):
    rows = await scanner.db.fetch_changes("left_tracked", hours, limit)
    await interaction.response.send_message("\n".join(f"{r['nick_name']} · `{r['governor_id']}` · {r['old_value']} → not tracked" for r in rows) or "No tracked departures found.", ephemeral=True)


@bot.tree.command(name="history", description="Show a player's stored hourly history")
@app_commands.describe(identifier="Governor ID or exact player name", hours="Look back this many hours")
@app_commands.autocomplete(identifier=player_autocomplete)
async def history(interaction: discord.Interaction, identifier: str, hours: app_commands.Range[int, 1, 720] = 168):
    gov_id, row = await resolve_player(identifier)
    if not row:
        await interaction.response.send_message("Player not found.", ephemeral=True)
        return
    snaps = await scanner.db.player_history(gov_id, hours, 100)
    changes = await scanner.db.fetch_changes("all", hours, 100)
    changes = [c for c in changes if str(c["governor_id"]) == gov_id]
    lines = [f"**{row['nick_name']} · {gov_id}**"]
    for s in snaps[-24:]:
        lines.append(f"`{s['captured_at']}` · {level_label(s['town_center_level'])} · {compact_number(s['power'])} · {s['alliance_tag']}")
    if changes:
        lines.append("\n**Changes**")
        lines.extend(f"{c['change_type']}: `{c['old_value']}` → `{c['new_value']}`" for c in changes[:20])
    await interaction.response.send_message("\n".join(lines)[:1900], ephemeral=True)


@bot.tree.command(name="inactive", description="List tracked players inactive for at least N days")
@app_commands.describe(days="Minimum inactivity", alliance="Optional exact alliance tag", limit="Maximum results")
@app_commands.autocomplete(alliance=alliance_autocomplete)
async def inactive(interaction: discord.Interaction, days: app_commands.Range[int, 1, 365] = 7, alliance: str | None = None, limit: app_commands.Range[int, 1, 50] = 25):
    rows = await scanner.db.inactive(days, limit, alliance)
    if not rows:
        await interaction.response.send_message("No matching inactive players.", ephemeral=True)
        return
    await interaction.response.send_message("\n".join(f"{r['governor_id']} · {r['nick_name']} · {r['alliance_tag']} · {level_label(r['town_center_level'])}" for r in rows), ephemeral=True)


@bot.tree.command(name="online", description="List currently online or offline tracked players")
@app_commands.describe(mode="Show online or offline players", alliance="Optional alliance tag", limit="Maximum results")
@app_commands.choices(mode=[app_commands.Choice(name="Online", value="online"), app_commands.Choice(name="Offline", value="offline")])
@app_commands.autocomplete(alliance=alliance_autocomplete)
async def online(interaction: discord.Interaction, mode: app_commands.Choice[str] | None = None, alliance: str | None = None, limit: app_commands.Range[int, 1, 50] = 25):
    rows = await scanner.db.current_online(mode.value != "offline" if mode else True, alliance, limit)
    await interaction.response.send_message("\n".join(f"{r['governor_id']} · {r['nick_name']} · {r['alliance_tag']} · {compact_number(r['power'])}" for r in rows) or "No matching players.", ephemeral=True)


@bot.tree.command(name="powerchanges", description="Show the biggest player power increases")
@app_commands.describe(hours="Look back this many hours", alliance="Optional alliance tag", limit="Maximum results")
async def powerchanges(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 720] = 24, alliance: str | None = None, limit: app_commands.Range[int, 1, 25] = 10):
    rows = await scanner.db.power_changes(hours, limit, alliance)
    if not rows:
        await interaction.response.send_message("No power growth found.", ephemeral=True)
        return
    await interaction.response.send_message("\n".join(f"**{i+1}. {r['nick_name']}** · {r['alliance_tag']} · {compact_number(r['old_power'])} → {compact_number(r['new_power'])} (**+{compact_number(r['growth'])}** )" for i, r in enumerate(rows)), ephemeral=True)


@bot.tree.command(name="powergrowth", description="Show a player's recorded power growth")
@app_commands.describe(identifier="Governor ID or exact player name", hours="Look back this many hours")
@app_commands.autocomplete(identifier=player_autocomplete)
async def powergrowth(interaction: discord.Interaction, identifier: str, hours: app_commands.Range[int, 1, 720] = 168):
    gov_id, row = await resolve_player(identifier)
    if not row:
        await interaction.response.send_message("Player not found.", ephemeral=True)
        return
    snaps = await scanner.db.player_history(gov_id, hours, 100)
    if len(snaps) < 2:
        await interaction.response.send_message("Not enough history yet.", ephemeral=True)
        return
    first, last = snaps[0], snaps[-1]
    growth = (last["power"] or 0) - (first["power"] or 0)
    await interaction.response.send_message(f"**{row['nick_name']}**\n{compact_number(first['power'])} → {compact_number(last['power'])}\nGrowth: **{growth:+,}** over {hours}h.", ephemeral=True)


@bot.tree.command(name="powergraph", description="Show a player's recent power history")
@app_commands.describe(identifier="Governor ID or exact player name", hours="Look back this many hours")
@app_commands.autocomplete(identifier=player_autocomplete)
async def powergraph(interaction: discord.Interaction, identifier: str, hours: app_commands.Range[int, 1, 720] = 168):
    gov_id, row = await resolve_player(identifier)
    if not row:
        await interaction.response.send_message("Player not found.", ephemeral=True)
        return
    snaps = await scanner.db.player_history(gov_id, hours, 24)
    if len(snaps) < 2:
        await interaction.response.send_message("Not enough history to graph this player.", ephemeral=True)
        return
    vals = [s["power"] or 0 for s in snaps]
    lo, hi = min(vals), max(vals)
    bars = "▁▂▃▄▅▆▇█"
    if hi == lo:
        chart = bars[0] * len(vals)
    else:
        chart = "".join(bars[min(7, int((v - lo) / (hi - lo) * 7))] for v in vals)
    await interaction.response.send_message(f"**{row['nick_name']} power history**\n`{chart}`\n{compact_number(vals[0])} → {compact_number(vals[-1])}\nLast {hours}h", ephemeral=True)


@bot.tree.command(name="truegold", description="Show recent Truegold progression")
@app_commands.describe(hours="Look back this many hours", alliance="Optional alliance tag", limit="Maximum results")
@app_commands.autocomplete(alliance=alliance_autocomplete)
async def truegold(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 720] = 24, alliance: str | None = None, limit: app_commands.Range[int, 1, 25] = 20):
    rows = await scanner.db.truegold_changes(hours, limit, alliance)
    if not rows:
        await interaction.response.send_message("No Truegold progression found.", ephemeral=True)
        return
    await interaction.response.send_message("\n".join(f"{r['nick_name']} · {r['alliance_tag']} · **{level_label(int(r['old_value']))} → {level_label(int(r['new_value']))}**" for r in rows), ephemeral=True)


@bot.tree.command(name="alliances", description="List tracked alliance tags")
async def alliances(interaction: discord.Interaction):
    rows = await scanner.db.get_alliances(100)
    content = ", ".join(dict.fromkeys((r["abbr"] or "").strip() for r in rows if r["abbr"]))
    if len(content) <= 1900:
        await interaction.response.send_message(content or "No alliances stored. Run /scan.", ephemeral=True)
        return
    await interaction.response.send_message(file=discord.File(io.BytesIO(content.encode("utf-8")), filename="alliances_810.txt"), ephemeral=True)


@bot.tree.command(name="alliance", description="Export one alliance's members as text")
@app_commands.describe(tag="Exact case-sensitive alliance tag")
async def alliance(interaction: discord.Interaction, tag: str):
    rows = await scanner.db.get_alliance_players(tag, 1000)
    if not rows:
        await interaction.response.send_message("No stored players found for that alliance tag.", ephemeral=True)
        return
    content = "\n".join(f"{r['governor_id'] or ''}, {r['nick_name'] or ''}, {r['town_center_level'] if r['town_center_level'] is not None else ''}, 810" for r in rows) + "\n"
    await interaction.response.send_message(file=discord.File(io.BytesIO(content.encode("utf-8")), filename=f"alliance_{tag}_810.txt"), ephemeral=True)


def csv_value(value):
    if value is None:
        return ""
    text = str(value)
    if any(ch in text for ch in [",", '"', "\n", "\r"]):
        return '"' + text.replace('"', '""') + '"'
    return text


def csv_line(row):
    return ",".join([
        csv_value(row["governor_id"]), csv_value(row["nick_name"]), csv_value(row["town_center_level"]), "810",
        csv_value(row["power"]), "", "", "",
    ])


def safe_tag(tag: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in tag)
    return clean or "alliance"


async def build_alliance_csv(tag: str, suffix: str = ""):
    rows = await scanner.db.get_alliance_players(tag, 1000)
    header = "ID,Name,TC Level,Kingdom,Power,Power Updated,Combat Power,Combat Power Updated"
    chunks = max(1, math.ceil(len(rows) / MAX_CSV_PLAYERS))
    result = []
    for i in range(chunks):
        part = rows[i * MAX_CSV_PLAYERS:(i + 1) * MAX_CSV_PLAYERS]
        content = header + "\n" + "\n".join(csv_line(r) for r in part) + "\n"
        number = f"_{i+1}" if chunks > 1 else ""
        result.append(discord.File(io.BytesIO(content.encode("utf-8-sig")), filename=f"alliance_{safe_tag(tag)}{number}_810.csv"))
    return result, len(rows)


@bot.tree.command(name="alliancefile", description="Export every tracked alliance as CSV files")
async def alliancefile(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    alliances_rows = await scanner.db.get_alliances(100)
    files = []
    for r in alliances_rows:
        f, _ = await build_alliance_csv(r["abbr"])
        files.extend(f)
    for i in range(0, len(files), 10):
        await interaction.followup.send(files=files[i:i+10], ephemeral=True)
    await interaction.followup.send(f"Finished. Created {len(files)} CSV files. Maximum {MAX_CSV_PLAYERS} players per file.", ephemeral=True)


@bot.tree.command(name="alliancefilemerged", description="Export top alliances separately and merge the rest")
@app_commands.describe(top_count="Number of top alliances to keep as individual files (1-99)")
async def alliancefilemerged(interaction: discord.Interaction, top_count: app_commands.Range[int, 1, 99]):
    await interaction.response.defer(ephemeral=True)
    rows = list((await scanner.db.get_alliances(100))[:100])
    top = rows[:top_count]
    rest = rows[top_count:]
    files = []
    for r in top:
        f, _ = await build_alliance_csv(r["abbr"])
        files.extend(f)
    merged_rows = []
    for r in rest:
        members = await scanner.db.get_alliance_players(r["abbr"], 1000)
        merged_rows.extend(members)
    header = "ID,Name,TC Level,Kingdom,Power,Power Updated,Combat Power,Combat Power Updated"
    chunks = max(1, math.ceil(len(merged_rows) / MAX_CSV_PLAYERS)) if merged_rows else 0
    for i in range(chunks):
        part = merged_rows[i * MAX_CSV_PLAYERS:(i + 1) * MAX_CSV_PLAYERS]
        content = header + "\n" + "\n".join(csv_line(r) for r in part) + "\n"
        name = "alliances_rest_810.csv" if chunks == 1 else f"alliances_rest_{i+1}_810.csv"
        files.append(discord.File(io.BytesIO(content.encode("utf-8-sig")), filename=name))
    for i in range(0, len(files), 10):
        await interaction.followup.send(files=files[i:i+10], ephemeral=True)
    await interaction.followup.send(f"Finished. Top {top_count} alliances are separate; {len(rest)} remaining alliances are merged. Maximum {MAX_CSV_PLAYERS} players per file.", ephemeral=True)


@bot.tree.command(name="alliancetop", description="Show the strongest players in an alliance")
@app_commands.describe(tag="Exact alliance tag", limit="Maximum players")
@app_commands.autocomplete(tag=alliance_autocomplete)
async def alliancetop(interaction: discord.Interaction, tag: str, limit: app_commands.Range[int, 1, 50] = 20):
    rows = await scanner.db.get_alliance_players(tag, limit)
    await interaction.response.send_message("\n".join(f"#{i+1} {r['nick_name']} · {compact_number(r['power'])} · {level_label(r['town_center_level'])}" for i, r in enumerate(rows)) or "No players found.", ephemeral=True)


@bot.tree.command(name="allianceinfo", description="Show detailed information about one alliance")
@app_commands.describe(tag="Exact alliance tag")
@app_commands.autocomplete(tag=alliance_autocomplete)
async def allianceinfo(interaction: discord.Interaction, tag: str):
    alliance, players = await scanner.db.alliance_stats(tag)
    if not alliance:
        await interaction.response.send_message("Alliance not found.", ephemeral=True)
        return
    powers = [p["power"] or 0 for p in players]
    levels = [p["town_center_level"] for p in players if p["town_center_level"] is not None]
    embed = discord.Embed(title=f"{alliance['abbr']} · {alliance['name']}", color=discord.Color.blue())
    embed.add_field(name="Rank", value=f"#{alliance['power_rank']}", inline=True)
    embed.add_field(name="Power", value=compact_number(alliance['power']), inline=True)
    embed.add_field(name="Members", value=str(alliance['member_count']), inline=True)
    embed.add_field(name="Average power", value=compact_number(sum(powers)/len(powers)) if powers else "0", inline=True)
    embed.add_field(name="Highest player", value=compact_number(max(powers)) if powers else "0", inline=True)
    for tg in range(10, 0, -1):
        start = 35 + (tg - 1) * 5
        embed.add_field(name=f"TG {tg}+", value=str(sum(1 for lv in levels if lv >= start)), inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="alliancecompare", description="Compare two tracked alliances")
@app_commands.describe(first="First alliance tag", second="Second alliance tag")
async def alliancecompare(interaction: discord.Interaction, first: str, second: str):
    a1, p1 = await scanner.db.alliance_stats(first)
    a2, p2 = await scanner.db.alliance_stats(second)
    if not a1 or not a2:
        await interaction.response.send_message("Both alliances must be tracked.", ephemeral=True)
        return
    avg1 = sum((p["power"] or 0) for p in p1) / max(1, len(p1))
    avg2 = sum((p["power"] or 0) for p in p2) / max(1, len(p2))
    await interaction.response.send_message(
        f"**{first} vs {second}**\nPower: {compact_number(a1['power'])} vs {compact_number(a2['power'])}\n"
        f"Members: {len(p1)} vs {len(p2)}\nAverage power: {compact_number(avg1)} vs {compact_number(avg2)}\n"
        f"Rank: #{a1['power_rank']} vs #{a2['power_rank']}", ephemeral=True)


@bot.tree.command(name="alliancehistory", description="Show alliance history")
@app_commands.describe(tag="Exact alliance tag", hours="Look back this many hours")
async def alliancehistory(interaction: discord.Interaction, tag: str, hours: app_commands.Range[int, 1, 720] = 168):
    alliance, _ = await scanner.db.alliance_stats(tag)
    if not alliance:
        await interaction.response.send_message("Alliance not found.", ephemeral=True)
        return
    snaps = await scanner.db.alliance_history(alliance["aid"], hours, 100)
    if len(snaps) < 2:
        await interaction.response.send_message("Not enough alliance history yet.", ephemeral=True)
        return
    first, last = snaps[0], snaps[-1]
    await interaction.response.send_message(
        f"**{tag} history**\nPower: {compact_number(first['power'])} → {compact_number(last['power'])} ({(last['power'] or 0)-(first['power'] or 0):+,})\n"
        f"Members: {first['member_count']} → {last['member_count']}\nRank: #{first['power_rank']} → #{last['power_rank']}\n"
        f"Snapshots: {len(snaps)}", ephemeral=True)


@bot.tree.command(name="alliancegraph", description="Show an alliance power history chart")
@app_commands.describe(tag="Exact alliance tag", hours="Look back this many hours")
async def alliancegraph(interaction: discord.Interaction, tag: str, hours: app_commands.Range[int, 1, 720] = 168):
    alliance, _ = await scanner.db.alliance_stats(tag)
    if not alliance:
        await interaction.response.send_message("Alliance not found.", ephemeral=True)
        return
    snaps = await scanner.db.alliance_history(int(alliance["aid"]), hours, 100)
    if len(snaps) < 2:
        await interaction.response.send_message("Not enough alliance history yet.", ephemeral=True)
        return
    vals = [s["power"] or 0 for s in snaps]
    lo, hi = min(vals), max(vals)
    bars = "▁▂▃▄▅▆▇█"
    chart = bars[0] * len(vals) if hi == lo else "".join(bars[min(7, int((v-lo)/(hi-lo)*7))] for v in vals)
    await interaction.response.send_message(f"**{tag} power history**\n`{chart}`\n{compact_number(vals[0])} → {compact_number(vals[-1])}\nLast {hours}h", ephemeral=True)


@bot.tree.command(name="alliancehealth", description="Calculate a tracked alliance health score")
@app_commands.describe(tag="Exact alliance tag")
@app_commands.autocomplete(tag=alliance_autocomplete)
async def alliancehealth(interaction: discord.Interaction, tag: str):
    alliance, players = await scanner.db.alliance_stats(tag)
    if not alliance:
        await interaction.response.send_message("Alliance not found.", ephemeral=True)
        return
    if not players:
        await interaction.response.send_message("No player data available.", ephemeral=True)
        return
    avg_power = sum((p["power"] or 0) for p in players) / len(players)
    inactive = sum(1 for p in players if p["last_active_at"] and p["last_active_at"] < (datetime.now(timezone.utc)-timedelta(days=7)).timestamp())
    high = sum(1 for p in players if (p["town_center_level"] or 0) >= 60)
    member_score = min(20, len(players) / 5)
    activity_score = max(0, 20 - inactive * 0.5)
    high_score = min(30, high * 0.6)
    avg_score = min(30, avg_power / 20_000_000)
    score = round(member_score + activity_score + high_score + avg_score)
    score = max(0, min(100, score))
    await interaction.response.send_message(f"**{tag} health score: {score}/100**\nAverage power: {compact_number(avg_power)}\nInactive 7d+: {inactive}\nTG 6+: {high}", ephemeral=True)


async def board_autocomplete(interaction: discord.Interaction, current: str):
    text = current.strip().lower()
    return [
        app_commands.Choice(name=name, value=value)
        for name, value in BOARD_OPTIONS
        if text in name.lower() or text in value.lower()
    ][:25]


@bot.tree.command(name="top", description="Show a Kingdom 810 leaderboard. Start typing to select a board.")
@app_commands.describe(board="Leaderboard", limit="Number of results")
@app_commands.autocomplete(board=board_autocomplete)
async def top(interaction: discord.Interaction, board: str, limit: app_commands.Range[int, 1, 100] = 20):
    await interaction.response.defer(ephemeral=True)
    data = await scanner.api.get(f"/kingdoms/{KINGDOM_ID}/ranks?board={urllib.parse.quote(board, safe='')}&limit={limit}")
    if not data:
        await interaction.followup.send("Leaderboard request failed.", ephemeral=True)
        return
    rows = (data.get("boards", [{}])[0].get("rows", []) if data.get("boards") else [])
    lines = []
    for r in rows[:limit]:
        label = r.get("abbr") or r.get("nick_name") or r.get("name") or "Unknown"
        score = r.get("score")
        lines.append(f"#{r.get('rank','?')} {label} · {compact_number(score)}")
    await interaction.followup.send("\n".join(lines) or "No leaderboard rows returned.", ephemeral=True)


@bot.tree.command(name="rankchanges", description="Show alliance rank changes")
@app_commands.describe(hours="Look back this many hours", limit="Maximum results")
async def rankchanges(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 720] = 24, limit: app_commands.Range[int, 1, 25] = 15):
    rows = await scanner.db.get_alliances(100)
    result = []
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    async with aiosqlite.connect(scanner.db.path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT aid,abbr,MIN(power_rank) old_rank,MAX(power_rank) new_rank FROM alliance_snapshots WHERE captured_at>=? GROUP BY aid,abbr HAVING old_rank!=new_rank ORDER BY ABS(new_rank-old_rank) DESC LIMIT ?",
            (since, limit),
        )
        result = await cur.fetchall()
    await interaction.response.send_message("\n".join(f"{r['abbr']} · #{r['old_rank']} → #{r['new_rank']}" for r in result) or "No alliance rank changes found.", ephemeral=True)


@bot.tree.command(name="alliancestats", description="Show a compact overview of top alliances")
async def alliancesstats(interaction: discord.Interaction):
    rows = await scanner.db.get_alliances(10)
    await interaction.response.send_message("\n".join(f"#{r['power_rank']} {r['abbr']} · {compact_number(r['power'])} · {r['member_count']} members" for r in rows), ephemeral=True)


@bot.tree.command(name="dashboard", description="Show the Kingdom 810 dashboard")
async def dashboard(interaction: discord.Interaction):
    players, alliances_count, changes, last_scan = await scanner.db.status()
    levelups_24 = await scanner.db.fetch_changes("level", 24, 500)
    tg_24 = [r for r in levelups_24 if str(r["new_value"]).isdigit() and int(r["new_value"]) >= 35 and int(r["new_value"]) > int(r["old_value"])]
    power = await scanner.db.power_changes(24, 1)
    moves_24 = await scanner.db.fetch_changes("alliance", 24, 500)
    embed = discord.Embed(title="Kingdom 810 Dashboard", color=discord.Color.blurple())
    embed.add_field(name="Tracked", value=f"{players:,} players\n{alliances_count} alliances", inline=True)
    embed.add_field(name="Last 24h", value=f"Level ups: {len(levelups_24)}\nTG progressions: {len(tg_24)}\nAlliance moves: {len(moves_24)}", inline=True)
    embed.add_field(name="Biggest power growth", value=(f"{power[0]['nick_name']} +{compact_number(power[0]['growth'])}" if power else "None"), inline=True)
    if last_scan:
        embed.set_footer(text=f"Last scan: {last_scan[0]}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="compare", description="Compare two tracked players")
@app_commands.describe(first="First player ID/name", second="Second player ID/name")
async def compare(interaction: discord.Interaction, first: str, second: str):
    _, a = await resolve_player(first)
    _, b = await resolve_player(second)
    if not a or not b:
        await interaction.response.send_message("Both players must be tracked.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"**{a['nick_name']} vs {b['nick_name']}**\n"
        f"Power: {compact_number(a['power'])} vs {compact_number(b['power'])}\n"
        f"Kills: {compact_number(a['kills'])} vs {compact_number(b['kills'])}\n"
        f"TC: {level_label(a['town_center_level'])} vs {level_label(b['town_center_level'])}\n"
        f"Alliance: {a['alliance_tag']} vs {b['alliance_tag']}", ephemeral=True)


@bot.tree.command(name="watch", description="Watch a player for future changes")
@app_commands.describe(identifier="Governor ID or exact player name")
async def watch(interaction: discord.Interaction, identifier: str):
    gov_id, row = await resolve_player(identifier)
    if not row:
        await interaction.response.send_message("Player must already be tracked.", ephemeral=True)
        return
    await scanner.db.add_watch_player(gov_id)
    await interaction.response.send_message(f"Now watching {row['nick_name']} ({gov_id}).", ephemeral=True)


@bot.tree.command(name="unwatch", description="Stop watching a player")
@app_commands.describe(identifier="Governor ID")
async def unwatch(interaction: discord.Interaction, identifier: str):
    gov_id, _ = await resolve_player(identifier)
    await scanner.db.remove_watch_player(gov_id)
    await interaction.response.send_message(f"Stopped watching {gov_id}.", ephemeral=True)


@bot.tree.command(name="watchalliance", description="Watch an alliance for changes")
@app_commands.describe(tag="Exact alliance tag")
async def watchalliance(interaction: discord.Interaction, tag: str):
    alliance_row, _ = await scanner.db.alliance_stats(tag)
    if not alliance_row:
        await interaction.response.send_message("Alliance must be tracked.", ephemeral=True)
        return
    await scanner.db.add_watch_alliance(int(alliance_row["aid"]), tag)
    await interaction.response.send_message(f"Now watching alliance {tag}.", ephemeral=True)


@bot.tree.command(name="unwatchalliance", description="Stop watching an alliance")
@app_commands.describe(tag="Exact alliance tag")
async def unwatchalliance(interaction: discord.Interaction, tag: str):
    alliance_row, _ = await scanner.db.alliance_stats(tag)
    if alliance_row:
        await scanner.db.remove_watch_alliance(int(alliance_row["aid"]))
    await interaction.response.send_message(f"Stopped watching {tag}.", ephemeral=True)


@bot.tree.command(name="watchlist", description="Show watched players and alliances")
async def watchlist(interaction: discord.Interaction):
    ps = await scanner.db.watched_players()
    als = await scanner.db.watched_alliances()
    text = "**Players**\n" + "\n".join(str(p["governor_id"]) for p in ps) + "\n\n**Alliances**\n" + "\n".join(a["abbr"] for a in als)
    await interaction.response.send_message(text[:1900], ephemeral=True)


@bot.tree.command(name="discover", description="Compare tracked top-100 alliances with kingdom totals and alliance-kills rankings")
async def discover(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    count, power_count, kill_count, extras = await scanner.discover()
    extra_names = ", ".join(r.get("abbr", "?") for r in extras[:25]) or "None"
    await interaction.followup.send(
        f"**Kingdom 810 alliance discovery**\nKingdom reports: **{count} alliances**\n"
        f"Top power board: {power_count}\nAlliance-kills board: {kill_count}\n"
        f"Known extra alliance IDs from kills board: {len(extras)}\n{extra_names}\n\n"
        "The scanner still monitors the top 100 by alliance power. Alliances below that are treated as untracked, not automatically inactive.",
        ephemeral=True)


@bot.tree.command(name="scannerhealth", description="Show scanner health and recent scan performance")
async def scannerhealth(interaction: discord.Interaction):
    players, alliances, changes, last_scan = await scanner.db.status()
    snapshots, alliance_snapshots = await scanner.db.scan_counts()
    if last_scan:
        finished = datetime.fromisoformat(last_scan[0])
        age = datetime.now(timezone.utc) - finished
        scan_info = f"Last scan: {age.total_seconds()/60:.1f} min ago\nErrors: {last_scan[4]}"
    else:
        scan_info = "No scans recorded"
    await interaction.response.send_message(
        f"**Scanner health**\n{scan_info}\nTracked players: {players:,}\nTracked alliances: {alliances}\n"
        f"Player snapshots: {snapshots:,}\nAlliance snapshots: {alliance_snapshots:,}", ephemeral=True)


@bot.tree.command(name="freshness", description="Show age of the stored tracking data")
async def freshness(interaction: discord.Interaction):
    players, alliances, changes, last_scan = await scanner.db.status()
    if not last_scan:
        await interaction.response.send_message("No scan has completed yet.", ephemeral=True)
        return
    finished = datetime.fromisoformat(last_scan[0])
    age = datetime.now(timezone.utc) - finished
    await interaction.response.send_message(f"**Kingdom 810 data freshness**\nLast completed scan: {age.total_seconds()/60:.1f} minutes ago.\nStored players: {players:,}\nStored alliances: {alliances}", ephemeral=True)


@bot.tree.command(name="inactivealliances", description="List tracked alliances with many inactive members")
@app_commands.describe(days="Inactivity threshold", limit="Maximum alliances")
async def inactivealliances(interaction: discord.Interaction, days: app_commands.Range[int, 1, 365] = 7, limit: app_commands.Range[int, 1, 25] = 15):
    rows = await scanner.db.get_alliances(100)
    result = []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
    for r in rows:
        members = await scanner.db.get_alliance_players(r["abbr"], 1000)
        inactive_count = sum(1 for m in members if m["last_active_at"] and m["last_active_at"] < cutoff)
        pct = inactive_count / max(1, len(members)) * 100
        result.append((pct, r["abbr"], len(members), inactive_count))
    result.sort(reverse=True)
    await interaction.response.send_message("\n".join(f"{tag}: {inactive}/{members} inactive ({pct:.0f}%)" for pct, tag, members, inactive in result[:limit]) or "No data.", ephemeral=True)


@bot.tree.command(name="truegoldtop", description="Show players with the highest current Truegold levels")
@app_commands.describe(limit="Maximum results")
async def truegoldtop(interaction: discord.Interaction, limit: app_commands.Range[int, 1, 50] = 20):
    async with aiosqlite.connect(scanner.db.path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM players WHERE kid=? AND town_center_level>=35 ORDER BY town_center_level DESC, power DESC LIMIT ?", (KINGDOM_ID, limit))
        rows = await cur.fetchall()
    await interaction.response.send_message("\n".join(f"#{i+1} {r['nick_name']} · {level_label(r['town_center_level'])} · {r['alliance_tag']}" for i, r in enumerate(rows)) or "No TG players tracked.", ephemeral=True)


@bot.tree.command(name="allianceexport", description="Export current tracked players as one CSV per selected alliance")
@app_commands.describe(tag="Exact case-sensitive alliance tag")
@app_commands.autocomplete(tag=alliance_autocomplete)
async def allianceexport(interaction: discord.Interaction, tag: str):
    files, count = await build_alliance_csv(tag)
    if not files:
        await interaction.response.send_message("No players found.", ephemeral=True)
        return
    await interaction.response.send_message(content=f"{count} players exported.", files=files[:10], ephemeral=True)


@bot.tree.command(name="dailyreport", description="Send the daily report now")
async def dailyreport(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await send_report("daily")
    await interaction.followup.send("Daily report sent.", ephemeral=True)


@bot.tree.command(name="weeklyreport", description="Send the weekly report now")
async def weeklyreport(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await send_report("weekly")
    await interaction.followup.send("Weekly report sent.", ephemeral=True)



@bot.tree.command(name="kvkopponent", description="Show opponent links, top 5 alliances, top 20 hero-power players, and an optional kingdom comparison")
@app_commands.describe(
    kingdom="Opponent kingdom number",
    compare="Also compare the opponent with Kingdom 810",
    send_to_kvk_opponent="Send the full result to the configured #kvk-opponent channel",
    detailed_players="Number of top players to show in the detailed comparison (1-50)",
)
async def kvkopponent(
    interaction: discord.Interaction,
    kingdom: app_commands.Range[int, 1, 99999],
    compare: bool = False,
    send_to_kvk_opponent: bool = False,
    detailed_players: app_commands.Range[int, 1, 50] = 5,
):
    await interaction.response.defer(ephemeral=send_to_kvk_opponent)

    target_channel = None
    if send_to_kvk_opponent:
        if not KVK_OPPONENT_CHANNEL_ID:
            await interaction.followup.send(
                "KVK_OPPONENT_CHANNEL_ID is not configured in Compose.",
                ephemeral=True,
            )
            return
        target_channel = bot.get_channel(KVK_OPPONENT_CHANNEL_ID)
        if target_channel is None:
            try:
                target_channel = await bot.fetch_channel(KVK_OPPONENT_CHANNEL_ID)
            except Exception as exc:
                print(f"Could not fetch KVK opponent channel: {exc}", flush=True)
                await interaction.followup.send(
                    "I could not access the configured #kvk-opponent channel.",
                    ephemeral=True,
                )
                return

    async def send_section(content: str):
        if not content:
            return
        if target_channel is not None:
            await target_channel.send(content)
        else:
            await interaction.followup.send(content, ephemeral=False)

    # Top 5 alliances by alliance power and top 20 players by Hero Total.
    public_opponent = await scanner.api.get_public_kingdom(kingdom)
    alliances_data = await scanner.api.get(f"/kingdoms/{kingdom}/ranks?board=alliance_power&limit=5")
    opponent_players_data = await scanner.api.get(f"/kingdoms/{kingdom}/ranks?board=hero_total&limit=100")

    opponent_kingdom_data = None
    our_kingdom_data = None
    our_public_kingdom = None
    our_players_data = None
    if compare:
        our_public_kingdom = await scanner.api.get_public_kingdom(KINGDOM_ID)
        opponent_kingdom_data = await scanner.api.get(f"/kingdoms/{kingdom}")
        our_kingdom_data = await scanner.api.get(f"/kingdoms/{KINGDOM_ID}")
        our_players_data = await scanner.api.get(f"/kingdoms/{KINGDOM_ID}/ranks?board=hero_total&limit=100")

    if not alliances_data and not opponent_players_data and not opponent_kingdom_data:
        await interaction.followup.send(
            f"Could not retrieve data for Kingdom {kingdom}.",
            ephemeral=True,
        )
        return

    alliance_rows = []
    if alliances_data and alliances_data.get("boards"):
        alliance_rows = alliances_data["boards"][0].get("rows", [])[:5]

    opponent_top100 = []
    if opponent_players_data and opponent_players_data.get("boards"):
        opponent_top100 = opponent_players_data["boards"][0].get("rows", [])[:100]
    opponent_top20 = opponent_top100[:20]

    lines = [f"**Kingdom {kingdom}**", f"<https://mightpulse.com/kingdom/{kingdom}>"]

    lines.append("\n**Top 5 alliances**")
    if alliance_rows:
        for row in alliance_rows:
            tag = str(row.get("abbr") or "").strip()
            if tag:
                safe = urllib.parse.quote(tag, safe="")
                lines.append(
                    f"#{row.get('rank', '?')} {tag} — <https://mightpulse.com/{kingdom}/{safe}>"
                )
    else:
        lines.append("No alliance leaderboard data returned.")

    lines.append("\n**Top 20 players · Hero Power**")
    if opponent_top20:
        for row in opponent_top20:
            uid = row.get("uid")
            name = row.get("nick_name") or "Unknown"
            score = compact_number(row.get("score"))
            player_link = f" — <https://mightpulse.com/player/{uid}>" if uid else ""
            lines.append(f"#{row.get('rank', '?')} {name} · {score}{player_link}")
    else:
        lines.append("No hero-power leaderboard data returned.")

    if compare:
        def kingdom_obj(data):
            if not isinstance(data, dict):
                return {}
            if isinstance(data.get("kingdom"), dict):
                return data["kingdom"]
            return data

        ours = kingdom_obj(our_public_kingdom or our_kingdom_data)
        opp = kingdom_obj(public_opponent or opponent_kingdom_data)

        # Kingdom-level comparison intentionally excludes troop power.
        # For these comparisons, use only the top 100 player-ranked contribution
        # for each kingdom rather than the kingdom-wide totals. Troop Power is excluded.
        async def get_rank_rows(board: str, kingdom_id: int):
            data = await scanner.api.get(f"/kingdoms/{kingdom_id}/ranks?board={board}&limit=100")
            return data.get("boards", [{}])[0].get("rows", [])[:100] if data else []

        async def top100_sum(board: str, kingdom_id: int):
            rows = await get_rank_rows(board, kingdom_id)
            return sum((row.get("score") or 0) for row in rows)

        metric_specs = [
            ("hero_total", "Hero Power"),
            ("research_power", "Research Power"),
            ("gov_gear", "Governor Gear"),
            ("gov_charm", "Governor Charm"),
            ("pet_power", "Pet Power"),
        ]
        metric_jobs = []
        for board, label in metric_specs:
            metric_jobs.append(top100_sum(board, KINGDOM_ID))
            metric_jobs.append(top100_sum(board, kingdom))
        metric_results = await asyncio.gather(*metric_jobs)
        top100_metrics = {
            label: (metric_results[i * 2], metric_results[i * 2 + 1])
            for i, (_, label) in enumerate(metric_specs)
        }

        lines.append("\n**Quick Kingdom Comparison · no troop power**")
        lines.append("```text")
        lines.append(f"{'Metric':<22} {'810':>14} {str(kingdom):>14}")
        lines.append(f"{'Power':<22} {compact_number(ours.get('power')):>14} {compact_number(opp.get('power')):>14}")
        lines.append(f"{'Average Power':<22} {compact_number(ours.get('avg_power')):>14} {compact_number(opp.get('avg_power')):>14}")
        lines.append(f"{'Players':<22} {ours.get('player_count', 0):>14,} {opp.get('player_count', 0):>14,}")
        lines.append(f"{'Active 7d':<22} {ours.get('active_7d', 0):>14,} {opp.get('active_7d', 0):>14,}")
        lines.append(f"{'Alliances':<22} {ours.get('alliance_count', 0):>14,} {opp.get('alliance_count', 0):>14,}")
        lines.append(f"{'Kills':<22} {compact_number(ours.get('kills')):>14} {compact_number(opp.get('kills')):>14}")
        for label in ["Hero Power", "Research Power", "Governor Gear", "Governor Charm", "Pet Power"]:
            left, right = top100_metrics[label]
            lines.append(f"{label:<22} {compact_number(left):>14} {compact_number(right):>14}")
        lines.append("```")

        # Compare the top 20 Hero Total players roughly by rank. Troop power is
        # deliberately excluded from all player comparison output.
        our_top100 = []
        if our_players_data and our_players_data.get("boards"):
            our_top100 = our_players_data["boards"][0].get("rows", [])[:100]
        our_top20 = our_top100[:20]

        # Kingdom-wide Truegold distribution from the same public endpoint used
        # by the MightPulse kingdom page. The documented /v1 town_center board is
        # capped at 100, so it cannot reproduce the full kingdom-wide counts.
        def public_tg_distribution(public_data):
            counts = {}
            if not isinstance(public_data, dict):
                return counts, None
            pyramid = public_data.get("pyramid") or {}
            tg_rows = pyramid.get("tg") if isinstance(pyramid, dict) else None
            if not isinstance(tg_rows, list):
                return counts, None
            tc_total = None
            for item in tg_rows:
                if not isinstance(item, dict):
                    continue
                label = str(item.get("label") or "").strip().upper()
                count = item.get("count")
                if count is None:
                    continue
                if label == "TC":
                    tc_total = int(count)
                elif label.startswith("TG"):
                    suffix = label[2:]
                    try:
                        counts[int(suffix)] = int(count)
                    except ValueError:
                        continue
            return counts, tc_total

        our_public_tg, our_public_tc = public_tg_distribution(our_public_kingdom)
        opp_public_tg, opp_public_tc = public_tg_distribution(public_opponent)

        lines.append("\n**Kingdom-wide Town Center distribution**")
        lines.append("```text")
        tg_labels = sorted(set(our_public_tg) | set(opp_public_tg), reverse=True)
        if tg_labels:
            left = "   ".join(f"TG {tg} × {our_public_tg.get(tg, 0)}" for tg in tg_labels)
            right = "   ".join(f"TG {tg} × {opp_public_tg.get(tg, 0)}" for tg in tg_labels)
            lines.append(f"810      {left}")
            lines.append(f"{kingdom:<8} {right}")
            if our_public_tc is not None or opp_public_tc is not None:
                lines.append(f"TC       {our_public_tc if our_public_tc is not None else '-':>8}   {opp_public_tc if opp_public_tc is not None else '-':>8}")
        else:
            lines.append("No kingdom-wide TG distribution was returned by the MightPulse kingdom endpoint.")
        lines.append("```")

        # Top-100 Hero Power Truegold distribution. This remains a separate
        # statistic because it describes only the strongest 100 Hero Power players.
        def tg_bucket(level):
            try:
                level = int(level)
            except (TypeError, ValueError):
                return None
            if level < 35:
                return None
            return ((level - 35) // 5) + 1

        async def get_top100_board_index(board: str, kingdom_id: int):
            rows = await get_rank_rows(board, kingdom_id)
            index = {}
            for row in rows:
                score = row.get("score")
                if score is None:
                    continue
                for key in ("governor_id", "uid"):
                    value = row.get(key)
                    if value is not None:
                        index[str(value)] = score
            return index

        # Top-100 Hero Power Truegold distribution should use the documented API.
        # Kingdom 810 can use the local scanner database; opponent players are
        # resolved through the documented /players/{uid}?include=base endpoint.
        # The public website endpoint is NOT used for this statistic.
        our_db_players = await scanner.db.get_players_by_governor_ids(
            [str(row.get("governor_id")) for row in our_top100 if row.get("governor_id") is not None]
        )
        our_tg_top100 = {}
        for row in our_top100:
            saved = our_db_players.get(str(row.get("governor_id")))
            if saved:
                tg = tg_bucket(saved["town_center_level"])
                if tg is not None:
                    our_tg_top100[tg] = our_tg_top100.get(tg, 0) + 1

        async def opponent_top100_tg_distribution(rows):
            counts = {}

            async def resolve(row):
                uid = row.get("uid")
                gov_id = row.get("governor_id")
                if uid is None and gov_id is None:
                    return None

                # Prefer UID because the hero_total board already exposes it.
                if uid is not None:
                    data = await scanner.api.get_player_base(str(uid), "uid")
                else:
                    data = await scanner.api.get_player_base(str(gov_id), "governor_id")

                if not data:
                    return None
                player = data.get("player") or {}
                return player.get("town_center_level")

            levels = await asyncio.gather(*(resolve(row) for row in rows[:100]))
            for level in levels:
                tg = tg_bucket(level)
                if tg is not None:
                    counts[tg] = counts.get(tg, 0) + 1
            return counts

        opp_tg_top100 = await opponent_top100_tg_distribution(opponent_top100)

        lines.append("\n**Top 100 Hero Power · Truegold distribution**")
        lines.append("```text")
        tg_keys = sorted(set(our_tg_top100) | set(opp_tg_top100), reverse=True)
        if tg_keys:
            left = "   ".join(f"TG {tg} × {our_tg_top100.get(tg, 0)}" for tg in tg_keys)
            right = "   ".join(f"TG {tg} × {opp_tg_top100.get(tg, 0)}" for tg in tg_keys)
            lines.append(f"810      {left}")
            lines.append(f"{kingdom:<8} {right}")
        else:
            lines.append("No TG players found in the top 100 Hero Power rankings.")
        lines.append("```")

        lines.append("\n**Top 20 Hero Power comparison · rough**")
        lines.append("```text")
        lines.append(f"{'#':>2}  {'Kingdom 810':<24} {'Opponent':<24}")
        for idx in range(20):
            left = our_top20[idx] if idx < len(our_top20) else {}
            right = opponent_top20[idx] if idx < len(opponent_top20) else {}
            left_name = (left.get("nick_name") or "-")[:14]
            right_name = (right.get("nick_name") or "-")[:14]
            left_score_val = left.get("score") if left else None
            right_score_val = right.get("score") if right else None
            left_score = compact_number(left_score_val) if left else "-"
            right_score = compact_number(right_score_val) if right else "-"
            left_mark = "✓" if left_score_val is not None and (right_score_val is None or left_score_val > right_score_val) else " "
            right_mark = "✓" if right_score_val is not None and (left_score_val is None or right_score_val > left_score_val) else " "
            lines.append(
                f"{idx + 1:>2}  {left_mark}{left_name:<14} {left_score:>7}   {right_mark}{right_name:<14} {right_score:>7}"
            )
        lines.append("```")

        # Fetch the component leaderboards once and match the top-5 players to
        # their scores by governor ID/UID. The individual player API does not
        # expose research/governor-gear/charm/pet power directly, while these
        # kingdom ranking boards do.
        component_boards = [
            ("research_power", "Research"),
            ("gov_gear", "Gov. Gear"),
            ("gov_charm", "Gov. Charm"),
            ("pet_power", "Pet Power"),
        ]

        async def build_rank_index(kingdom_id: int):
            results = await asyncio.gather(
                *(get_rank_rows(board, kingdom_id) for board, _ in component_boards)
            )
            index = {label: {} for _, label in component_boards}
            for (board, label), rows in zip(component_boards, results):
                for rank_row in rows:
                    score = rank_row.get("score")
                    if score is None:
                        continue
                    for key in ("governor_id", "uid"):
                        value = rank_row.get(key)
                        if value is not None:
                            index[label][str(value)] = score
            return index

        our_component_index, opp_component_index = await asyncio.gather(
            build_rank_index(KINGDOM_ID),
            build_rank_index(kingdom),
        )

        # Detailed comparison. Fetch full player data for the requested number
        # of players from each kingdom, capped at 50 per side.
        async def fetch_detail(row):
            if not row:
                return {}
            uid = row.get("uid")
            governor_id = row.get("governor_id")
            try:
                # Prefer UID because the hero leaderboard supplies it, but fall
                # back to governor_id if the UID lookup is unavailable.
                if uid:
                    data = await scanner.api.get_player_full(str(uid), "uid")
                    if data and data.get("player"):
                        return data
                if governor_id:
                    data = await scanner.api.get_player_full(str(governor_id), "governor_id")
                    if data and data.get("player"):
                        return data
                print(
                    f"Top-5 detail lookup returned no player for uid={uid}, governor_id={governor_id}",
                    flush=True,
                )
            except Exception as exc:
                print(
                    f"Top-5 detail lookup failed for uid={uid}, governor_id={governor_id}: {exc}",
                    flush=True,
                )
            return {}

        detailed_count = min(int(detailed_players), 50)
        max_available = max(len(our_top100), len(opponent_top100))
        detailed_count = min(detailed_count, max_available)
        pair_rows = [
            (
                our_top100[idx] if idx < len(our_top100) else None,
                opponent_top100[idx] if idx < len(opponent_top100) else None,
            )
            for idx in range(detailed_count)
        ]
        detail_results = await asyncio.gather(
            *(fetch_detail(row) if row else asyncio.sleep(0, result={})
              for pair in pair_rows for row in pair)
        )
        detail_rows = []
        for idx, (our_row, opp_row) in enumerate(pair_rows):
            our_detail = detail_results[idx * 2]
            opp_detail = detail_results[idx * 2 + 1]
            detail_rows.append((our_row or {}, our_detail, opp_row or {}, opp_detail))

        def detail_player(row, data):
            return data.get("player", {}) if isinstance(data, dict) else {}

        def detail_value(player, key, default="-"):
            value = player.get(key)
            return default if value is None else value

        def detail_ranks(data):
            return data.get("ranks", {}) if isinstance(data, dict) else {}

        def hero_gear_summary(hero):
            gear = hero.get("gear")
            if not isinstance(gear, list):
                return []
            parts = []
            for item in gear:
                if not isinstance(item, dict):
                    continue
                slot = item.get("slot") or item.get("name") or "Gear"
                enh = item.get("enhancement_level")
                ref = item.get("refine_level")
                if enh is None and ref is None:
                    parts.append(f"{slot}")
                elif ref is None:
                    parts.append(f"{slot} +{enh}")
                elif enh is None:
                    parts.append(f"{slot} R{ref}")
                else:
                    parts.append(f"{slot} +{enh}/R{ref}")
            return parts

        def hero_block(data):
            heroes = data.get("heroes") if isinstance(data, dict) else None
            if not isinstance(heroes, list):
                return ["Arena: unavailable"]

            out = ["Arena"]
            shown = 0
            for position, hero in enumerate(heroes[:5], 1):
                if not isinstance(hero, dict):
                    continue
                shown += 1
                name = hero.get("name") or "Unknown"
                hero_power = hero.get("power")
                power = compact_number(hero_power) if hero_power is not None else "-"
                widget = hero.get("exclusive_gear_level")
                out.append(f"{position}. {name}" + (f" · {power}" if power != "-" else ""))
                out.append(f"   Widget: {widget if widget is not None else '-'}")
                gear_parts = hero_gear_summary(hero)
                if gear_parts:
                    for gear_line in gear_parts:
                        out.append(f"   {gear_line}")
                else:
                    out.append("   Gear: unavailable")

            if shown == 0:
                out.append("Unavailable")
            return out

        # Build the detailed top-5 comparison as independent player blocks.
        # Each block is sent as its own public Discord message so Discord cannot
        # split or truncate the detailed information.
        detail_blocks = []
        for idx, (our_row, our_detail, opp_row, opp_detail) in enumerate(detail_rows, 1):
            ours_player = detail_player(our_row, our_detail)
            opp_player = detail_player(opp_row, opp_detail)
            ours_alliance = ours_player.get("alliance") or {}
            opp_alliance = opp_player.get("alliance") or {}
            ours_ranks = detail_ranks(our_detail)
            opp_ranks = detail_ranks(opp_detail)

            ours_name = ours_player.get("nick_name") or our_row.get("nick_name") or "-"
            opp_name = opp_player.get("nick_name") or opp_row.get("nick_name") or "-"
            ours_tag = ours_alliance.get("abbr") or our_row.get("alliance_abbr") or "-"
            opp_tag = opp_alliance.get("abbr") or opp_row.get("alliance_abbr") or "-"

            ours_mystic = ours_ranks.get("mystic_trial", "-")
            opp_mystic = opp_ranks.get("mystic_trial", "-")
            ours_mystic_rank = ours_ranks.get("mystic_rank", "-")
            opp_mystic_rank = opp_ranks.get("mystic_rank", "-")

            def leaderboard_component_value(row, player, index, label):
                # The ranking rows normally expose both governor_id and uid.
                for source in (row, player):
                    if not isinstance(source, dict):
                        continue
                    for key in ("governor_id", "uid"):
                        value = source.get(key)
                        if value is not None:
                            score = index.get(label, {}).get(str(value))
                            if score is not None:
                                return score
                return None

            ours_metric_values = {
                "Hero Power": our_row.get("score"),
                "Research": leaderboard_component_value(our_row, ours_player, our_component_index, "Research"),
                "Gov. Gear": leaderboard_component_value(our_row, ours_player, our_component_index, "Gov. Gear"),
                "Gov. Charm": leaderboard_component_value(our_row, ours_player, our_component_index, "Gov. Charm"),
                "Pet Power": leaderboard_component_value(our_row, ours_player, our_component_index, "Pet Power"),
            }
            opp_metric_values = {
                "Hero Power": opp_row.get("score"),
                "Research": leaderboard_component_value(opp_row, opp_player, opp_component_index, "Research"),
                "Gov. Gear": leaderboard_component_value(opp_row, opp_player, opp_component_index, "Gov. Gear"),
                "Gov. Charm": leaderboard_component_value(opp_row, opp_player, opp_component_index, "Gov. Charm"),
                "Pet Power": leaderboard_component_value(opp_row, opp_player, opp_component_index, "Pet Power"),
            }

            def metric_line(label, ours_value, opp_value):
                # Do not use fixed-width columns here. Discord mobile wraps
                # whitespace aggressively, which makes aligned tables unreadable.
                ours_text = compact_number(ours_value) if ours_value is not None else "-"
                opp_text = compact_number(opp_value) if opp_value is not None else "-"
                if ours_value is not None and opp_value is not None:
                    if ours_value > opp_value:
                        return f"    {label}: 810 {ours_text} ✓ · {kingdom} {opp_text}"
                    if opp_value > ours_value:
                        return f"    {label}: 810 {ours_text} · {kingdom} {opp_text} ✓"
                    return f"    {label}: 810 {ours_text} = {kingdom} {opp_text}"
                if ours_value is not None:
                    return f"    {label}: 810 {ours_text} ✓ · {kingdom} -"
                if opp_value is not None:
                    return f"    {label}: 810 - · {kingdom} {opp_text} ✓"
                return f"    {label}: 810 - · {kingdom} -"

            block = [
                f"#{idx}",
                f"810 [{ours_tag}]{ours_name}",
                f"    {level_label(detail_value(ours_player, 'town_center_level'))}",
                f"    Mystic Trial: {ours_mystic} · Rank: {ours_mystic_rank}",
                "",
                f"{kingdom} [{opp_tag}]{opp_name}",
                f"    {level_label(detail_value(opp_player, 'town_center_level'))}",
                f"    Mystic Trial: {opp_mystic} · Rank: {opp_mystic_rank}",
                "",
                "    Power comparison",
                metric_line("Hero Power", ours_metric_values["Hero Power"], opp_metric_values["Hero Power"]),
                metric_line("Research", ours_metric_values["Research"], opp_metric_values["Research"]),
                metric_line("Gov. Gear", ours_metric_values["Gov. Gear"], opp_metric_values["Gov. Gear"]),
                metric_line("Gov. Charm", ours_metric_values["Gov. Charm"], opp_metric_values["Gov. Charm"]),
                metric_line("Pet Power", ours_metric_values["Pet Power"], opp_metric_values["Pet Power"]),
                "",
            ]

            # Keep the Arena details grouped under each kingdom after the compact
            # player/power comparison so the important matchup numbers are easy
            # to find.
            block.extend(hero_block(our_detail))
            block.append("")
            block.extend(hero_block(opp_detail))
            detail_blocks.append("\n".join(block))


    def _font(size: int, bold: bool = False):
        candidates = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        ]
        for path in candidates:
            if os.path.exists(path):
                return ImageFont.truetype(path, size)
        return ImageFont.load_default()

    def _draw_wrapped(draw, text, xy, font, fill=(20, 20, 20), max_width=1400, line_gap=6):
        x, y = xy
        words = str(text).split()
        lines = []
        current = ""
        for word in words:
            test = word if not current else current + " " + word
            if draw.textbbox((0, 0), test, font=font)[2] <= max_width:
                current = test
            else:
                if current:
                    lines.append(current)
                current = word
        if current:
            lines.append(current)
        for line in lines:
            draw.text((x, y), line, font=font, fill=fill)
            y += draw.textbbox((0, 0), line, font=font)[3] - draw.textbbox((0, 0), line, font=font)[1] + line_gap
        return y

    def _metric_winner(a, b):
        if a is None or b is None:
            return ""
        if a > b:
            return "810"
        if b > a:
            return str(kingdom)
        return "="

    def _render_kvk_images(detail_rows, kingdom_id):
        """Render mobile-readable, plain comparison images.

        Two player matchups per image.  The previous version packed five very
        tall cards into a single 1500px-wide image, which Discord scaled down
        aggressively on phones and made the text microscopic.
        """
        images = []
        if not detail_rows:
            return images

        # Deliberately large fonts. Discord will scale the image to the phone's
        # available width, so readability depends much more on font size than
        # on trying to squeeze everything into one enormous image.
        title_font = _font(34, True)
        subtitle_font = _font(24, True)
        section_font = _font(24, True)
        body_font = _font(24)
        body_bold = _font(24, True)
        small_font = _font(21)
        gear_font = _font(19)

        width = 1200
        chunk_size = 2
        margin = 30
        gap = 18
        card_height = 690

        def component_value(row, player, index, label):
            for source in (row, player):
                if not isinstance(source, dict):
                    continue
                for key in ("governor_id", "uid"):
                    value = source.get(key)
                    if value is not None:
                        score = index.get(label, {}).get(str(value))
                        if score is not None:
                            return score
            return None

        def player_info(row, data):
            player = data.get("player", {}) if isinstance(data, dict) else {}
            ranks = data.get("ranks", {}) if isinstance(data, dict) else {}
            alliance = player.get("alliance") or {}
            name = player.get("nick_name") or row.get("nick_name") or "-"
            tag = alliance.get("abbr") or row.get("alliance_abbr") or "-"
            level = player.get("town_center_level")
            mystic = ranks.get("mystic_trial")
            mystic_rank = ranks.get("mystic_rank")
            return player, name, tag, level, mystic, mystic_rank

        def heroes(data):
            hs = data.get("heroes") if isinstance(data, dict) else None
            return hs[:5] if isinstance(hs, list) else []

        def gear(hero):
            vals = {}
            for item in hero.get("gear") or []:
                if not isinstance(item, dict):
                    continue
                slot = str(item.get("slot") or item.get("name") or "").lower()
                enh = item.get("enhancement_level")
                ref = item.get("refine_level")
                if enh is None and ref is None:
                    value = "-"
                elif ref is None:
                    value = f"+{enh}"
                elif enh is None:
                    value = f"R{ref}"
                else:
                    value = f"+{enh}/R{ref}"
                for key in ("helmet", "gloves", "armor", "boots"):
                    if key in slot:
                        vals[key] = value
            return vals

        def draw_hero_column(draw, x, y, hero_list):
            for pos in range(5):
                hero = hero_list[pos] if pos < len(hero_list) else None
                if hero:
                    g = gear(hero)
                    widget = hero.get("exclusive_gear_level")
                    widget_text = f"Widget: {widget}" if widget is not None else "Widget: -"
                    draw.text(
                        (x, y),
                        f"{pos + 1}. {hero.get('name', 'Unknown')} · {widget_text}",
                        font=small_font,
                        fill=(25, 25, 25),
                    )
                    draw.text(
                        (x + 18, y + 29),
                        f"Helmet {g.get('helmet', '-')}   ·   Gloves {g.get('gloves', '-')}",
                        font=gear_font,
                        fill=(75, 75, 75),
                    )
                    draw.text(
                        (x + 18, y + 53),
                        f"Armor {g.get('armor', '-')}   ·   Boots {g.get('boots', '-')}",
                        font=gear_font,
                        fill=(75, 75, 75),
                    )
                else:
                    draw.text((x, y), f"{pos + 1}. -", font=small_font, fill=(100, 100, 100))
                y += 83

        for chunk_start in range(0, len(detail_rows), chunk_size):
            chunk = detail_rows[chunk_start:chunk_start + chunk_size]
            height = 145 + len(chunk) * (card_height + gap)
            img = Image.new("RGB", (width, height), "white")
            draw = ImageDraw.Draw(img)

            y = 22
            draw.text((margin, y), "KvK Opponent Comparison", font=title_font, fill=(15, 15, 15))
            y += 43
            draw.text(
                (margin, y),
                f"Kingdom 810 vs Kingdom {kingdom_id} · #{chunk_start + 1}–#{chunk_start + len(chunk)}",
                font=subtitle_font,
                fill=(45, 45, 45),
            )
            y += 38
            draw.line((margin, y, width - margin, y), fill=(180, 180, 180), width=2)
            y += 16

            for row_offset, (orow, od, prow, pd) in enumerate(chunk):
                card_top = y
                card_bottom = card_top + card_height
                draw.rectangle(
                    (margin, card_top, width - margin, card_bottom),
                    outline=(190, 190, 190),
                    width=2,
                )
                draw.rectangle(
                    (margin, card_top, width - margin, card_top + 45),
                    fill=(235, 235, 235),
                )
                idx = chunk_start + row_offset + 1
                draw.text((margin + 14, card_top + 8), f"#{idx}", font=section_font, fill=(15, 15, 15))

                col_gap = 28
                col_w = (width - 2 * margin - col_gap) // 2
                left_x = margin + 18
                right_x = left_x + col_w + col_gap
                content_y = card_top + 61

                op, oname, otag, olvl, omystic, orank = player_info(orow, od)
                pp, pname, ptag, plvl, pmystic, prank = player_info(prow, pd)

                draw.text((left_x, content_y), f"810 [{otag}]{oname}", font=body_bold, fill=(15, 15, 15))
                draw.text((right_x, content_y), f"{kingdom_id} [{ptag}]{pname}", font=body_bold, fill=(15, 15, 15))
                content_y += 34

                draw.text((left_x, content_y), level_label(olvl), font=body_font, fill=(35, 35, 35))
                draw.text((right_x, content_y), level_label(plvl), font=body_font, fill=(35, 35, 35))
                content_y += 31

                draw.text(
                    (left_x, content_y),
                    f"Mystic Trial: {omystic if omystic is not None else '-'} · Rank: {orank if orank is not None else '-'}",
                    font=small_font,
                    fill=(60, 60, 60),
                )
                draw.text(
                    (right_x, content_y),
                    f"Mystic Trial: {pmystic if pmystic is not None else '-'} · Rank: {prank if prank is not None else '-'}",
                    font=small_font,
                    fill=(60, 60, 60),
                )
                content_y += 38

                draw.text((left_x, content_y), "Power Comparison", font=section_font, fill=(20, 20, 20))
                draw.text((right_x, content_y), "Power Comparison", font=section_font, fill=(20, 20, 20))
                content_y += 34

                metrics = [
                    ("Hero Power", orow.get("score"), prow.get("score")),
                    ("Research", component_value(orow, op, our_component_index, "Research"), component_value(prow, pp, opp_component_index, "Research")),
                    ("Gov. Gear", component_value(orow, op, our_component_index, "Gov. Gear"), component_value(prow, pp, opp_component_index, "Gov. Gear")),
                    ("Gov. Charm", component_value(orow, op, our_component_index, "Gov. Charm"), component_value(prow, pp, opp_component_index, "Gov. Charm")),
                    ("Pet Power", component_value(orow, op, our_component_index, "Pet Power"), component_value(prow, pp, opp_component_index, "Pet Power")),
                ]

                for label, a, b in metrics:
                    winner = _metric_winner(a, b)
                    at = compact_number(a) if a is not None else "-"
                    bt = compact_number(b) if b is not None else "-"
                    if winner == "=":
                        left_mark, right_mark = "=", "="
                    elif winner == "810":
                        left_mark, right_mark = "✓", ""
                    elif winner:
                        left_mark, right_mark = "", "✓"
                    else:
                        left_mark, right_mark = "", ""
                    draw.text((left_x, content_y), f"{label}: {at} {left_mark}", font=small_font, fill=(30, 30, 30))
                    draw.text((right_x, content_y), f"{label}: {bt} {right_mark}", font=small_font, fill=(30, 30, 30))
                    content_y += 27

                content_y += 8
                draw.text((left_x, content_y), "Arena · 810", font=section_font, fill=(20, 20, 20))
                draw.text((right_x, content_y), f"Arena · {kingdom_id}", font=section_font, fill=(20, 20, 20))
                content_y += 31

                draw_hero_column(draw, left_x, content_y, heroes(od))
                draw_hero_column(draw, right_x, content_y, heroes(pd))

                y = card_bottom + gap

            img = img.crop((0, 0, width, y - gap + 12))
            bio = io.BytesIO()
            img.save(bio, format="PNG", optimize=True)
            bio.seek(0)
            images.append(bio)

        return images

    # Send deliberate public sections. The initial links, kingdom comparison,
    # rough top-20 comparison, and each detailed top-5 block are separate
    # messages so Discord does not split a code block in the middle.
    text = "\n".join(lines)
    compare_marker = "**Quick Kingdom Comparison · no troop power**"
    rough_marker = "**Top 20 Hero Power comparison · rough**"

    first_end = text.find(compare_marker)
    if first_end < 0:
        first_end = len(text)

    first = text[:first_end].strip()
    if first:
        await send_section(first)

    if compare_marker in text:
        compare_start = text.find(compare_marker)
        rough_start = text.find(rough_marker, compare_start)
        comparison = text[compare_start:rough_start if rough_start >= 0 else len(text)].strip()
        if comparison:
            await send_section(comparison)

        rough = text[rough_start:].strip() if rough_start >= 0 else ""
        if rough:
            await send_section(rough)

        if detail_blocks:
            try:
                report_images = _render_kvk_images(detail_rows, kingdom)
                if report_images:
                    for image_index, image_buffer in enumerate(report_images, 1):
                        filename = f"kvk_{KINGDOM_ID}_vs_{kingdom}_part_{image_index}.png"
                        file = discord.File(image_buffer, filename=filename)
                        if target_channel is not None:
                            await target_channel.send(file=file)
                        else:
                            await interaction.followup.send(file=file, ephemeral=False)
                else:
                    await send_section(f"**Top {len(detail_blocks)} Hero Power comparison · detailed**")
            except Exception as exc:
                print(f"Could not render KvK comparison image: {exc}", flush=True)
                await send_section(f"**Top {len(detail_blocks)} Hero Power comparison · detailed**")

    if target_channel is not None:
        await interaction.followup.send(
            f"Sent the Kingdom {kingdom} KVK comparison to <#{KVK_OPPONENT_CHANNEL_ID}>.",
            ephemeral=True,
        )


@bot.tree.command(name="webhooktest", description="Send a test notification")
async def webhooktest(interaction: discord.Interaction):
    if not NOTIFY_CHANNEL_ID:
        await interaction.response.send_message("NOTIFY_CHANNEL_ID is not configured.", ephemeral=True)
        return
    channel = bot.get_channel(NOTIFY_CHANNEL_ID)
    if channel is None:
        channel = await bot.fetch_channel(NOTIFY_CHANNEL_ID)
    await channel.send("Player Scanner notification test successful.")
    await interaction.response.send_message("Test notification sent.", ephemeral=True)


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
