import asyncio
import io
import json
import math
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
MAX_CSV_PLAYERS = 399
REPORT_HOUR_UTC = int(os.getenv("REPORT_HOUR_UTC", "0"))
RANK_ALERT_THRESHOLD = int(os.getenv("RANK_ALERT_THRESHOLD", "5"))
REPORT_CHANNEL_ID = int(os.getenv("REPORT_CHANNEL_ID", "0") or 0)
DAILY_REPORT_CHANNEL_ID = int(os.getenv("DAILY_REPORT_CHANNEL_ID", str(REPORT_CHANNEL_ID)) or 0)
WEEKLY_REPORT_CHANNEL_ID = int(os.getenv("WEEKLY_REPORT_CHANNEL_ID", str(REPORT_CHANNEL_ID)) or 0)
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

BOARD_CHOICES = [
    app_commands.Choice(name="Alliance Power", value="alliance_power"),
    app_commands.Choice(name="Alliance Kills", value="alliance_kills"),
    app_commands.Choice(name="Personal Power", value="personal_power"),
    app_commands.Choice(name="Kills", value="kills"),
    app_commands.Choice(name="Town Center", value="town_center"),
    app_commands.Choice(name="Rebel Conquest", value="rebel_conquest"),
    app_commands.Choice(name="Single Hero", value="single_hero"),
    app_commands.Choice(name="Hero Total", value="hero_total"),
    app_commands.Choice(name="Troop Power", value="troop_power"),
    app_commands.Choice(name="Building Power", value="building_power"),
    app_commands.Choice(name="Research Power", value="research_power"),
    app_commands.Choice(name="Hero No Equip", value="hero_no_equip"),
    app_commands.Choice(name="Hero Equip", value="hero_equip"),
    app_commands.Choice(name="Governor Gear", value="gov_gear"),
    app_commands.Choice(name="Governor Charm", value="gov_charm"),
    app_commands.Choice(name="Pet Power", value="pet_power"),
    app_commands.Choice(name="Island Prosperity", value="island_prosperity"),
    app_commands.Choice(name="Migrant Score", value="migrant_score"),
    app_commands.Choice(name="Mystic Trial", value="mystic_trial"),
    app_commands.Choice(name="Coliseum", value="coliseum"),
    app_commands.Choice(name="Forest of Life", value="forest_of_life"),
    app_commands.Choice(name="Crystal Cave", value="crystal_cave"),
    app_commands.Choice(name="Knowledge Nexus", value="knowledge_nexus"),
    app_commands.Choice(name="Molten Fort", value="molten_fort"),
    app_commands.Choice(name="Radiant Spire", value="radiant_spire"),
    app_commands.Choice(name="Master Power", value="master_power"),
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
            self.tree.clear_commands(guild=guild)
            self.tree.copy_global_to(guild=guild)
            self.tree.clear_commands(guild=None)
            await self.tree.sync()
            synced = await self.tree.sync(guild=guild)
            print(f"Synced {len(synced)} slash commands to guild {GUILD_ID}", flush=True)
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


@bot.tree.command(name="top", description="Show a Kingdom 810 leaderboard")
@app_commands.describe(board="Leaderboard", limit="Number of results")
@app_commands.choices(board=BOARD_CHOICES)
async def top(interaction: discord.Interaction, board: app_commands.Choice[str], limit: app_commands.Range[int, 1, 100] = 20):
    await interaction.response.defer(ephemeral=True)
    data = await scanner.api.get(f"/kingdoms/{KINGDOM_ID}/ranks?board={urllib.parse.quote(board.value, safe='')}&limit={limit}")
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



@bot.tree.command(name="kvkopponent", description="Show links to a kingdom, its top 5 alliances, and top 10 players")
@app_commands.describe(kingdom="Opponent kingdom number")
async def kvkopponent(interaction: discord.Interaction, kingdom: app_commands.Range[int, 1, 99999]):
    await interaction.response.defer(ephemeral=True)

    # Fetch top alliances and top players from the opponent kingdom.
    alliances_data = await scanner.api.get(f"/kingdoms/{kingdom}/ranks?board=alliance_power&limit=5")
    players_data = await scanner.api.get(f"/kingdoms/{kingdom}/ranks?board=personal_power&limit=10")

    if not alliances_data and not players_data:
        await interaction.followup.send(
            f"Could not retrieve leaderboard data for Kingdom {kingdom}.",
            ephemeral=True,
        )
        return

    alliance_rows = []
    if alliances_data and alliances_data.get("boards"):
        alliance_rows = alliances_data["boards"][0].get("rows", [])[:5]

    player_rows = []
    if players_data and players_data.get("boards"):
        player_rows = players_data["boards"][0].get("rows", [])[:10]

    lines = [f"**Kingdom {kingdom}**", f"<https://mightpulse.com/kingdom/{kingdom}>"]

    lines.append("\n**Top 5 alliances**")
    if alliance_rows:
        for row in alliance_rows:
            tag = str(row.get("abbr") or "").strip()
            if tag:
                safe = urllib.parse.quote(tag, safe="")
                lines.append(f"#{row.get('rank', '?')} {tag} — <https://mightpulse.com/{kingdom}/{safe}>")
    else:
        lines.append("No alliance leaderboard data returned.")

    lines.append("\n**Top 10 players**")
    if player_rows:
        for row in player_rows:
            uid = row.get("uid")
            name = row.get("nick_name") or "Unknown"
            if uid:
                lines.append(f"#{row.get('rank', '?')} {name} — <https://mightpulse.com/player/{uid}>")
            else:
                lines.append(f"#{row.get('rank', '?')} {name}")
    else:
        lines.append("No personal-power leaderboard data returned.")

    await interaction.followup.send("\n".join(lines), ephemeral=True)


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
