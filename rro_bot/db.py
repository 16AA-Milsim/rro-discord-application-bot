from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

import aiosqlite


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class ApplicationRecord:
    topic_id: int
    discord_channel_id: int
    discord_message_id: int
    discord_thread_id: int | None
    discord_control_message_id: int | None
    claimed_by_user_id: int | None
    tags_last_seen: list[str]
    tags_last_written: list[str] | None
    tags_written_at: str | None
    accepted_at: str | None
    archive_scheduled_at: str | None
    archived_at: str | None
    created_at: str
    updated_at: str


class BotDb:
    def __init__(self, path: str) -> None:
        self._path = path

    async def init(self) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS applications (
                    topic_id INTEGER PRIMARY KEY,
                    discord_channel_id INTEGER NOT NULL,
                    discord_message_id INTEGER NOT NULL,
                    discord_thread_id INTEGER,
                    discord_control_message_id INTEGER,
                    claimed_by_user_id INTEGER,
                    tags_last_seen TEXT NOT NULL,
                    tags_last_written TEXT,
                    tags_written_at TEXT,
                    accepted_at TEXT,
                    archive_scheduled_at TEXT,
                    archived_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            # Lightweight migrations for existing DBs
            try:
                await db.execute("ALTER TABLE applications ADD COLUMN discord_control_message_id INTEGER")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE applications ADD COLUMN accepted_at TEXT")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE applications ADD COLUMN archive_scheduled_at TEXT")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE applications ADD COLUMN archived_at TEXT")
            except Exception:
                pass
            await db.commit()

    async def upsert_application(
        self,
        *,
        topic_id: int,
        discord_channel_id: int,
        discord_message_id: int,
        discord_thread_id: int | None,
        tags_last_seen: list[str],
    ) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                """
                INSERT INTO applications (
                    topic_id, discord_channel_id, discord_message_id, discord_thread_id,
                    discord_control_message_id,
                    claimed_by_user_id, tags_last_seen, tags_last_written, tags_written_at,
                    accepted_at, archive_scheduled_at, archived_at,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL, ?, ?)
                ON CONFLICT(topic_id) DO UPDATE SET
                    discord_channel_id=excluded.discord_channel_id,
                    discord_message_id=excluded.discord_message_id,
                    discord_thread_id=excluded.discord_thread_id,
                    tags_last_seen=excluded.tags_last_seen,
                    updated_at=excluded.updated_at
                """,
                (
                    topic_id,
                    discord_channel_id,
                    discord_message_id,
                    discord_thread_id,
                    json.dumps(tags_last_seen),
                    now,
                    now,
                ),
            )
            await db.commit()

    async def get_application(self, topic_id: int) -> ApplicationRecord | None:
        async with aiosqlite.connect(self._path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM applications WHERE topic_id=?",
                (topic_id,),
            ) as cur:
                row = await cur.fetchone()
            if not row:
                return None
            return self._row_to_record(row)

    async def list_applications(self) -> list[ApplicationRecord]:
        async with aiosqlite.connect(self._path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM applications") as cur:
                rows = await cur.fetchall()
                return [self._row_to_record(r) for r in rows]

    async def try_claim(self, *, topic_id: int, user_id: int) -> bool:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            cur = await db.execute(
                """
                UPDATE applications
                SET claimed_by_user_id=?, updated_at=?
                WHERE topic_id=? AND claimed_by_user_id IS NULL
                """,
                (user_id, now, topic_id),
            )
            await db.commit()
            return cur.rowcount == 1

    async def force_claim(self, *, topic_id: int, user_id: int | None) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET claimed_by_user_id=?, updated_at=? WHERE topic_id=?",
                (user_id, now, topic_id),
            )
            await db.commit()

    async def set_thread_id(self, *, topic_id: int, thread_id: int | None) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET discord_thread_id=?, updated_at=? WHERE topic_id=?",
                (thread_id, now, topic_id),
            )
            await db.commit()

    async def set_control_message_id(self, *, topic_id: int, message_id: int | None) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET discord_control_message_id=?, updated_at=? WHERE topic_id=?",
                (message_id, now, topic_id),
            )
            await db.commit()

    async def set_tags_last_seen(self, *, topic_id: int, tags: list[str]) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET tags_last_seen=?, updated_at=? WHERE topic_id=?",
                (json.dumps(tags), now, topic_id),
            )
            await db.commit()

    async def set_tags_last_written(self, *, topic_id: int, tags: list[str]) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                """
                UPDATE applications
                SET tags_last_written=?, tags_written_at=?, updated_at=?
                WHERE topic_id=?
                """,
                (json.dumps(tags), now, now, topic_id),
            )
            await db.commit()

    async def mark_accepted(self, *, topic_id: int, accepted: bool) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET accepted_at=?, updated_at=? WHERE topic_id=?",
                (now if accepted else None, now, topic_id),
            )
            await db.commit()

    async def schedule_archive(self, *, topic_id: int, when_iso: str | None) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET archive_scheduled_at=?, updated_at=? WHERE topic_id=?",
                (when_iso, now, topic_id),
            )
            await db.commit()

    async def mark_archived(self, *, topic_id: int, archived: bool) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET archived_at=?, updated_at=? WHERE topic_id=?",
                (now if archived else None, now, topic_id),
            )
            await db.commit()

    @staticmethod
    def _row_to_record(row: Any) -> ApplicationRecord:
        tags_last_seen = json.loads(row["tags_last_seen"]) if row["tags_last_seen"] else []
        tags_last_written = (
            json.loads(row["tags_last_written"]) if row["tags_last_written"] else None
        )
        return ApplicationRecord(
            topic_id=int(row["topic_id"]),
            discord_channel_id=int(row["discord_channel_id"]),
            discord_message_id=int(row["discord_message_id"]),
            discord_thread_id=int(row["discord_thread_id"]) if row["discord_thread_id"] else None,
            discord_control_message_id=BotDb._safe_int(row, "discord_control_message_id"),
            claimed_by_user_id=int(row["claimed_by_user_id"]) if row["claimed_by_user_id"] else None,
            tags_last_seen=tags_last_seen,
            tags_last_written=tags_last_written,
            tags_written_at=row["tags_written_at"],
            accepted_at=row["accepted_at"] if "accepted_at" in row.keys() else None,
            archive_scheduled_at=row["archive_scheduled_at"] if "archive_scheduled_at" in row.keys() else None,
            archived_at=row["archived_at"] if "archived_at" in row.keys() else None,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _safe_int(row: Any, key: str) -> int | None:
        try:
            value = row[key]
        except Exception:
            return None
        return int(value) if value else None
