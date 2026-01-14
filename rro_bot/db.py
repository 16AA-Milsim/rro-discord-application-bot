from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

import aiosqlite


SCHEMA_VERSION = 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class ApplicationRecord:
    topic_id: int
    discord_channel_id: int
    discord_message_id: int
    discord_message_missing: bool
    discord_thread_id: int | None
    discord_control_message_id: int | None
    claimed_by_user_id: int | None
    tags_last_seen: list[str]
    topic_title: str | None
    topic_author: str | None
    topic_synced_at: str | None
    thread_name_history: list[str]
    tags_last_written: list[str] | None
    tags_written_at: str | None
    accepted_at: str | None
    archive_status: str | None
    archive_scheduled_at: str | None
    archived_at: str | None
    archive_in_progress: bool
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
                    discord_message_missing INTEGER NOT NULL DEFAULT 0,
                    discord_thread_id INTEGER,
                    discord_control_message_id INTEGER,
                    claimed_by_user_id INTEGER,
                    tags_last_seen TEXT NOT NULL,
                    topic_title TEXT,
                    topic_author TEXT,
                    topic_synced_at TEXT,
                    thread_name_history TEXT,
                    tags_last_written TEXT,
                    tags_written_at TEXT,
                    accepted_at TEXT,
                    archive_status TEXT,
                    archive_scheduled_at TEXT,
                    archived_at TEXT,
                    archive_in_progress INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            current_version = await self._get_user_version(db)
            if current_version < SCHEMA_VERSION:
                await self._migrate_schema(db, current_version)
                await self._set_user_version(db, SCHEMA_VERSION)
            await db.commit()

    @staticmethod
    async def _get_user_version(db: aiosqlite.Connection) -> int:
        async with db.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    @staticmethod
    async def _set_user_version(db: aiosqlite.Connection, version: int) -> None:
        await db.execute(f"PRAGMA user_version = {int(version)}")

    @staticmethod
    async def _migrate_schema(db: aiosqlite.Connection, current_version: int) -> None:
        if current_version < 1:
            # Lightweight migrations for existing DBs.
            for statement in (
                "ALTER TABLE applications ADD COLUMN discord_control_message_id INTEGER",
                "ALTER TABLE applications ADD COLUMN discord_message_missing INTEGER",
                "ALTER TABLE applications ADD COLUMN accepted_at TEXT",
                "ALTER TABLE applications ADD COLUMN archive_scheduled_at TEXT",
                "ALTER TABLE applications ADD COLUMN archived_at TEXT",
                "ALTER TABLE applications ADD COLUMN archive_status TEXT",
                "ALTER TABLE applications ADD COLUMN archive_in_progress INTEGER",
                "ALTER TABLE applications ADD COLUMN topic_title TEXT",
                "ALTER TABLE applications ADD COLUMN topic_author TEXT",
                "ALTER TABLE applications ADD COLUMN topic_synced_at TEXT",
                "ALTER TABLE applications ADD COLUMN thread_name_history TEXT",
            ):
                try:
                    await db.execute(statement)
                except Exception:
                    pass

    async def upsert_application(
        self,
        *,
        topic_id: int,
        discord_channel_id: int,
        discord_message_id: int,
        discord_thread_id: int | None,
        tags_last_seen: list[str],
        topic_title: str | None,
        topic_author: str | None,
        topic_synced_at: str | None,
    ) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                """
                INSERT INTO applications (
                    topic_id, discord_channel_id, discord_message_id, discord_message_missing, discord_thread_id,
                    discord_control_message_id,
                    claimed_by_user_id, tags_last_seen, topic_title, topic_author, topic_synced_at, thread_name_history,
                    tags_last_written, tags_written_at,
                    accepted_at, archive_status, archive_scheduled_at, archived_at, archive_in_progress,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, 0, ?, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, 0, ?, ?)
                ON CONFLICT(topic_id) DO UPDATE SET
                    discord_channel_id=excluded.discord_channel_id,
                    discord_message_id=excluded.discord_message_id,
                    discord_thread_id=excluded.discord_thread_id,
                    tags_last_seen=excluded.tags_last_seen,
                    topic_title=excluded.topic_title,
                    topic_author=excluded.topic_author,
                    topic_synced_at=excluded.topic_synced_at,
                    updated_at=excluded.updated_at
                """,
                (
                    topic_id,
                    discord_channel_id,
                    discord_message_id,
                    discord_thread_id,
                    json.dumps(tags_last_seen),
                    topic_title,
                    topic_author,
                    topic_synced_at,
                    json.dumps([]),
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

    async def get_application_by_message_id(self, message_id: int) -> ApplicationRecord | None:
        async with aiosqlite.connect(self._path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM applications WHERE discord_message_id=?",
                (message_id,),
            ) as cur:
                row = await cur.fetchone()
            if not row:
                return None
            return self._row_to_record(row)

    async def get_application_by_thread_id(self, thread_id: int) -> ApplicationRecord | None:
        async with aiosqlite.connect(self._path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM applications WHERE discord_thread_id=?",
                (thread_id,),
            ) as cur:
                row = await cur.fetchone()
            if not row:
                return None
            return self._row_to_record(row)

    async def get_application_by_control_message_id(self, message_id: int) -> ApplicationRecord | None:
        async with aiosqlite.connect(self._path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM applications WHERE discord_control_message_id=?",
                (message_id,),
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

    async def set_message_missing(self, *, topic_id: int, missing: bool) -> None:
        now = _now_iso()
        value = 1 if missing else 0
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET discord_message_missing=?, updated_at=? WHERE topic_id=?",
                (value, now, topic_id),
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

    async def set_topic_snapshot(
        self,
        *,
        topic_id: int,
        title: str | None,
        author: str | None,
        tags: list[str],
        synced_at: str,
    ) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                """
                UPDATE applications
                SET tags_last_seen=?, topic_title=?, topic_author=?, topic_synced_at=?, updated_at=?
                WHERE topic_id=?
                """,
                (json.dumps(tags), title, author, synced_at, now, topic_id),
            )
            await db.commit()

    async def set_topic_title(self, *, topic_id: int, title: str | None) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET topic_title=?, updated_at=? WHERE topic_id=?",
                (title, now, topic_id),
            )
            await db.commit()

    async def set_topic_synced_at(self, *, topic_id: int, synced_at: str) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET topic_synced_at=?, updated_at=? WHERE topic_id=?",
                (synced_at, now, topic_id),
            )
            await db.commit()

    async def set_thread_name_history(self, *, topic_id: int, names: list[str]) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET thread_name_history=?, updated_at=? WHERE topic_id=?",
                (json.dumps(names), now, topic_id),
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

    async def set_archive_status(self, *, topic_id: int, status: str | None) -> None:
        now = _now_iso()
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET archive_status=?, updated_at=? WHERE topic_id=?",
                (status, now, topic_id),
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

    async def set_archive_in_progress(self, *, topic_id: int, in_progress: bool) -> None:
        now = _now_iso()
        value = 1 if in_progress else 0
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                "UPDATE applications SET archive_in_progress=?, updated_at=? WHERE topic_id=?",
                (value, now, topic_id),
            )
            await db.commit()

    async def delete_application(self, *, topic_id: int) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute("DELETE FROM applications WHERE topic_id=?", (topic_id,))
            await db.commit()

    @staticmethod
    def _row_to_record(row: Any) -> ApplicationRecord:
        tags_last_seen = json.loads(row["tags_last_seen"]) if row["tags_last_seen"] else []
        tags_last_written = (
            json.loads(row["tags_last_written"]) if row["tags_last_written"] else None
        )
        thread_name_history = (
            json.loads(row["thread_name_history"]) if row["thread_name_history"] else []
        )
        return ApplicationRecord(
            topic_id=int(row["topic_id"]),
            discord_channel_id=int(row["discord_channel_id"]),
            discord_message_id=int(row["discord_message_id"]),
            discord_message_missing=bool(row["discord_message_missing"])
            if "discord_message_missing" in row.keys()
            else False,
            discord_thread_id=int(row["discord_thread_id"]) if row["discord_thread_id"] else None,
            discord_control_message_id=BotDb._safe_int(row, "discord_control_message_id"),
            claimed_by_user_id=int(row["claimed_by_user_id"]) if row["claimed_by_user_id"] else None,
            tags_last_seen=tags_last_seen,
            topic_title=row["topic_title"] if "topic_title" in row.keys() else None,
            topic_author=row["topic_author"] if "topic_author" in row.keys() else None,
            topic_synced_at=row["topic_synced_at"] if "topic_synced_at" in row.keys() else None,
            thread_name_history=thread_name_history,
            tags_last_written=tags_last_written,
            tags_written_at=row["tags_written_at"],
            accepted_at=row["accepted_at"] if "accepted_at" in row.keys() else None,
            archive_status=row["archive_status"] if "archive_status" in row.keys() else None,
            archive_scheduled_at=row["archive_scheduled_at"] if "archive_scheduled_at" in row.keys() else None,
            archived_at=row["archived_at"] if "archived_at" in row.keys() else None,
            archive_in_progress=bool(row["archive_in_progress"]) if "archive_in_progress" in row.keys() else False,
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
