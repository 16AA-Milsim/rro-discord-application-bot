from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import logging
import os
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiohttp
from aiohttp import web
import discord

from .config import BotConfig, load_config
from .db import ApplicationRecord, BotDb
from .discourse import DiscourseClient, DiscourseTopic
from .render import (
    STAGE_TAGS_DISCOURSE,
    build_application_embed,
    discourse_tags_to_discord,
    discourse_tags_to_stage_label,
)
from .views import ApplicationView, RenameTopicModal


log = logging.getLogger("rro_bot")

LOG_TAG_STATUS = ":small_orange_diamond: STATUS"
LOG_TAG_NOTE = ":speech_balloon: NOTE"
LOG_TAG_ASSIGN = ":small_blue_diamond: ASSIGN"
LOG_TAG_SYSTEM = ":gear: SYSTEM"


def _configure_logging() -> None:
    def _env_bool(name: str, default: bool = False) -> bool:
        raw = os.environ.get(name)
        if raw is None:
            return default
        value = raw.strip().lower()
        if value in ("1", "true", "yes", "y", "on"):
            return True
        if value in ("0", "false", "no", "n", "off"):
            return False
        return default

    def _env_int(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    level_name = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    level = logging.getLevelName(level_name)
    if not isinstance(level, int):
        level = logging.INFO

    log_file = os.environ.get("LOG_FILE", "logs/bot.log").strip()
    log_to_console = _env_bool("LOG_TO_CONSOLE", True)
    max_bytes = _env_int("LOG_MAX_BYTES", 10 * 1024 * 1024)
    backup_count = _env_int("LOG_BACKUP_COUNT", 5)

    handlers: list[logging.Handler] = []
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        handlers.append(file_handler)

    if log_to_console:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers if handlers else None,
    )

    logging.captureWarnings(True)


def _log_task_exceptions(task: asyncio.Task) -> None:
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    except Exception:
        log.exception("Background task error")
        return
    if exc:
        log.exception("Background task error", exc_info=exc)


class BotService(discord.Client):
    def __init__(self, *, config: BotConfig, db: BotDb, discourse: DiscourseClient):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.message_content = True
        super().__init__(intents=intents)
        self.config = config
        self.db = db
        self.discourse = discourse
        self._topic_locks: dict[int, asyncio.Lock] = {}
        self._archive_tasks: dict[int, asyncio.Task] = {}
        self._expected_message_deletes: set[int] = set()
        self._expected_thread_deletes: set[int] = set()

    async def setup_hook(self) -> None:
        await self.db.init()

    async def on_ready(self) -> None:
        log.info("Logged in as %s", self.user)
        await self._restore_views()
        await self._restore_scheduled_archives()
        await self._reconcile_missing_resources()

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent) -> None:
        if not payload.guild_id:
            return
        target_guild_id, _ = self._target_ids()
        if payload.guild_id != target_guild_id:
            return
        message_id = payload.message_id
        if message_id in self._expected_message_deletes:
            self._expected_message_deletes.discard(message_id)
            return

        record = await self.db.get_application_by_message_id(message_id)
        if record and not record.archived_at:
            guild = self.get_guild(payload.guild_id)
            actor = None
            if guild:
                actor = await self._resolve_audit_actor_for_message_delete(
                    guild=guild,
                    channel_id=payload.channel_id,
                )
            await self._handle_missing_card(record=record, actor=actor, reason="delete-event")
            return

        record = await self.db.get_application_by_control_message_id(message_id)
        if record and not record.archived_at:
            if record.discord_thread_id and record.discord_thread_id in self._expected_thread_deletes:
                return
            guild = self.get_guild(payload.guild_id)
            actor = None
            if guild:
                actor = await self._resolve_audit_actor_for_message_delete(
                    guild=guild,
                    channel_id=payload.channel_id,
                )
            await self._handle_missing_controls(
                record=record,
                actor=actor,
                reason="delete-event",
                message_id=message_id,
            )

    async def on_thread_delete(self, thread: discord.Thread) -> None:
        if not thread.guild:
            return
        target_guild_id, _ = self._target_ids()
        if thread.guild.id != target_guild_id:
            return
        if thread.id in self._expected_thread_deletes:
            self._expected_thread_deletes.discard(thread.id)
            return
        record = await self.db.get_application_by_thread_id(thread.id)
        if record and not record.archived_at:
            actor = await self._resolve_audit_actor_for_thread_delete(
                guild=thread.guild,
                thread_id=thread.id,
            )
            await self._handle_missing_thread(
                record=record,
                actor=actor,
                reason="delete-event",
                thread_id=thread.id,
            )

    async def _restore_views(self) -> None:
        for record in await self.db.list_applications():
            self.add_view(
                ApplicationView(
                    topic_id=record.topic_id,
                    service=self,
                    claimed=record.claimed_by_user_id is not None,
                )
            )

    async def _restore_scheduled_archives(self) -> None:
        now = datetime.now(timezone.utc)
        for record in await self.db.list_applications():
            if record.archived_at:
                continue
            if not record.archive_scheduled_at:
                continue
            try:
                when = datetime.fromisoformat(record.archive_scheduled_at)
            except Exception:
                continue
            delay = max(0.0, (when - now).total_seconds())
            self._schedule_archive(topic_id=record.topic_id, delay_seconds=delay, reason="restore")

    @staticmethod
    def _is_accepted(tags: list[str]) -> bool:
        return "p-file" in set(tags)

    def _schedule_archive(self, *, topic_id: int, delay_seconds: float, reason: str) -> None:
        existing = self._archive_tasks.get(topic_id)
        if existing and not existing.done():
            return

        async def _runner() -> None:
            try:
                await asyncio.sleep(delay_seconds)
                await self._archive_topic_if_accepted(topic_id=topic_id)
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("Archive task failed (topic_id=%s, reason=%s)", topic_id, reason)

        self._archive_tasks[topic_id] = asyncio.create_task(_runner())

    def _cancel_archive(self, *, topic_id: int) -> None:
        task = self._archive_tasks.pop(topic_id, None)
        if task and not task.done():
            task.cancel()

    def _accepted_archive_delay_minutes(self) -> int:
        return max(0, self.config.accepted_archive_delay_minutes)

    def _accepted_archive_delay_seconds(self) -> float:
        return float(self._accepted_archive_delay_minutes()) * 60.0

    def _accepted_archive_message(self) -> str:
        minutes = self._accepted_archive_delay_minutes()
        if minutes <= 0:
            return "Accepted. Archiving now."
        unit = "minute" if minutes == 1 else "minutes"
        return f"Accepted. Archiving in {minutes} {unit} (you can revert status until then)."

    def _rejected_archive_message(self) -> str:
        minutes = self._accepted_archive_delay_minutes()
        if minutes <= 0:
            return "Rejected. Archiving now."
        unit = "minute" if minutes == 1 else "minutes"
        return f"Rejected. Archiving in {minutes} {unit} (you can revert status until then)."

    async def _archive_topic_if_accepted(self, *, topic_id: int) -> None:
        record = await self.db.get_application(topic_id)
        if not record or record.archived_at:
            return

        topic = await self.discourse.fetch_topic(topic_id)
        archive_status = record.archive_status
        if archive_status != "rejected" and not self._is_accepted(topic.tags):
            await self.db.schedule_archive(topic_id=topic_id, when_iso=None)
            return
        if archive_status == "rejected" and topic.tags and not self.config.is_dry_run:
            try:
                await self.discourse.set_topic_tags(topic_id, [])
                await self.db.set_tags_last_written(topic_id=topic_id, tags=[])
                topic = await self.discourse.fetch_topic(topic_id)
            except Exception:
                log.exception("Failed to clear Discourse tags on reject (topic_id=%s)", topic_id)

        archive_started = False
        try:
            await self.db.set_archive_in_progress(topic_id=topic_id, in_progress=True)
            await self._apply_processing_view(topic_id=topic_id, label="Archiving...")
            archive_started = True

            record = await self.db.get_application(topic_id)
            if not record:
                return
            notify_msg = await self._get_notify_message(topic_id=topic_id, log_missing=False)
            parent_channel = self.get_channel(record.discord_channel_id)
            if not isinstance(parent_channel, discord.TextChannel):
                parent_channel = None
            if notify_msg:
                try:
                    embed, view = await self._render_for_topic_data(topic=topic, record=record)
                    await notify_msg.edit(embed=embed, view=view)
                except Exception:
                    pass

            thread = await self._get_thread_for_topic(topic_id=topic_id)
            thread_link = None
            if thread:
                guild_id, _ = self._target_ids()
                thread_link = f"https://discord.com/channels/{guild_id}/{thread.id}"
                await self._ensure_thread_controls(topic_id=topic_id, topic=topic, record=record)

            # Optional: post summary and transcript thread in archive channel.
            archive_posted = False
            transcript_sent = False
            archive_thread: discord.Thread | None = None
            archive_channel_id = self.config.target_archive_channel_id()
            archive_channel: discord.TextChannel | None = None
            if archive_channel_id:
                archive_channel = self.get_channel(archive_channel_id)
                if archive_channel is None:
                    try:
                        archive_channel = await self.fetch_channel(archive_channel_id)
                    except Exception:
                        archive_channel = None
                if isinstance(archive_channel, discord.TextChannel):
                    owner = await self._resolve_claimed_user(user_id=record.claimed_by_user_id)
                    if archive_status == "rejected":
                        status = "❌ Rejected"
                    else:
                        status = discourse_tags_to_stage_label(topic.tags, icons=self._status_icons())
                    color = 0xE74C3C if archive_status == "rejected" else 0x2ECC71
                    embed = discord.Embed(
                        title=topic.title or "Application",
                        url=topic.url,
                        color=color,
                        description=f"Owner: {self._user_label(owner)}\nStatus: {status}",
                    )
                    try:
                        archive_label = "Rejected (Archived)" if archive_status == "rejected" else "Accepted (Archived)"
                        archive_msg = await archive_channel.send(content=archive_label, embed=embed)
                        archive_posted = True
                        log.info(
                            "Archive summary posted (topic_id=%s channel_id=%s message_id=%s)",
                            topic_id,
                            archive_channel.id,
                            archive_msg.id,
                        )
                        archive_thread = await self._create_archive_thread(
                            message=archive_msg,
                            topic_title=topic.title,
                        )
                        log.info(
                            "Archive thread created (topic_id=%s thread_id=%s)",
                            topic_id,
                            archive_thread.id,
                        )
                    except Exception:
                        log.exception("Failed to post archive summary (topic_id=%s)", topic_id)

                    if archive_thread:
                        if thread:
                            try:
                                await archive_thread.send("Thread log:")
                                messages_sent = await self._send_transcript_to_thread(
                                    source_thread=thread,
                                    dest_thread=archive_thread,
                                )
                                log.info(
                                    "Archive transcript sent (topic_id=%s messages=%s)",
                                    topic_id,
                                    messages_sent,
                                )
                                transcript_sent = True
                            except Exception:
                                log.exception("Failed to export thread transcript (topic_id=%s)", topic_id)
                        else:
                            try:
                                await archive_thread.send("No source thread was available.")
                                transcript_sent = True
                            except Exception:
                                pass
                    elif archive_posted:
                        log.warning(
                            "Archive thread missing (topic_id=%s channel_id=%s)",
                            topic_id,
                            archive_channel.id,
                        )

            # Main channel: remove the application card once the archive summary and transcript are posted.
            if archive_posted and transcript_sent and notify_msg:
                try:
                    self._expected_message_deletes.add(notify_msg.id)
                    await notify_msg.delete()
                    notify_msg = None
                except discord.NotFound:
                    self._expected_message_deletes.discard(notify_msg.id)
                    notify_msg = None
                except Exception:
                    self._expected_message_deletes.discard(notify_msg.id)
                    log.exception("Failed to delete archived notification (topic_id=%s)", topic_id)

            # Fallback: keep a minimal Accepted stub if we did not delete the message.
            if notify_msg:
                try:
                    embed, _view = await self._render_for_topic_data(topic=topic, record=record)
                    embed.add_field(
                        name="Archive",
                        value=f"[Open thread]({thread_link})" if thread_link else "Thread not available",
                        inline=False,
                    )
                    await notify_msg.edit(embed=embed, view=None)
                except discord.NotFound:
                    pass
                except Exception:
                    log.exception("Failed to update archived notification (topic_id=%s)", topic_id)

            if transcript_sent and archive_posted and thread:
                try:
                    if record.discord_control_message_id:
                        controls_msg = await thread.fetch_message(record.discord_control_message_id)
                        embed = controls_msg.embeds[0] if controls_msg.embeds else None
                        final_label = "Archived (Rejected)" if archive_status == "rejected" else "Archived (Accepted)"
                        await controls_msg.edit(content=final_label, embed=embed, view=None)
                except Exception:
                    pass
                try:
                    await thread.edit(locked=True, archived=True)
                except Exception:
                    pass
                try:
                    if record.discord_control_message_id:
                        self._expected_message_deletes.add(record.discord_control_message_id)
                    self._expected_thread_deletes.add(thread.id)
                    await thread.delete()
                except Exception:
                    self._expected_thread_deletes.discard(thread.id)
                    if record.discord_control_message_id:
                        self._expected_message_deletes.discard(record.discord_control_message_id)
                    log.exception("Failed to delete archived thread (topic_id=%s)", topic_id)
                if parent_channel:
                    record = await self.db.get_application(topic_id)
                    names = record.thread_name_history if record else None
                    await self._delete_thread_system_message(
                        channel=parent_channel,
                        thread=thread,
                        thread_names=names,
                    )
            elif thread:
                try:
                    if record.discord_control_message_id:
                        controls_msg = await thread.fetch_message(record.discord_control_message_id)
                        embed = controls_msg.embeds[0] if controls_msg.embeds else None
                        final_label = "Archived (Rejected)" if archive_status == "rejected" else "Archived (Accepted)"
                        await controls_msg.edit(content=final_label, embed=embed, view=None)
                except Exception:
                    pass
                try:
                    await thread.edit(locked=True, archived=True)
                except Exception:
                    pass

            await self.db.mark_archived(topic_id=topic_id, archived=True)
            await self.db.schedule_archive(topic_id=topic_id, when_iso=None)
        finally:
            if archive_started:
                await self.db.set_archive_in_progress(topic_id=topic_id, in_progress=False)
                record = await self.db.get_application(topic_id)
                if record and not record.archived_at:
                    try:
                        embed, view = await self._render_for_topic(topic_id=topic_id)
                        notify_msg = await self._get_notify_message(topic_id=topic_id, log_missing=False)
                        if notify_msg:
                            await notify_msg.edit(embed=embed, view=view)
                        await self._ensure_thread_controls(topic_id=topic_id, allow_create=False)
                    except Exception:
                        pass

    def _target_ids(self) -> tuple[int, int]:
        return self.config.target_guild_and_channel()

    def _status_icons(self) -> dict[str, str]:
        guild_id, _ = self._target_ids()
        guild = self.get_guild(guild_id)
        if not guild:
            return {}
        icons: dict[str, str] = {}
        for e in guild.emojis:
            icons[e.name] = str(e)
        return icons

    def _stage_icon_for_name(self, stage: str) -> str:
        key = stage.strip().lower().replace(" ", "-")
        icons = self._status_icons()
        if key in ("accept", "accepted", "p-file"):
            return icons.get("accepted") or ":white_check_mark:"
        if key in ("reject", "rejected"):
            return icons.get("rejected") or ":x:"
        if key == "new-application":
            return icons.get("new_application") or ":star:"
        if key == "letter-sent":
            return icons.get("letter_sent") or ":envelope:"
        if key == "interview-scheduled":
            return icons.get("interview_scheduled") or ":calendar:"
        if key == "interview-held":
            return icons.get("interview_held") or ":calendar_check:"
        if key == "on-hold":
            return icons.get("pause") or ":pause_button:"
        return ":grey_question:"

    def _format_status_update(self, new_stage: str) -> str:
        new_icon = self._stage_icon_for_name(new_stage)
        return f"{new_icon} {new_stage}"

    def _topic_cache_is_fresh(
        self,
        record: ApplicationRecord,
        *,
        max_age_seconds: int | None = None,
    ) -> bool:
        if max_age_seconds is None:
            max_age_seconds = self.config.discourse_topic_cache_ttl_seconds
        if max_age_seconds <= 0:
            return False
        if not record.topic_synced_at:
            return False
        try:
            synced_at = datetime.fromisoformat(record.topic_synced_at)
        except Exception:
            return False
        if synced_at.tzinfo is None:
            synced_at = synced_at.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - synced_at).total_seconds() <= max_age_seconds

    def _cached_topic_from_record(self, record: ApplicationRecord) -> DiscourseTopic:
        title = record.topic_title or f"Topic {record.topic_id}"
        author = record.topic_author or "Unknown"
        url = f"{self.config.discourse_base_url}/t/{record.topic_id}"
        return DiscourseTopic(
            id=record.topic_id,
            title=title,
            slug=str(record.topic_id),
            url=url,
            category_id=self.config.target_applications_category_id(),
            tags=list(record.tags_last_seen),
            author=author,
        )

    async def _record_thread_name(self, *, topic_id: int, name: str) -> None:
        record = await self.db.get_application(topic_id)
        if not record:
            return
        names = list(record.thread_name_history)
        if name in names:
            return
        names.append(name)
        await self.db.set_thread_name_history(topic_id=topic_id, names=names)

    async def _resolve_claimed_user(self, *, user_id: int | None) -> discord.abc.User | None:
        if not user_id:
            return None
        user = self.get_user(user_id)
        if user:
            return user
        guild_id, _ = self._target_ids()
        guild = self.get_guild(guild_id)
        if guild:
            member = guild.get_member(user_id)
            if member:
                return member
            try:
                return await guild.fetch_member(user_id)
            except Exception:
                return None
        try:
            return await self.fetch_user(user_id)
        except Exception:
            return None

    @staticmethod
    def _user_label(user: discord.abc.User | None) -> str:
        if not user:
            return "Unassigned"
        display_name = getattr(user, "display_name", None) or user.name
        username = getattr(user, "name", "")
        if username and display_name != username:
            return f"{display_name} ({username})"
        return display_name

    @staticmethod
    def _user_display_name(user: discord.abc.User | None) -> str:
        if not user:
            return "Unknown"
        return getattr(user, "display_name", None) or user.name

    async def _respond_ephemeral(self, interaction: discord.Interaction, message: str) -> None:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    async def _defer_interaction(
        self,
        interaction: discord.Interaction,
        *,
        thinking: bool = False,
        ephemeral: bool = False,
    ) -> bool:
        if interaction.response.is_done():
            return False
        await interaction.response.defer(thinking=thinking, ephemeral=ephemeral)
        return True

    async def _finish_interaction(
        self,
        interaction: discord.Interaction,
        *,
        deferred: bool,
        message: str | None = None,
    ) -> None:
        if deferred:
            return
        if message:
            try:
                await interaction.followup.send(message, ephemeral=True, delete_after=6)
            except Exception:
                pass

    def _format_transcript_line(self, msg: discord.Message) -> str:
        author = getattr(msg.author, "display_name", msg.author.name)
        timestamp = msg.created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        content = msg.content or msg.clean_content or msg.system_content or ""
        if msg.attachments:
            attachments = " ".join(a.url for a in msg.attachments)
            if content:
                content += " "
            content += f"[attachments: {attachments}]"
        if msg.stickers:
            stickers = " ".join(s.name for s in msg.stickers)
            if content:
                content += " "
            content += f"[stickers: {stickers}]"
        if msg.embeds:
            if content:
                content += " "
            content += "[embeds]"
        if not content:
            content = "(no content)"

        is_bot = bool(self.user and msg.author.id == self.user.id)
        if is_bot:
            match = re.match(r"^<t:\d+(?::[a-zA-Z])?>\s*", content)
            if match:
                content = content[match.end():]
            return f"[{timestamp} UTC] {content}"

        return f"[{timestamp} UTC] {LOG_TAG_NOTE}: {author}: {content}"

    async def _send_transcript_to_thread(
        self,
        *,
        source_thread: discord.Thread,
        dest_thread: discord.Thread,
    ) -> int:
        max_len = 1900
        buffer = ""
        messages_sent = 0
        ignore_types = {
            discord.MessageType.thread_created,
            discord.MessageType.thread_starter_message,
        }
        for attr in ("thread_name_change", "channel_name_change"):
            msg_type = getattr(discord.MessageType, attr, None)
            if msg_type is not None:
                ignore_types.add(msg_type)
        async for msg in source_thread.history(limit=None, oldest_first=True):
            if msg.type in ignore_types:
                continue
            if self.user and msg.author.id == self.user.id:
                if msg.content.strip() == "Controls" and msg.embeds:
                    continue
                if not msg.content and not msg.embeds and not msg.attachments and not msg.stickers:
                    continue
            line = self._format_transcript_line(msg)
            if len(line) > max_len:
                line = line[: max_len - 3] + "..."
            if buffer and len(buffer) + 1 + len(line) > max_len:
                await dest_thread.send(buffer)
                messages_sent += 1
                buffer = line
            else:
                buffer = line if not buffer else f"{buffer}\n{line}"
        if buffer:
            await dest_thread.send(buffer)
            messages_sent += 1
        return messages_sent

    async def _add_thread_members(
        self,
        *,
        thread: discord.Thread,
        claimed_user_id: int | None,
    ) -> None:
        if not claimed_user_id:
            return
        guild_id, _ = self._target_ids()
        guild = self.get_guild(guild_id)
        if not guild:
            return
        member = guild.get_member(claimed_user_id)
        if not member:
            return
        try:
            await thread.add_user(member)
        except Exception:
            pass

    @staticmethod
    def _discord_ts() -> str:
        # Example: <t:1700000000:f> renders as a formatted timestamp in Discord clients.
        return f"<t:{int(datetime.now(timezone.utc).timestamp())}:f>"

    async def _get_thread_for_topic(self, *, topic_id: int) -> discord.Thread | None:
        record = await self.db.get_application(topic_id)
        if not record or not record.discord_thread_id:
            return None
        thread = self.get_channel(record.discord_thread_id)
        if thread is None:
            try:
                thread = await self.fetch_channel(record.discord_thread_id)
            except Exception:
                thread = None
        return thread if isinstance(thread, discord.Thread) else None

    async def _thread_log(self, *, topic_id: int, message: str) -> None:
        thread = await self._get_thread_for_topic(topic_id=topic_id)
        if not thread:
            return
        try:
            await thread.send(f"{self._discord_ts()} {message}")
        except Exception:
            log.exception("Failed to send thread log (topic_id=%s)", topic_id)

    @staticmethod
    def _stage_tag_from_discourse_tags(tags: list[str]) -> str:
        for t in tags:
            if t in STAGE_TAGS_DISCOURSE:
                return "Accepted" if t == "p-file" else t
        return "(none)"

    def _ensure_interaction_in_target(self, interaction: discord.Interaction) -> None:
        target_guild_id, target_channel_id = self._target_ids()
        if not interaction.guild or interaction.guild.id != target_guild_id:
            raise PermissionError("Wrong guild for current DISCORD_MODE")
        if not interaction.channel:
            raise PermissionError("Missing channel")

    async def _ensure_interaction_allowed_for_topic(
        self, interaction: discord.Interaction, *, topic_id: int
    ) -> None:
        self._ensure_interaction_in_target(interaction)
        _, target_channel_id = self._target_ids()

        channel = interaction.channel
        if channel.id == target_channel_id:
            return

        # Allow interaction from the topic's own thread.
        record = await self.db.get_application(topic_id)
        if record and record.discord_thread_id and channel.id == record.discord_thread_id:
            return

        raise PermissionError("Wrong channel for current DISCORD_MODE")

    async def _get_notify_message(
        self,
        *,
        topic_id: int,
        log_missing: bool = True,
    ) -> discord.Message | None:
        record = await self.db.get_application(topic_id)
        if not record or record.discord_message_missing:
            return None
        channel = self.get_channel(record.discord_channel_id)
        if not isinstance(channel, discord.TextChannel):
            return None
        try:
            return await channel.fetch_message(record.discord_message_id)
        except discord.NotFound:
            if log_missing:
                await self._handle_missing_card(record=record, actor=None, reason="missing")
            return None
        except Exception:
            return None

    def _can_view_audit_log(self, guild: discord.Guild) -> bool:
        member = guild.me
        if not member and self.user:
            member = guild.get_member(self.user.id)
        return bool(member and member.guild_permissions.view_audit_log)

    @staticmethod
    def _audit_actor_label(user: discord.abc.User | None) -> str:
        if not user:
            return "Unknown"
        display_name = getattr(user, "display_name", None) or user.name
        username = getattr(user, "name", "")
        if username and display_name != username:
            return f"{display_name} ({username})"
        return display_name

    async def _resolve_audit_actor_for_message_delete(
        self,
        *,
        guild: discord.Guild,
        channel_id: int,
    ) -> discord.abc.User | None:
        if not self._can_view_audit_log(guild):
            return None
        now = datetime.now(timezone.utc)
        try:
            async for entry in guild.audit_logs(
                limit=6,
                action=discord.AuditLogAction.message_delete,
            ):
                extra = entry.extra
                extra_channel = getattr(extra, "channel", None) if extra else None
                if extra_channel and extra_channel.id != channel_id:
                    continue
                created_at = entry.created_at
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                if (now - created_at).total_seconds() > 30:
                    continue
                return entry.user
        except Exception:
            return None
        return None

    async def _resolve_audit_actor_for_thread_delete(
        self,
        *,
        guild: discord.Guild,
        thread_id: int,
    ) -> discord.abc.User | None:
        if not self._can_view_audit_log(guild):
            return None
        action = getattr(discord.AuditLogAction, "thread_delete", discord.AuditLogAction.channel_delete)
        now = datetime.now(timezone.utc)
        try:
            async for entry in guild.audit_logs(limit=6, action=action):
                target = entry.target
                if target and getattr(target, "id", None) != thread_id:
                    continue
                created_at = entry.created_at
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                if (now - created_at).total_seconds() > 30:
                    continue
                return entry.user
        except Exception:
            return None
        return None

    async def _fetch_topic_title(self, *, topic_id: int) -> str | None:
        try:
            topic = await self.discourse.fetch_topic(topic_id)
        except Exception:
            return None
        return topic.title or None

    async def _create_audit_thread(
        self,
        *,
        message: discord.Message,
        topic_title: str | None,
        topic_id: int,
    ) -> discord.Thread:
        base = topic_title or f"Topic {topic_id}"
        base_name = f"Audit - {base}".strip()
        thread_name = base_name[:100] if len(base_name) > 100 else base_name
        archive_options = (10080, 4320, 1440)
        last_error: Exception | None = None
        for duration in archive_options:
            try:
                return await message.create_thread(
                    name=thread_name,
                    auto_archive_duration=duration,
                )
            except Exception as e:
                last_error = e
        raise last_error or RuntimeError("Failed to create audit thread")

    async def _post_audit_thread(
        self,
        *,
        topic_id: int,
        topic_title: str | None,
        summary: str,
        details: list[str],
    ) -> None:
        archive_channel_id = self.config.target_archive_channel_id()
        if not archive_channel_id:
            log.warning("Audit log skipped (no archive channel). topic_id=%s", topic_id)
            return
        archive_channel = self.get_channel(archive_channel_id)
        if archive_channel is None:
            try:
                archive_channel = await self.fetch_channel(archive_channel_id)
            except Exception:
                archive_channel = None
        if not isinstance(archive_channel, discord.TextChannel):
            log.warning("Audit log skipped (archive channel missing). topic_id=%s", topic_id)
            return
        try:
            audit_msg = await archive_channel.send(content=summary)
        except Exception:
            log.exception("Failed to post audit summary (topic_id=%s)", topic_id)
            return
        try:
            audit_thread = await self._create_audit_thread(
                message=audit_msg,
                topic_title=topic_title,
                topic_id=topic_id,
            )
            if details:
                await audit_thread.send("\n".join(details))
        except Exception:
            log.exception("Failed to post audit details (topic_id=%s)", topic_id)

    async def _apply_processing_view(self, *, topic_id: int, label: str) -> None:
        record = await self.db.get_application(topic_id)
        if not record or record.archived_at:
            return
        view = ApplicationView(
            topic_id=topic_id,
            service=self,
            claimed=bool(record.claimed_by_user_id),
            processing=True,
            processing_label=label,
        )
        if not record.discord_message_missing:
            channel = self.get_channel(record.discord_channel_id)
            if isinstance(channel, discord.TextChannel):
                try:
                    msg = await channel.fetch_message(record.discord_message_id)
                    await msg.edit(view=view)
                except Exception:
                    pass
        if record.discord_control_message_id:
            thread = await self._get_thread_for_topic(topic_id=topic_id)
            if thread:
                try:
                    controls_msg = await thread.fetch_message(record.discord_control_message_id)
                    await controls_msg.edit(view=view)
                except Exception:
                    pass

    async def _show_processing(
        self,
        *,
        interaction: discord.Interaction,
        topic_id: int,
        view: ApplicationView,
    ) -> bool:
        responded = False
        if interaction.message:
            try:
                await interaction.response.edit_message(view=view)
                responded = True
            except Exception:
                responded = False
        deferred = False if responded else await self._defer_interaction(interaction)
        if not responded:
            try:
                if interaction.message:
                    await interaction.message.edit(view=view)
                else:
                    notify_msg = await self._get_notify_message(topic_id=topic_id)
                    if notify_msg:
                        await notify_msg.edit(view=view)
            except Exception:
                pass
        return deferred

    async def _ensure_thread_for_action(
        self,
        *,
        topic_id: int,
        interaction: discord.Interaction,
        claimed_user_id: int | None,
    ) -> discord.Thread | None:
        record = await self.db.get_application(topic_id)
        if not record:
            return None
        thread = await self._get_thread_for_topic(topic_id=topic_id)
        if thread:
            await self._add_thread_members(thread=thread, claimed_user_id=claimed_user_id)
            return thread

        _, target_channel_id = self._target_ids()
        if not interaction.channel or interaction.channel.id != target_channel_id:
            return None
        if not isinstance(interaction.channel, discord.TextChannel):
            return None

        msg = interaction.message
        if not msg and not record.discord_message_missing:
            channel = self.get_channel(record.discord_channel_id)
            if isinstance(channel, discord.TextChannel):
                try:
                    msg = await channel.fetch_message(record.discord_message_id)
                except Exception:
                    msg = None
        if not msg:
            return None

        topic = await self.discourse.fetch_topic(topic_id)
        await self._create_thread_if_needed(
            channel=interaction.channel,
            message=msg,
            topic_title=topic.title,
            topic_id=topic_id,
        )
        thread = await self._get_thread_for_topic(topic_id=topic_id)
        if thread:
            await self._add_thread_members(thread=thread, claimed_user_id=claimed_user_id)
        return thread

    async def _fetch_card_message(self, *, record: ApplicationRecord) -> discord.Message | None:
        if record.discord_message_missing:
            return None
        channel = self.get_channel(record.discord_channel_id)
        if not isinstance(channel, discord.TextChannel):
            return None
        try:
            return await channel.fetch_message(record.discord_message_id)
        except Exception:
            return None

    async def _handle_missing_card(
        self,
        *,
        record: ApplicationRecord,
        actor: discord.abc.User | None,
        reason: str,
    ) -> None:
        if record.archived_at:
            return
        already_missing = record.discord_message_missing
        if not already_missing:
            await self.db.set_message_missing(topic_id=record.topic_id, missing=True)

        thread = await self._get_thread_for_topic(topic_id=record.topic_id)
        thread_missing = thread is None
        if thread_missing and record.discord_thread_id:
            await self.db.set_thread_id(topic_id=record.topic_id, thread_id=None)
            await self.db.set_control_message_id(topic_id=record.topic_id, message_id=None)

        if not already_missing:
            topic_title = await self._fetch_topic_title(topic_id=record.topic_id)
            actor_label = self._audit_actor_label(actor)
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            details = [
                "Manual deletion detected.",
                f"Deleted item: application card message",
                f"Actor: {actor_label}",
                f"Detected at: {timestamp} UTC",
                f"Reason: {reason}",
                f"Topic ID: {record.topic_id}",
                f"Channel ID: {record.discord_channel_id}",
                f"Message ID: {record.discord_message_id}",
                f"Outcome: {'record removed' if thread_missing else 'continuing via thread'}",
            ]
            await self._post_audit_thread(
                topic_id=record.topic_id,
                topic_title=topic_title,
                summary=f"Audit: application card deleted (topic {record.topic_id})",
                details=details,
            )

        if thread_missing:
            await self._cleanup_application_record(topic_id=record.topic_id, reason="card-missing")

    async def _handle_missing_thread(
        self,
        *,
        record: ApplicationRecord,
        actor: discord.abc.User | None,
        reason: str,
        thread_id: int,
    ) -> None:
        if record.archived_at:
            return
        await self.db.set_thread_id(topic_id=record.topic_id, thread_id=None)
        await self.db.set_control_message_id(topic_id=record.topic_id, message_id=None)

        card_msg = await self._fetch_card_message(record=record)
        card_exists = card_msg is not None
        if not card_exists and not record.discord_message_missing:
            await self.db.set_message_missing(topic_id=record.topic_id, missing=True)

        topic_title = await self._fetch_topic_title(topic_id=record.topic_id)
        actor_label = self._audit_actor_label(actor)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        details = [
            "Manual deletion detected.",
            "Deleted item: application thread",
            f"Actor: {actor_label}",
            f"Detected at: {timestamp} UTC",
            f"Reason: {reason}",
            f"Topic ID: {record.topic_id}",
            f"Thread ID: {thread_id}",
            f"Outcome: {'record removed' if not card_exists else 'continuing via card'}",
        ]
        await self._post_audit_thread(
            topic_id=record.topic_id,
            topic_title=topic_title,
            summary=f"Audit: application thread deleted (topic {record.topic_id})",
            details=details,
        )

        if not card_exists:
            await self._cleanup_application_record(topic_id=record.topic_id, reason="thread-missing")

    async def _handle_missing_controls(
        self,
        *,
        record: ApplicationRecord,
        actor: discord.abc.User | None,
        reason: str,
        message_id: int,
    ) -> None:
        if record.archived_at:
            return
        await self.db.set_control_message_id(topic_id=record.topic_id, message_id=None)

        topic_title = await self._fetch_topic_title(topic_id=record.topic_id)
        actor_label = self._audit_actor_label(actor)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        cleanup = bool(record.discord_message_missing)
        details = [
            "Manual deletion detected.",
            "Deleted item: thread controls message",
            f"Actor: {actor_label}",
            f"Detected at: {timestamp} UTC",
            f"Reason: {reason}",
            f"Topic ID: {record.topic_id}",
            f"Thread ID: {record.discord_thread_id or 'unknown'}",
            f"Message ID: {message_id}",
            f"Outcome: {'record removed' if cleanup else 'controls will be recreated on next card action'}",
        ]
        await self._post_audit_thread(
            topic_id=record.topic_id,
            topic_title=topic_title,
            summary=f"Audit: thread controls deleted (topic {record.topic_id})",
            details=details,
        )

        if cleanup:
            await self._cleanup_application_record(topic_id=record.topic_id, reason="controls-missing")

    async def _cleanup_application_record(self, *, topic_id: int, reason: str) -> None:
        self._cancel_archive(topic_id=topic_id)
        await self.db.delete_application(topic_id=topic_id)
        self._topic_locks.pop(topic_id, None)
        log.info("Application record removed (topic_id=%s, reason=%s)", topic_id, reason)

    async def _reconcile_missing_resources(self) -> None:
        records = await self.db.list_applications()
        for record in records:
            if record.archived_at:
                continue

            if not record.discord_message_missing:
                msg = await self._fetch_card_message(record=record)
                if msg is None:
                    await self._handle_missing_card(record=record, actor=None, reason="startup")
                    record = await self.db.get_application(record.topic_id) or record

            record = await self.db.get_application(record.topic_id)
            if not record or record.archived_at:
                continue

            if record.discord_thread_id:
                thread = await self._get_thread_for_topic(topic_id=record.topic_id)
                if thread is None:
                    await self._handle_missing_thread(
                        record=record,
                        actor=None,
                        reason="startup",
                        thread_id=record.discord_thread_id,
                    )
                    record = await self.db.get_application(record.topic_id) or record
                else:
                    if not record.thread_name_history:
                        await self._record_thread_name(topic_id=record.topic_id, name=thread.name)

            record = await self.db.get_application(record.topic_id)
            if not record or record.archived_at:
                continue

            if record.discord_thread_id and record.discord_control_message_id:
                thread = await self._get_thread_for_topic(topic_id=record.topic_id)
                if thread:
                    try:
                        await thread.fetch_message(record.discord_control_message_id)
                    except discord.NotFound:
                        await self._handle_missing_controls(
                            record=record,
                            actor=None,
                            reason="startup",
                            message_id=record.discord_control_message_id,
                        )
                    except Exception:
                        pass

    async def _ensure_thread_controls(
        self,
        *,
        topic_id: int,
        allow_create: bool = False,
        topic: DiscourseTopic | None = None,
        record: ApplicationRecord | None = None,
    ) -> None:
        record = record or await self.db.get_application(topic_id)
        if not record or not record.discord_thread_id:
            return
        if record.archived_at:
            return
        thread = await self._get_thread_for_topic(topic_id=topic_id)
        if not thread:
            return

        # Send or update a pinned controls message in the thread.
        if topic:
            embed, view = await self._render_for_topic_data(topic=topic, record=record)
        else:
            embed, view = await self._render_for_topic(topic_id=topic_id)
        content = "Controls"
        controls_msg: discord.Message | None = None

        if record.discord_control_message_id:
            try:
                controls_msg = await thread.fetch_message(record.discord_control_message_id)
            except discord.NotFound:
                controls_msg = None
                await self.db.set_control_message_id(topic_id=topic_id, message_id=None)
            except Exception:
                controls_msg = None

        if controls_msg is None:
            if not allow_create:
                return
            controls_msg = await thread.send(content=content, embed=embed, view=view)
            await self.db.set_control_message_id(topic_id=topic_id, message_id=controls_msg.id)
        else:
            try:
                await controls_msg.edit(content=content, embed=embed, view=view)
            except Exception:
                pass

    def _member_has_claim_permission(self, member: discord.Member) -> bool:
        allowed = {n.lower() for n in self.config.discord_allowed_role_names}
        return any(role.name.lower() in allowed for role in member.roles)

    def _member_has_override_permission(self, member: discord.Member) -> bool:
        allowed = {n.lower() for n in self.config.discord_override_role_names}
        return any(role.name.lower() in allowed for role in member.roles)

    def _member_has_admin_permission(self, member: discord.Member) -> bool:
        allowed = {n.lower() for n in self.config.discord_override_role_names}
        return any(role.name.lower() in allowed for role in member.roles)

    def _member_is_claim_eligible(self, member: discord.Member) -> bool:
        return self._member_has_claim_permission(member)

    async def _build_reassign_options(self) -> list[tuple[int, str]]:
        guild_id, _ = self._target_ids()
        guild = self.get_guild(guild_id)
        if not guild:
            return []

        eligible: list[tuple[int, str]] = []
        # Prefer cache, but fall back to fetching if the cache is empty.
        if guild.members:
            members = list(guild.members)
        else:
            members = []
            try:
                async for m in guild.fetch_members(limit=None):
                    members.append(m)
            except Exception:
                members = []

        for m in members:
            if self._member_is_claim_eligible(m):
                eligible.append((m.id, m.display_name))

        eligible.sort(key=lambda t: t[1].lower())
        return eligible

    async def _render_for_topic_data(
        self,
        *,
        topic: DiscourseTopic,
        record: ApplicationRecord | None,
        show_reassign_selector: bool = False,
        claimed_by_override: discord.abc.User | None = None,
        reassign_options: list[tuple[int, str]] | None = None,
    ) -> tuple[discord.Embed, ApplicationView]:
        tags_discord = discourse_tags_to_discord(topic.tags)
        stage_label = discourse_tags_to_stage_label(topic.tags, icons=self._status_icons())

        if record and record.archive_status == "rejected":
            stage_label = "Rejected"
        claimed_user = claimed_by_override or await self._resolve_claimed_user(
            user_id=record.claimed_by_user_id if record else None
        )
        view = ApplicationView(
            topic_id=topic.id,
            service=self,
            claimed=bool(record and record.claimed_by_user_id),
            show_reassign_selector=show_reassign_selector,
            reassign_options=reassign_options or [],
        )
        rendered = build_application_embed(
            topic=topic,
            tags_discord=tags_discord,
            stage_label=stage_label,
            claimed_by=claimed_user,
        )
        return rendered.embed, view

    async def _render_for_topic(
        self,
        *,
        topic_id: int,
        show_reassign_selector: bool = False,
        claimed_by_override: discord.abc.User | None = None,
        reassign_options: list[tuple[int, str]] | None = None,
    ) -> tuple[discord.Embed, ApplicationView]:
        record = await self.db.get_application(topic_id)
        if record and record.topic_title and record.tags_last_seen and self._topic_cache_is_fresh(record):
            topic = self._cached_topic_from_record(record)
        else:
            topic = await self.discourse.fetch_topic(topic_id)
            if record:
                await self.db.set_topic_snapshot(
                    topic_id=topic_id,
                    title=topic.title,
                    author=topic.author,
                    tags=topic.tags,
                    synced_at=datetime.now(timezone.utc).isoformat(),
                )
        return await self._render_for_topic_data(
            topic=topic,
            record=record,
            show_reassign_selector=show_reassign_selector,
            claimed_by_override=claimed_by_override,
            reassign_options=reassign_options,
        )

    async def handle_discourse_topic_event(
        self,
        *,
        topic_id: int,
        event_type: str = "",
        discourse_actor: str | None = None,
    ) -> None:
        # Multiple Discourse webhooks/events can arrive for the same topic in quick succession.
        # Serialize per-topic processing to avoid duplicate Discord posts.
        lock = self._topic_locks.setdefault(topic_id, asyncio.Lock())
        async with lock:
            await self._handle_discourse_topic_event_inner(
                topic_id=topic_id,
                event_type=event_type,
                discourse_actor=discourse_actor,
            )

    async def _handle_discourse_topic_event_inner(
        self,
        *,
        topic_id: int,
        event_type: str = "",
        discourse_actor: str | None = None,
    ) -> None:
        topic = await self.discourse.fetch_topic(topic_id)
        expected_category_id = self.config.target_applications_category_id()
        if topic.category_id != expected_category_id:
            log.info(
                "Ignored webhook (category mismatch). topic_id=%s category_id=%s expected=%s",
                topic_id,
                topic.category_id,
                expected_category_id,
            )
            return

        tags_discord = discourse_tags_to_discord(topic.tags)
        stage_label = discourse_tags_to_stage_label(topic.tags, icons=self._status_icons())

        _, target_channel_id = self._target_ids()
        channel = self.get_channel(target_channel_id)

        if not isinstance(channel, discord.TextChannel):
            if self.config.is_dry_run:
                log.info("dry-run: would post/update topic_id=%s title=%r", topic_id, topic.title)
                return
            raise RuntimeError(f"Channel not found or not a text channel: {target_channel_id}")

        record = await self.db.get_application(topic_id)
        if record and record.archived_at:
            log.info("Ignored webhook for archived topic_id=%s", topic_id)
            return
        previous_tags = list(record.tags_last_seen) if record else None
        claimed_user = None
        claimed = False
        if record and record.claimed_by_user_id:
            claimed_user = await self._resolve_claimed_user(user_id=record.claimed_by_user_id)
            claimed = True
        rendered = build_application_embed(
            topic=topic,
            tags_discord=tags_discord,
            stage_label=stage_label,
            claimed_by=claimed_user,
        )
        view = ApplicationView(
            topic_id=topic_id,
            service=self,
            claimed=claimed,
        )

        if record:
            if self.config.is_dry_run:
                log.info("dry-run: would edit message topic_id=%s message_id=%s", topic_id, record.discord_message_id)
            else:
                if not record.discord_message_missing:
                    try:
                        msg = await channel.fetch_message(record.discord_message_id)
                        await msg.edit(embed=rendered.embed, view=view)
                    except discord.NotFound:
                        await self._handle_missing_card(
                            record=record,
                            actor=None,
                            reason="discourse-update",
                        )
                        record = await self.db.get_application(topic_id)
                        if not record:
                            return
                    except Exception:
                        pass
            await self.db.set_topic_snapshot(
                topic_id=topic_id,
                title=topic.title,
                author=topic.author,
                tags=topic.tags,
                synced_at=datetime.now(timezone.utc).isoformat(),
            )
            if record.topic_title and record.topic_title != topic.title:
                actor = discourse_actor or "Unknown"
                await self._thread_log(
                    topic_id=topic_id,
                    message=(
                        f"{LOG_TAG_SYSTEM}: Application title changed to {topic.title} "
                        f"(by {actor}, discourse)"
                    ),
                )
            await self._sync_thread_title(topic_id=topic_id, topic_title=topic.title)
            await self._ensure_thread_controls(topic_id=topic_id, allow_create=False)

            suppress_echo = False
            if previous_tags is not None and previous_tags != topic.tags:
                suppress_echo = bool(
                    record.tags_last_written is not None
                    and sorted(record.tags_last_written) == sorted(topic.tags)
                )

            # Schedule delayed archive when Accepted arrives from Discourse.
            if previous_tags is not None:
                ignore_reopen_for_reject = bool(record.archive_status == "rejected" and suppress_echo)
                if not ignore_reopen_for_reject and not suppress_echo:
                    became_accepted = (not self._is_accepted(previous_tags)) and self._is_accepted(topic.tags)
                    reopened = self._is_accepted(previous_tags) and (not self._is_accepted(topic.tags))
                    if became_accepted:
                        delay_minutes = self._accepted_archive_delay_minutes()
                        when = datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)
                        await self.db.mark_accepted(topic_id=topic_id, accepted=True)
                        await self.db.set_archive_status(topic_id=topic_id, status="accepted")
                        await self.db.schedule_archive(topic_id=topic_id, when_iso=when.isoformat())
                        self._schedule_archive(
                            topic_id=topic_id,
                            delay_seconds=self._accepted_archive_delay_seconds(),
                            reason="discourse-accepted",
                        )
                        await self._thread_log(
                            topic_id=topic_id,
                            message=f"{LOG_TAG_SYSTEM}: {self._accepted_archive_message()}",
                        )
                    elif reopened:
                        await self.db.mark_accepted(topic_id=topic_id, accepted=False)
                        await self.db.set_archive_status(topic_id=topic_id, status=None)
                        await self.db.schedule_archive(topic_id=topic_id, when_iso=None)
                        self._cancel_archive(topic_id=topic_id)
                        await self._thread_log(
                            topic_id=topic_id,
                            message=f"{LOG_TAG_STATUS}: Reopened (Accepted removed).",
                        )

            # If Discourse tags changed, log it in the thread (if one exists), unless it matches
            # tags we just wrote from Discord (to avoid duplicate "echo" logs).
            if previous_tags is not None and previous_tags != topic.tags and not suppress_echo:
                prev_stage = self._stage_tag_from_discourse_tags(previous_tags)
                new_stage = self._stage_tag_from_discourse_tags(topic.tags)
                actor = discourse_actor or "Unknown"
                await self._thread_log(
                    topic_id=topic_id,
                    message=f"{self._format_status_update(new_stage)} (by {actor}, discourse)",
                )
            return

        if self.config.is_dry_run:
            log.info("dry-run: would send new notification for topic_id=%s title=%r", topic_id, topic.title)
            return

        msg = await channel.send(
            content="A new 16AA Membership Application has been submitted",
            embed=rendered.embed,
            view=view,
        )
        self.add_view(view)
        await self.db.upsert_application(
            topic_id=topic_id,
            discord_channel_id=channel.id,
            discord_message_id=msg.id,
            discord_thread_id=None,
            tags_last_seen=topic.tags,
            topic_title=topic.title,
            topic_author=topic.author,
            topic_synced_at=datetime.now(timezone.utc).isoformat(),
        )

        try:
            _ = await self._create_thread_if_needed(
                channel=channel,
                message=msg,
                topic_title=topic.title,
                topic_id=topic_id,
            )
            thread = await self._get_thread_for_topic(topic_id=topic_id)
            if thread:
                await self._add_thread_members(thread=thread, claimed_user_id=None)
            await self._ensure_thread_controls(topic_id=topic_id, allow_create=True)
        except Exception:
            log.exception("Failed to create thread for new application (topic_id=%s)", topic_id)

        if self._is_accepted(topic.tags):
            delay_minutes = self._accepted_archive_delay_minutes()
            when = datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)
            await self.db.mark_accepted(topic_id=topic_id, accepted=True)
            await self.db.set_archive_status(topic_id=topic_id, status="accepted")
            await self.db.schedule_archive(topic_id=topic_id, when_iso=when.isoformat())
            self._schedule_archive(
                topic_id=topic_id,
                delay_seconds=self._accepted_archive_delay_seconds(),
                reason="discourse-accepted-initial",
            )

    def _truncate_thread_name(self, name: str) -> str:
        base = name.strip()
        return base[:100] if len(base) > 100 else base

    async def _sync_thread_title(self, *, topic_id: int, topic_title: str) -> None:
        record = await self.db.get_application(topic_id)
        if not record or not record.discord_thread_id:
            return
        thread = await self._get_thread_for_topic(topic_id=topic_id)
        if not thread:
            return
        new_name = self._truncate_thread_name(topic_title)
        if thread.name == new_name:
            return
        try:
            await self._record_thread_name(topic_id=topic_id, name=thread.name)
            await thread.edit(name=new_name)
            await self._record_thread_name(topic_id=topic_id, name=new_name)
        except Exception:
            pass

    async def _create_thread_if_needed(
        self,
        *,
        channel: discord.TextChannel,
        message: discord.Message,
        topic_title: str,
        topic_id: int,
    ) -> int:
        record = await self.db.get_application(topic_id)
        if record and record.discord_thread_id:
            return record.discord_thread_id

        thread_name = self._truncate_thread_name(topic_title)

        # Discord does not support disabling auto-archive. Prefer the maximum, but fall back
        # if the guild does not allow it.
        archive_options = (10080, 4320, 1440)
        last_error: Exception | None = None
        for duration in archive_options:
            try:
                # Prefer creating a thread without a parent message so the non-clickable
                # component preview isn't shown at the top of the thread.
                thread = await channel.create_thread(
                    name=thread_name,
                    auto_archive_duration=duration,
                    type=discord.ChannelType.public_thread,
                )
                break
            except Exception as e:
                last_error = e
                # Fall back to creating from the message if the guild/channel disallows
                # threads without a parent message.
                try:
                    thread = await message.create_thread(
                        name=thread_name,
                        auto_archive_duration=duration,
                    )
                    last_error = None
                    break
                except Exception as e2:
                    last_error = e2
        else:
            raise last_error or RuntimeError("Failed to create thread")

        await self.db.set_thread_id(topic_id=topic_id, thread_id=thread.id)
        await self._record_thread_name(topic_id=topic_id, name=thread.name)
        return thread.id

    async def _create_archive_thread(
        self,
        *,
        message: discord.Message,
        topic_title: str,
    ) -> discord.Thread:
        thread_name = self._truncate_thread_name(f"Application - {topic_title}")
        archive_options = (10080, 4320, 1440)
        last_error: Exception | None = None
        for duration in archive_options:
            try:
                return await message.create_thread(
                    name=thread_name,
                    auto_archive_duration=duration,
                )
            except Exception as e:
                last_error = e
        raise last_error or RuntimeError("Failed to create archive thread")

    async def _delete_thread_system_message(
        self,
        *,
        channel: discord.TextChannel,
        thread: discord.Thread | None,
        thread_names: list[str] | None = None,
    ) -> None:
        try:
            names = [n for n in (thread_names or []) if n]
            if thread:
                names.append(thread.name)
            async for msg in channel.history(limit=50):
                if msg.type == discord.MessageType.thread_created:
                    msg_thread = getattr(msg, "thread", None)
                    if thread and msg_thread and msg_thread.id == thread.id:
                        await msg.delete()
                        return
                    for name in names:
                        if name and name in msg.content:
                            await msg.delete()
                            return
        except Exception:
            thread_id = thread.id if thread else "unknown"
            log.exception("Failed to delete thread system message (thread_id=%s)", thread_id)

    async def handle_claim(self, interaction: discord.Interaction, *, topic_id: int) -> None:
        try:
            await self._ensure_interaction_allowed_for_topic(interaction, topic_id=topic_id)
        except PermissionError as e:
            await self._respond_ephemeral(interaction, str(e))
            return

        if not isinstance(interaction.user, discord.Member):
            await self._respond_ephemeral(interaction, "Unexpected user type.")
            return

        if not self._member_has_claim_permission(interaction.user):
            await self._respond_ephemeral(interaction, "Only RRO can claim applications.")
            return

        ok = await self.db.try_claim(topic_id=topic_id, user_id=interaction.user.id)
        if not ok:
            await self._respond_ephemeral(interaction, "This application is already claimed.")
            return

        if self.config.is_dry_run:
            await self._respond_ephemeral(interaction, "dry-run: claim recorded; no Discord updates.")
            return

        processing_view = ApplicationView(
            topic_id=topic_id,
            service=self,
            claimed=False,
            processing=True,
            processing_label="Claiming...",
        )
        deferred = await self._show_processing(
            interaction=interaction,
            topic_id=topic_id,
            view=processing_view,
        )

        record = await self.db.get_application(topic_id)
        if not record:
            await self._respond_ephemeral(interaction, "Internal error: missing record.")
            return
        await self._apply_processing_view(topic_id=topic_id, label="Claiming...")
        thread = await self._get_thread_for_topic(topic_id=topic_id)
        if thread is None:
            _, target_channel_id = self._target_ids()
            channel: discord.TextChannel | None = None
            msg: discord.Message | None = None
            if interaction.channel and interaction.channel.id == target_channel_id:
                if isinstance(interaction.channel, discord.TextChannel):
                    channel = interaction.channel
                    msg = interaction.message
            if not msg and not record.discord_message_missing:
                channel = self.get_channel(record.discord_channel_id)
                if isinstance(channel, discord.TextChannel):
                    try:
                        msg = await channel.fetch_message(record.discord_message_id)
                    except Exception:
                        msg = None
            if channel and msg:
                topic = await self.discourse.fetch_topic(topic_id)
                await self._create_thread_if_needed(
                    channel=channel,
                    message=msg,
                    topic_title=topic.title,
                    topic_id=topic_id,
                )
                thread = await self._get_thread_for_topic(topic_id=topic_id)
        if thread:
            await self._add_thread_members(thread=thread, claimed_user_id=interaction.user.id)
        await self._ensure_thread_controls(topic_id=topic_id, allow_create=True)
        await self._thread_log(
            topic_id=topic_id,
            message=f"{LOG_TAG_ASSIGN}: Claimed by {self._user_display_name(interaction.user)}.",
        )

        await self.handle_discourse_topic_event(topic_id=topic_id)
        await self._finish_interaction(interaction, deferred=deferred)

    async def handle_unclaim(self, interaction: discord.Interaction, *, topic_id: int) -> None:
        try:
            await self._ensure_interaction_allowed_for_topic(interaction, topic_id=topic_id)
        except PermissionError as e:
            await self._respond_ephemeral(interaction, str(e))
            return

        if not isinstance(interaction.user, discord.Member):
            await self._respond_ephemeral(interaction, "Unexpected user type.")
            return

        before = await self.db.get_application(topic_id)
        is_owner = bool(before and before.claimed_by_user_id == interaction.user.id)
        is_override = self._member_has_override_permission(interaction.user)
        if not is_owner and not is_override:
            await self._respond_ephemeral(interaction, "Only the owner or override roles can unclaim.")
            return
        await self.db.force_claim(topic_id=topic_id, user_id=None)
        if self.config.is_dry_run:
            await self._respond_ephemeral(interaction, "dry-run: unclaimed in DB.")
            return

        claimed_before = bool(before and before.claimed_by_user_id)
        processing_view = ApplicationView(
            topic_id=topic_id,
            service=self,
            claimed=claimed_before,
            processing=True,
            processing_label="Unclaiming...",
        )
        deferred = await self._show_processing(
            interaction=interaction,
            topic_id=topic_id,
            view=processing_view,
        )

        await self._apply_processing_view(topic_id=topic_id, label="Unclaiming...")
        await self._ensure_thread_for_action(
            topic_id=topic_id,
            interaction=interaction,
            claimed_user_id=None,
        )
        await self._ensure_thread_controls(topic_id=topic_id, allow_create=True)
        await self.handle_discourse_topic_event(topic_id=topic_id)
        previous = await self._resolve_claimed_user(user_id=before.claimed_by_user_id) if before else None
        prev_text = self._user_label(previous)
        await self._thread_log(
            topic_id=topic_id,
            message=(
                f"{LOG_TAG_ASSIGN}: Unclaimed by {self._user_label(interaction.user)} "
                f"(previous owner: {prev_text})."
            ),
        )
        await self._finish_interaction(interaction, deferred=deferred)

    async def handle_reassign(self, interaction: discord.Interaction, *, topic_id: int) -> None:
        try:
            await self._ensure_interaction_allowed_for_topic(interaction, topic_id=topic_id)
        except PermissionError as e:
            await self._respond_ephemeral(interaction, str(e))
            return

        if not isinstance(interaction.user, discord.Member):
            await self._respond_ephemeral(interaction, "Unexpected user type.")
            return

        if not self._member_has_admin_permission(interaction.user):
            await self._respond_ephemeral(interaction, "You do not have permission to reassign.")
            return

        record = await self.db.get_application(topic_id)

        claimed = bool(record and record.claimed_by_user_id)
        processing_view = ApplicationView(
            topic_id=topic_id,
            service=self,
            claimed=claimed,
            processing=True,
            processing_label="Loading assignees...",
        )
        deferred = await self._show_processing(
            interaction=interaction,
            topic_id=topic_id,
            view=processing_view,
        )
        if deferred:
            await self._finish_interaction(interaction, deferred=deferred)

        await self._apply_processing_view(topic_id=topic_id, label="Loading assignees...")
        await self._ensure_thread_for_action(
            topic_id=topic_id,
            interaction=interaction,
            claimed_user_id=record.claimed_by_user_id if record else None,
        )

        # Show a temporary user selector on the message where the button was clicked.
        options = await self._build_reassign_options()
        embed, view = await self._render_for_topic(
            topic_id=topic_id,
            show_reassign_selector=True,
            reassign_options=options,
        )
        try:
            if interaction.message:
                await interaction.message.edit(embed=embed, view=view)
            else:
                notify_msg = await self._get_notify_message(topic_id=topic_id)
                if notify_msg:
                    await notify_msg.edit(embed=embed, view=view)
        except Exception:
            pass
        target_is_thread_controls = bool(
            record
            and interaction.message
            and record.discord_control_message_id == interaction.message.id
        )
        if not target_is_thread_controls:
            await self._ensure_thread_controls(topic_id=topic_id, allow_create=True)

    async def handle_force_claim(self, interaction: discord.Interaction, *, topic_id: int, new_user_id: int) -> None:
        await self.db.force_claim(topic_id=topic_id, user_id=new_user_id)
        if not self.config.is_dry_run:
            await self.handle_discourse_topic_event(topic_id=topic_id)

    async def handle_reassign_select(
        self,
        interaction: discord.Interaction,
        *,
        topic_id: int,
        new_user_id: int,
    ) -> None:
        try:
            await self._ensure_interaction_allowed_for_topic(interaction, topic_id=topic_id)
        except PermissionError as e:
            await self._respond_ephemeral(interaction, str(e))
            return

        if not isinstance(interaction.user, discord.Member):
            await self._respond_ephemeral(interaction, "Unexpected user type.")
            return

        if not self._member_has_admin_permission(interaction.user):
            await self._respond_ephemeral(interaction, "You do not have permission to reassign.")
            return

        guild_id, _ = self._target_ids()
        guild = self.get_guild(guild_id)
        if not guild:
            await self._respond_ephemeral(interaction, "Guild not available.")
            return

        target_member = guild.get_member(new_user_id)
        if target_member and not self._member_is_claim_eligible(target_member):
            await self._respond_ephemeral(
                interaction,
                "That user is not eligible (must have RRO or RRO ICs).",
            )
            return

        before = await self.db.get_application(topic_id)
        claimed_before = bool(before and before.claimed_by_user_id)
        processing_label = "Reassigning..." if claimed_before else "Assigning..."
        processing_view = ApplicationView(
            topic_id=topic_id,
            service=self,
            claimed=claimed_before,
            processing=True,
            processing_label=processing_label,
        )
        deferred = await self._show_processing(
            interaction=interaction,
            topic_id=topic_id,
            view=processing_view,
        )

        await self._apply_processing_view(topic_id=topic_id, label=processing_label)
        await self.db.force_claim(topic_id=topic_id, user_id=new_user_id)
        await self.handle_discourse_topic_event(topic_id=topic_id)

        await self._ensure_thread_for_action(
            topic_id=topic_id,
            interaction=interaction,
            claimed_user_id=new_user_id,
        )

        await self._ensure_thread_controls(topic_id=topic_id, allow_create=True)
        previous = await self._resolve_claimed_user(user_id=before.claimed_by_user_id) if before else None
        prev_text = self._user_label(previous)
        new_user = target_member or await self._resolve_claimed_user(user_id=new_user_id)
        new_text = self._user_label(new_user) if new_user else f"User {new_user_id}"
        await self._thread_log(
            topic_id=topic_id,
            message=(
                f"{LOG_TAG_ASSIGN}: Reassigned by {self._user_label(interaction.user)}: "
                f"{prev_text} -> {new_text}."
            ),
        )
        await self._finish_interaction(interaction, deferred=deferred)

    async def handle_set_stage(self, interaction: discord.Interaction, *, topic_id: int, stage_tag: str) -> None:
        try:
            await self._ensure_interaction_allowed_for_topic(interaction, topic_id=topic_id)
        except PermissionError as e:
            await self._respond_ephemeral(interaction, str(e))
            return

        if not isinstance(interaction.user, discord.Member):
            await self._respond_ephemeral(interaction, "Unexpected user type.")
            return

        if not (
            self._member_has_claim_permission(interaction.user)
            or self._member_has_override_permission(interaction.user)
        ):
            await self._respond_ephemeral(interaction, "You do not have permission to change stage.")
            return

        record = await self.db.get_application(topic_id)
        claimed = bool(record and record.claimed_by_user_id)
        processing_view = ApplicationView(
            topic_id=topic_id,
            service=self,
            claimed=claimed,
            processing=True,
            processing_label="Updating status...",
        )
        deferred = await self._show_processing(
            interaction=interaction,
            topic_id=topic_id,
            view=processing_view,
        )

        await self._apply_processing_view(topic_id=topic_id, label="Updating status...")
        await self._ensure_thread_for_action(
            topic_id=topic_id,
            interaction=interaction,
            claimed_user_id=record.claimed_by_user_id if record else None,
        )

        topic = await self.discourse.fetch_topic(topic_id)
        current = list(topic.tags)
        prev_stage = self._stage_tag_from_discourse_tags(current)

        stage_tag_lower = stage_tag.lower()
        if stage_tag_lower == "reject":
            next_tags = []
            new_stage = "Rejected"
        else:
            non_stage = [t for t in current if t not in STAGE_TAGS_DISCOURSE]
            next_tags = non_stage + [stage_tag]
            new_stage = "Accepted" if stage_tag_lower == "p-file" else stage_tag

        if self.config.is_dry_run:
            await interaction.followup.send(
                f"dry-run: would set Discourse tags to: {', '.join(next_tags)}",
                ephemeral=True,
            )
            await self._finish_interaction(interaction, deferred=deferred)
            return

        await self.discourse.set_topic_tags(topic_id, next_tags)
        await self.db.set_tags_last_written(topic_id=topic_id, tags=next_tags)
        if stage_tag_lower == "reject":
            await self.db.set_archive_status(topic_id=topic_id, status="rejected")
        await self.handle_discourse_topic_event(topic_id=topic_id)
        await self._thread_log(
            topic_id=topic_id,
            message=(
                f"{self._format_status_update(new_stage)} "
                f"(by {self._user_display_name(interaction.user)}, discord)"
            ),
        )
        await self._ensure_thread_controls(topic_id=topic_id, allow_create=True)

        if stage_tag_lower == "p-file":
            delay_minutes = self._accepted_archive_delay_minutes()
            when = datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)
            await self.db.mark_accepted(topic_id=topic_id, accepted=True)
            await self.db.set_archive_status(topic_id=topic_id, status="accepted")
            await self.db.schedule_archive(topic_id=topic_id, when_iso=when.isoformat())
            self._cancel_archive(topic_id=topic_id)
            self._schedule_archive(
                topic_id=topic_id,
                delay_seconds=self._accepted_archive_delay_seconds(),
                reason="discord-accepted",
            )
            await self._thread_log(
                topic_id=topic_id,
                message=f"{LOG_TAG_SYSTEM}: {self._accepted_archive_message()}",
            )
        elif stage_tag_lower == "reject":
            delay_minutes = self._accepted_archive_delay_minutes()
            when = datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)
            await self.db.mark_accepted(topic_id=topic_id, accepted=False)
            await self.db.schedule_archive(topic_id=topic_id, when_iso=when.isoformat())
            self._cancel_archive(topic_id=topic_id)
            self._schedule_archive(
                topic_id=topic_id,
                delay_seconds=self._accepted_archive_delay_seconds(),
                reason="discord-rejected",
            )
            await self._thread_log(
                topic_id=topic_id,
                message=f"{LOG_TAG_SYSTEM}: {self._rejected_archive_message()}",
            )
        elif self._is_accepted(current) and stage_tag_lower != "p-file":
            await self.db.mark_accepted(topic_id=topic_id, accepted=False)
            await self.db.set_archive_status(topic_id=topic_id, status=None)
            await self.db.schedule_archive(topic_id=topic_id, when_iso=None)
            self._cancel_archive(topic_id=topic_id)
            await self._thread_log(
                topic_id=topic_id,
                message=f"{LOG_TAG_STATUS}: Reopened (Accepted removed).",
            )
        elif stage_tag_lower not in ("p-file", "reject"):
            await self.db.set_archive_status(topic_id=topic_id, status=None)
            await self.db.schedule_archive(topic_id=topic_id, when_iso=None)
            self._cancel_archive(topic_id=topic_id)
        await self._finish_interaction(interaction, deferred=deferred)

    async def handle_rename_topic(self, interaction: discord.Interaction, *, topic_id: int) -> None:
        try:
            await self._ensure_interaction_allowed_for_topic(interaction, topic_id=topic_id)
        except PermissionError as e:
            await self._respond_ephemeral(interaction, str(e))
            return

        if not isinstance(interaction.user, discord.Member):
            await self._respond_ephemeral(interaction, "Unexpected user type.")
            return

        record = await self.db.get_application(topic_id)
        is_owner = bool(record and record.claimed_by_user_id == interaction.user.id)
        if not (is_owner or self._member_has_override_permission(interaction.user)):
            await self._respond_ephemeral(interaction, "You do not have permission to rename topics.")
            return

        try:
            topic = await self.discourse.fetch_topic(topic_id)
        except Exception:
            topic = None
        modal = RenameTopicModal(
            service=self,
            topic_id=topic_id,
            current_title=topic.title if topic else None,
        )
        await interaction.response.send_modal(modal)

    async def handle_rename_topic_submit(
        self,
        interaction: discord.Interaction,
        *,
        topic_id: int,
        new_title: str,
    ) -> None:
        cleaned = new_title.strip()
        if not cleaned:
            await interaction.response.send_message("Title cannot be empty.", ephemeral=True)
            return

        await interaction.response.defer()
        await self._apply_processing_view(topic_id=topic_id, label="Renaming...")

        if self.config.is_dry_run:
            await self._finish_interaction(interaction, deferred=True, message=None)
            return

        try:
            await self.discourse.set_topic_title(topic_id, cleaned)
        except Exception:
            log.exception("Failed to rename topic (topic_id=%s)", topic_id)
            return

        record = await self.db.get_application(topic_id)
        if record:
            await self.db.set_topic_snapshot(
                topic_id=topic_id,
                title=cleaned,
                author=record.topic_author,
                tags=record.tags_last_seen,
                synced_at=datetime.now(timezone.utc).isoformat(),
            )
        await self._sync_thread_title(topic_id=topic_id, topic_title=cleaned)
        await self._thread_log(
            topic_id=topic_id,
            message=(
                f"{LOG_TAG_SYSTEM}: Application title changed to {cleaned} "
                f"(by {self._user_display_name(interaction.user)}, discord)"
            ),
        )

        await self.handle_discourse_topic_event(
            topic_id=topic_id,
            event_type="topic_edited",
        )


def _verify_discourse_signature(
    *,
    secrets: tuple[str, ...],
    signature: str,
    raw_body: bytes,
    debug: bool = False,
) -> bool:
    def _preview(value: str) -> str:
        if not value:
            return "(empty)"
        if len(value) <= 12:
            return value
        return f"{value[:6]}...{value[-6:]}"

    def _fingerprint(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]

    if not secrets:
        if debug:
            log.info(
                "Discourse signature debug: no secrets configured; body_len=%s",
                len(raw_body),
            )
        return True
    sig = signature.strip()
    if sig.startswith("sha256="):
        sig = sig.split("sha256=", 1)[1].strip()
    if debug:
        log.info(
            "Discourse signature debug: header=%s normalized=%s body_len=%s secrets=%s",
            _preview(signature),
            _preview(sig),
            len(raw_body),
            len(secrets),
        )
    for secret in secrets:
        if not secret:
            continue
        expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        matched = hmac.compare_digest(sig, expected)
        if debug:
            log.info(
                "Discourse signature debug: match=%s expected=%s secret_len=%s secret_fp=%s",
                matched,
                _preview(expected),
                len(secret),
                _fingerprint(secret),
            )
        if matched:
            return True
    return False


async def create_web_app(*, config: BotConfig, bot: BotService) -> web.Application:
    app = web.Application()

    async def health(_: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "mode": config.discord_mode})

    async def discourse_handler(request: web.Request) -> web.Response:
        raw = await request.read()
        event_type = request.headers.get("X-Discourse-Event", "").strip()
        sig = (
            request.headers.get("X-Discourse-Event-Signature", "")
            or request.headers.get("X-Discourse-Event-Signature-SHA256", "")
            or request.headers.get("X-Discourse-Signature", "")
        )
        if config.discourse_signature_debug:
            log.info(
                "Discourse signature debug: content_length=%s body_len=%s encoding=%r body_sha256=%s",
                request.headers.get("Content-Length", ""),
                len(raw),
                request.headers.get("Content-Encoding"),
                hashlib.sha256(raw).hexdigest()[:12],
            )
        if not _verify_discourse_signature(
            secrets=config.discourse_webhook_secrets,
            signature=sig,
            raw_body=raw,
            debug=config.discourse_signature_debug,
        ):
            log.warning("Invalid signature. event=%r remote=%s", event_type, request.remote)
            return web.Response(status=403, text="Invalid signature")

        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return web.Response(status=400, text="Invalid JSON")

        topic_obj = payload.get("topic")
        topic = topic_obj if isinstance(topic_obj, dict) else {}
        topic_id = topic.get("id") or topic.get("topic_id") or payload.get("topic_id") or payload.get("id")
        if topic_id is None and isinstance(topic_obj, dict):
            topic_id = topic_obj.get("id") or topic_obj.get("topic_id")
        try:
            topic_id_int = int(topic_id)
        except Exception:
            log.info("Ignored webhook (no topic id). event=%r keys=%s", event_type, list(payload.keys()))
            return web.Response(status=200, text="Ignored (no topic id)")

        discourse_actor = None
        actor_obj = payload.get("user")
        if isinstance(actor_obj, dict):
            discourse_actor = actor_obj.get("username") or actor_obj.get("name")
        if not discourse_actor and isinstance(topic, dict):
            last_poster = topic.get("last_poster")
            if isinstance(last_poster, dict):
                discourse_actor = last_poster.get("username") or last_poster.get("name")

        log.info("Webhook received. event=%r topic_id=%s", event_type, topic_id_int)
        task = asyncio.create_task(
            bot.handle_discourse_topic_event(
                topic_id=topic_id_int,
                event_type=event_type,
                discourse_actor=discourse_actor,
            )
        )
        task.add_done_callback(_log_task_exceptions)
        return web.Response(status=200, text="OK")

    app.router.add_get("/health", health)
    app.router.add_post("/discourse", discourse_handler)
    return app


async def run() -> None:
    _configure_logging()
    config = load_config()

    async with aiohttp.ClientSession() as session:
        db = BotDb(config.database_path)
        discourse = DiscourseClient(
            base_url=config.discourse_base_url,
            api_key=config.discourse_api_key,
            api_user=config.discourse_api_user,
            session=session,
        )
        bot = BotService(config=config, db=db, discourse=discourse)
        web_app = await create_web_app(config=config, bot=bot)

        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, config.listen_host, config.listen_port)
        await site.start()

        log.info("Webhook server listening on http://%s:%s", config.listen_host, config.listen_port)
        try:
            await bot.start(config.discord_bot_token)
        except asyncio.CancelledError:
            # Normal shutdown path when Ctrl+C is pressed (asyncio.run cancels main task).
            pass
        finally:
            try:
                await bot.close()
            except Exception:
                pass
            try:
                await runner.cleanup()
            except Exception:
                pass


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        # Graceful Ctrl+C without a stack trace.
        return


if __name__ == "__main__":
    main()
