from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import logging
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web
import discord

from .config import BotConfig, load_config
from .db import BotDb
from .discourse import DiscourseClient
from .render import (
    STAGE_TAGS_DISCOURSE,
    build_application_embed,
    discourse_tags_to_discord,
    discourse_tags_to_stage_label,
)
from .views import ApplicationView


log = logging.getLogger("rro_bot")

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
        super().__init__(intents=intents)
        self.config = config
        self.db = db
        self.discourse = discourse
        self._topic_locks: dict[int, asyncio.Lock] = {}
        self._archive_tasks: dict[int, asyncio.Task] = {}

    async def setup_hook(self) -> None:
        await self.db.init()

    async def on_ready(self) -> None:
        log.info("Logged in as %s", self.user)
        await self._restore_views()
        await self._restore_scheduled_archives()

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

        notify_msg = await self._get_notify_message(topic_id=topic_id)
        parent_channel = self.get_channel(record.discord_channel_id)
        if not isinstance(parent_channel, discord.TextChannel):
            parent_channel = None

        # Ensure we have a thread; if acceptance happened without a claim/thread, create one.
        thread = await self._get_thread_for_topic(topic_id=topic_id)
        if thread is None and notify_msg:
            if parent_channel:
                _ = await self._create_thread_if_needed(
                    channel=parent_channel,
                    message=notify_msg,
                    topic_title=topic.title,
                    topic_id=topic_id,
                )
                thread = await self._get_thread_for_topic(topic_id=topic_id)

        thread_link = None
        if thread:
            guild_id, _ = self._target_ids()
            thread_link = f"https://discord.com/channels/{guild_id}/{thread.id}"

        # Thread: disable controls and lock/archive.
        if thread and record.discord_control_message_id:
            try:
                controls_msg = await thread.fetch_message(record.discord_control_message_id)
                embed = controls_msg.embeds[0] if controls_msg.embeds else None
                await controls_msg.edit(content="Archived (Accepted)", embed=embed, view=None)
            except Exception:
                pass
        if thread:
            try:
                await thread.edit(locked=True, archived=True)
            except Exception:
                pass

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
                embed = discord.Embed(
                    title=topic.title or "Application",
                    url=topic.url,
                    color=0x2ecc71,
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
                await notify_msg.delete()
                notify_msg = None
            except discord.NotFound:
                notify_msg = None
            except Exception:
                log.exception("Failed to delete archived notification (topic_id=%s)", topic_id)

        # Fallback: keep a minimal Accepted stub if we did not delete the message.
        if notify_msg:
            try:
                embed, _view = await self._render_for_topic(topic_id=topic_id)
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
                await thread.delete()
            except Exception:
                log.exception("Failed to delete archived thread (topic_id=%s)", topic_id)
            if parent_channel:
                await self._delete_thread_system_message(channel=parent_channel, thread=thread)

        await self.db.mark_archived(topic_id=topic_id, archived=True)
        await self.db.schedule_archive(topic_id=topic_id, when_iso=None)

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
        content = msg.content or ""
        if msg.attachments:
            attachments = " ".join(a.url for a in msg.attachments)
            if content:
                content += " "
            content += f"[attachments: {attachments}]"
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

        return f"[{timestamp} UTC] {author}: {content}"

    async def _send_transcript_to_thread(
        self,
        *,
        source_thread: discord.Thread,
        dest_thread: discord.Thread,
    ) -> int:
        max_len = 1900
        buffer = ""
        messages_sent = 0
        async for msg in source_thread.history(limit=None, oldest_first=True):
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

    async def _get_notify_message(self, *, topic_id: int) -> discord.Message | None:
        record = await self.db.get_application(topic_id)
        if not record:
            return None
        channel = self.get_channel(record.discord_channel_id)
        if not isinstance(channel, discord.TextChannel):
            return None
        try:
            return await channel.fetch_message(record.discord_message_id)
        except Exception:
            return None

    async def _ensure_thread_controls(self, *, topic_id: int) -> None:
        record = await self.db.get_application(topic_id)
        if not record or not record.discord_thread_id:
            return
        if record.archived_at:
            return
        thread = await self._get_thread_for_topic(topic_id=topic_id)
        if not thread:
            return

        # Send or update a pinned controls message in the thread.
        embed, view = await self._render_for_topic(topic_id=topic_id)
        controls_msg: discord.Message | None = None

        if record.discord_control_message_id:
            try:
                controls_msg = await thread.fetch_message(record.discord_control_message_id)
            except Exception:
                controls_msg = None

        if controls_msg is None:
            controls_msg = await thread.send(content="Controls", embed=embed, view=view)
            await self.db.set_control_message_id(topic_id=topic_id, message_id=controls_msg.id)
        else:
            try:
                await controls_msg.edit(content="Controls", embed=embed, view=view)
            except Exception:
                pass

    def _member_has_claim_permission(self, member: discord.Member) -> bool:
        allowed = {n.lower() for n in self.config.discord_allowed_role_names}
        return any(role.name.lower() in allowed for role in member.roles)

    def _member_has_override_permission(self, member: discord.Member) -> bool:
        allowed = {n.lower() for n in (self.config.discord_allowed_role_names + self.config.discord_override_role_names)}
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

    async def _render_for_topic(
        self,
        *,
        topic_id: int,
        show_reassign_selector: bool = False,
        claimed_by_override: discord.abc.User | None = None,
        reassign_options: list[tuple[int, str]] | None = None,
    ) -> tuple[discord.Embed, ApplicationView]:
        topic = await self.discourse.fetch_topic(topic_id)
        tags_discord = discourse_tags_to_discord(topic.tags)
        stage_label = discourse_tags_to_stage_label(topic.tags, icons=self._status_icons())

        record = await self.db.get_application(topic_id)
        if record and record.archive_status == "rejected":
            stage_label = "Rejected"
        claimed_user = claimed_by_override or await self._resolve_claimed_user(
            user_id=record.claimed_by_user_id if record else None
        )
        view = ApplicationView(
            topic_id=topic_id,
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
        if topic.category_id != self.config.applications_category_id:
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
        view = ApplicationView(topic_id=topic_id, service=self, claimed=claimed)

        if record:
            if self.config.is_dry_run:
                log.info("dry-run: would edit message topic_id=%s message_id=%s", topic_id, record.discord_message_id)
            else:
                msg = await channel.fetch_message(record.discord_message_id)
                await msg.edit(embed=rendered.embed, view=view)
            await self.db.set_tags_last_seen(topic_id=topic_id, tags=topic.tags)
            await self._ensure_thread_controls(topic_id=topic_id)

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
                            message=self._accepted_archive_message(),
                        )
                    elif reopened:
                        await self.db.mark_accepted(topic_id=topic_id, accepted=False)
                        await self.db.set_archive_status(topic_id=topic_id, status=None)
                        await self.db.schedule_archive(topic_id=topic_id, when_iso=None)
                        self._cancel_archive(topic_id=topic_id)
                        await self._thread_log(topic_id=topic_id, message="Reopened (Accepted removed).")

            # If Discourse tags changed, log it in the thread (if one exists), unless it matches
            # tags we just wrote from Discord (to avoid duplicate "echo" logs).
            if previous_tags is not None and previous_tags != topic.tags and not suppress_echo:
                prev_stage = self._stage_tag_from_discourse_tags(previous_tags)
                new_stage = self._stage_tag_from_discourse_tags(topic.tags)
                actor = discourse_actor or "Unknown"
                await self._thread_log(
                    topic_id=topic_id,
                    message=(
                        f"Status (discourse) changed by {actor}: "
                        f"**{prev_stage}** -> **{new_stage}**"
                    ),
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
        )

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

        base_name = f"{topic_title}".strip()
        thread_name = base_name[:100] if len(base_name) > 100 else base_name

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
        return thread.id

    async def _create_archive_thread(
        self,
        *,
        message: discord.Message,
        topic_title: str,
    ) -> discord.Thread:
        base_name = f"Application - {topic_title}".strip()
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
        raise last_error or RuntimeError("Failed to create archive thread")

    async def _delete_thread_system_message(
        self,
        *,
        channel: discord.TextChannel,
        thread: discord.Thread,
    ) -> None:
        try:
            async for msg in channel.history(limit=50):
                if msg.type == discord.MessageType.thread_created:
                    msg_thread = getattr(msg, "thread", None)
                    if msg_thread and msg_thread.id == thread.id:
                        await msg.delete()
                        return
                    if thread.name and thread.name in msg.content:
                        await msg.delete()
                        return
        except Exception:
            log.exception("Failed to delete thread system message (thread_id=%s)", thread.id)

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
        responded = False
        if interaction.message:
            try:
                await interaction.response.edit_message(view=processing_view)
                responded = True
            except Exception:
                responded = False
        deferred = False if responded else await self._defer_interaction(interaction)

        record = await self.db.get_application(topic_id)
        if not record:
            await self._respond_ephemeral(interaction, "Internal error: missing record.")
            return
        had_thread = bool(record.discord_thread_id)

        channel = self.get_channel(record.discord_channel_id)
        if not isinstance(channel, discord.TextChannel):
            await self._respond_ephemeral(interaction, "Internal error: channel missing.")
            return

        msg = await channel.fetch_message(record.discord_message_id)
        topic = await self.discourse.fetch_topic(topic_id)
        if not responded:
            notify_msg = await self._get_notify_message(topic_id=topic_id)
            if notify_msg:
                try:
                    await notify_msg.edit(view=processing_view)
                except Exception:
                    pass

        thread_id = await self._create_thread_if_needed(
            channel=channel,
            message=msg,
            topic_title=topic.title,
            topic_id=topic_id,
        )
        _ = thread_id
        await self._ensure_thread_controls(topic_id=topic_id)
        await self._thread_log(
            topic_id=topic_id,
            message=f"Claimed by {self._user_label(interaction.user)}.",
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

        if not self._member_has_override_permission(interaction.user):
            await self._respond_ephemeral(interaction, "Only RRO / ICs can unclaim.")
            return

        before = await self.db.get_application(topic_id)
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
        responded = False
        if interaction.message:
            try:
                await interaction.response.edit_message(view=processing_view)
                responded = True
            except Exception:
                responded = False
        deferred = False if responded else await self._defer_interaction(interaction)
        if not responded:
            try:
                if interaction.message:
                    await interaction.message.edit(view=processing_view)
                else:
                    notify_msg = await self._get_notify_message(topic_id=topic_id)
                    if notify_msg:
                        await notify_msg.edit(view=processing_view)
            except Exception:
                pass

        await self._ensure_thread_controls(topic_id=topic_id)
        await self.handle_discourse_topic_event(topic_id=topic_id)
        previous = await self._resolve_claimed_user(user_id=before.claimed_by_user_id) if before else None
        prev_text = self._user_label(previous)
        await self._thread_log(
            topic_id=topic_id,
            message=f"Unclaimed by {self._user_label(interaction.user)} (previous owner: {prev_text}).",
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
            await self._respond_ephemeral(interaction, "Only override roles can reassign.")
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
        responded = False
        if interaction.message:
            try:
                await interaction.response.edit_message(view=processing_view)
                responded = True
            except Exception:
                responded = False
        deferred = False
        if not responded:
            try:
                deferred = await self._defer_interaction(interaction)
                if interaction.message:
                    await interaction.message.edit(view=processing_view)
                else:
                    notify_msg = await self._get_notify_message(topic_id=topic_id)
                    if notify_msg:
                        await notify_msg.edit(view=processing_view)
            except Exception:
                pass
            if deferred:
                await self._finish_interaction(interaction, deferred=deferred)

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
            await self._ensure_thread_controls(topic_id=topic_id)

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
            await self._respond_ephemeral(interaction, "Only override roles can reassign.")
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
        responded = False
        if interaction.message:
            try:
                await interaction.response.edit_message(view=processing_view)
                responded = True
            except Exception:
                responded = False
        deferred = False
        if not responded:
            deferred = await self._defer_interaction(interaction)
            try:
                if interaction.message:
                    await interaction.message.edit(view=processing_view)
                else:
                    notify_msg = await self._get_notify_message(topic_id=topic_id)
                    if notify_msg:
                        await notify_msg.edit(view=processing_view)
            except Exception:
                pass

        await self.db.force_claim(topic_id=topic_id, user_id=new_user_id)
        await self.handle_discourse_topic_event(topic_id=topic_id)

        await self._ensure_thread_controls(topic_id=topic_id)
        previous = await self._resolve_claimed_user(user_id=before.claimed_by_user_id) if before else None
        prev_text = self._user_label(previous)
        new_user = target_member or await self._resolve_claimed_user(user_id=new_user_id)
        new_text = self._user_label(new_user) if new_user else f"User {new_user_id}"
        await self._thread_log(
            topic_id=topic_id,
            message=f"Reassigned by {self._user_label(interaction.user)}: {prev_text} -> {new_text}.",
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

        if not self._member_has_override_permission(interaction.user):
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
        responded = False
        if interaction.message:
            try:
                await interaction.response.edit_message(view=processing_view)
                responded = True
            except Exception:
                responded = False
        deferred = False
        if not responded:
            deferred = await self._defer_interaction(interaction)
            try:
                if interaction.message:
                    await interaction.message.edit(view=processing_view)
                else:
                    notify_msg = await self._get_notify_message(topic_id=topic_id)
                    if notify_msg:
                        await notify_msg.edit(view=processing_view)
            except Exception:
                pass

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
                f"Status (discord) changed by {self._user_label(interaction.user)}: "
                f"**{prev_stage}** -> **{new_stage}**"
            ),
        )
        await self._ensure_thread_controls(topic_id=topic_id)

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
                message=self._accepted_archive_message(),
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
                message=self._rejected_archive_message(),
            )
        elif self._is_accepted(current) and stage_tag_lower != "p-file":
            await self.db.mark_accepted(topic_id=topic_id, accepted=False)
            await self.db.set_archive_status(topic_id=topic_id, status=None)
            await self.db.schedule_archive(topic_id=topic_id, when_iso=None)
            self._cancel_archive(topic_id=topic_id)
            await self._thread_log(topic_id=topic_id, message="Reopened (Accepted removed).")
        elif stage_tag_lower not in ("p-file", "reject"):
            await self.db.set_archive_status(topic_id=topic_id, status=None)
            await self.db.schedule_archive(topic_id=topic_id, when_iso=None)
            self._cancel_archive(topic_id=topic_id)
        # Update message without posting extra chatter.
        try:
            embed, view = await self._render_for_topic(topic_id=topic_id)
            notify_msg = await self._get_notify_message(topic_id=topic_id)
            if notify_msg:
                await notify_msg.edit(embed=embed, view=view)
        except Exception:
            pass
        await self._finish_interaction(interaction, deferred=deferred)


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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
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
