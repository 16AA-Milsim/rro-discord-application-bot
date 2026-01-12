from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone

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

    async def setup_hook(self) -> None:
        await self.db.init()

    async def on_ready(self) -> None:
        log.info("Logged in as %s", self.user)
        await self._restore_views()

    async def _restore_views(self) -> None:
        for record in await self.db.list_applications():
            self.add_view(
                ApplicationView(
                    topic_id=record.topic_id,
                    service=self,
                    claimed=record.claimed_by_user_id is not None,
                )
            )

    def _target_ids(self) -> tuple[int, int]:
        return self.config.target_guild_and_channel()

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
        if not interaction.channel or interaction.channel.id != target_channel_id:
            raise PermissionError("Wrong channel for current DISCORD_MODE")

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
        stage_label = discourse_tags_to_stage_label(topic.tags)

        record = await self.db.get_application(topic_id)
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
        topic = await self.discourse.fetch_topic(topic_id)
        if topic.category_id != self.config.applications_category_id:
            return

        tags_discord = discourse_tags_to_discord(topic.tags)
        stage_label = discourse_tags_to_stage_label(topic.tags)

        _, target_channel_id = self._target_ids()
        channel = self.get_channel(target_channel_id)

        if not isinstance(channel, discord.TextChannel):
            if self.config.is_dry_run:
                log.info("dry-run: would post/update topic_id=%s title=%r", topic_id, topic.title)
                return
            raise RuntimeError(f"Channel not found or not a text channel: {target_channel_id}")

        record = await self.db.get_application(topic_id)
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

            # If Discourse tags changed, log it in the thread (if one exists), unless it matches
            # tags we just wrote from Discord (to avoid duplicate "echo" logs).
            if previous_tags is not None and previous_tags != topic.tags:
                suppress_echo = bool(
                    record.tags_last_written is not None
                    and sorted(record.tags_last_written) == sorted(topic.tags)
                )
                if not suppress_echo:
                    prev_stage = self._stage_tag_from_discourse_tags(previous_tags)
                    new_stage = self._stage_tag_from_discourse_tags(topic.tags)
                    actor = discourse_actor or "Unknown"
                    await self._thread_log(
                        topic_id=topic_id,
                        message=(
                            f"Status (discourse) changed by {actor}: "
                            f"{prev_stage} → {new_stage}"
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

    async def handle_claim(self, interaction: discord.Interaction, *, topic_id: int) -> None:
        try:
            self._ensure_interaction_in_target(interaction)
        except PermissionError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Unexpected user type.", ephemeral=True)
            return

        if not self._member_has_claim_permission(interaction.user):
            await interaction.response.send_message("Only RRO can claim applications.", ephemeral=True)
            return

        ok = await self.db.try_claim(topic_id=topic_id, user_id=interaction.user.id)
        if not ok:
            await interaction.response.send_message("This application is already claimed.", ephemeral=True)
            return

        if self.config.is_dry_run:
            await interaction.response.send_message("dry-run: claim recorded; no Discord updates.", ephemeral=True)
            return

        record = await self.db.get_application(topic_id)
        if not record:
            await interaction.response.send_message("Internal error: missing record.", ephemeral=True)
            return

        channel = self.get_channel(record.discord_channel_id)
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Internal error: channel missing.", ephemeral=True)
            return

        msg = await channel.fetch_message(record.discord_message_id)
        topic = await self.discourse.fetch_topic(topic_id)

        # Update the main message (no ephemeral "Only you can see this" on success).
        embed, view = await self._render_for_topic(
            topic_id=topic_id,
            claimed_by_override=interaction.user,
        )
        await interaction.response.edit_message(embed=embed, view=view)

        thread_id = await self._create_thread_if_needed(
            channel=channel,
            message=msg,
            topic_title=topic.title,
            topic_id=topic_id,
        )
        _ = thread_id
        thread = await self._get_thread_for_topic(topic_id=topic_id)
        if thread:
            await thread.send(
                f"{self._discord_ts()} Thread opened.\n"
                f"Handler: {interaction.user.mention}\n"
                f"Topic: {topic.url}\n"
                f"Status: {discourse_tags_to_stage_label(topic.tags)}"
            )
        await self._thread_log(topic_id=topic_id, message=f"Claimed by {interaction.user.mention}.")

        await self.handle_discourse_topic_event(topic_id=topic_id)

    async def handle_unclaim(self, interaction: discord.Interaction, *, topic_id: int) -> None:
        try:
            self._ensure_interaction_in_target(interaction)
        except PermissionError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Unexpected user type.", ephemeral=True)
            return

        if not self._member_has_override_permission(interaction.user):
            await interaction.response.send_message("Only RRO / ICs can unclaim.", ephemeral=True)
            return

        before = await self.db.get_application(topic_id)
        await self.db.force_claim(topic_id=topic_id, user_id=None)
        if self.config.is_dry_run:
            await interaction.response.send_message("dry-run: unclaimed in DB.", ephemeral=True)
            return

        embed, view = await self._render_for_topic(topic_id=topic_id)
        await interaction.response.edit_message(embed=embed, view=view)
        await self.handle_discourse_topic_event(topic_id=topic_id)
        previous = await self._resolve_claimed_user(user_id=before.claimed_by_user_id) if before else None
        prev_text = previous.mention if previous else "someone"
        await self._thread_log(
            topic_id=topic_id,
            message=f"Unclaimed by {interaction.user.mention} (previous handler: {prev_text}).",
        )

    async def handle_reassign(self, interaction: discord.Interaction, *, topic_id: int) -> None:
        try:
            self._ensure_interaction_in_target(interaction)
        except PermissionError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Unexpected user type.", ephemeral=True)
            return

        if not self._member_has_admin_permission(interaction.user):
            await interaction.response.send_message("Only override roles can reassign.", ephemeral=True)
            return

        # Show a temporary user selector on the main message (avoid ephemeral noise).
        options = await self._build_reassign_options()
        embed, view = await self._render_for_topic(
            topic_id=topic_id,
            show_reassign_selector=True,
            reassign_options=options,
        )
        await interaction.response.edit_message(embed=embed, view=view)

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
            self._ensure_interaction_in_target(interaction)
        except PermissionError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Unexpected user type.", ephemeral=True)
            return

        if not self._member_has_admin_permission(interaction.user):
            await interaction.response.send_message("Only override roles can reassign.", ephemeral=True)
            return

        guild_id, _ = self._target_ids()
        guild = self.get_guild(guild_id)
        if not guild:
            await interaction.response.send_message("Guild not available.", ephemeral=True)
            return

        target_member = guild.get_member(new_user_id)
        if target_member and not self._member_is_claim_eligible(target_member):
            await interaction.response.send_message(
                "That user is not eligible (must have RRO or RRO ICs).",
                ephemeral=True,
            )
            return

        before = await self.db.get_application(topic_id)
        await self.db.force_claim(topic_id=topic_id, user_id=new_user_id)
        await self.handle_discourse_topic_event(topic_id=topic_id)

        embed, view = await self._render_for_topic(topic_id=topic_id, show_reassign_selector=False)
        await interaction.response.edit_message(embed=embed, view=view)
        previous = await self._resolve_claimed_user(user_id=before.claimed_by_user_id) if before else None
        prev_text = previous.mention if previous else "Unassigned"
        await self._thread_log(
            topic_id=topic_id,
            message=f"Reassigned by {interaction.user.mention}: {prev_text} → <@{new_user_id}>.",
        )

    async def handle_set_stage(self, interaction: discord.Interaction, *, topic_id: int, stage_tag: str) -> None:
        try:
            self._ensure_interaction_in_target(interaction)
        except PermissionError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Unexpected user type.", ephemeral=True)
            return

        if not self._member_has_override_permission(interaction.user):
            await interaction.response.send_message("You do not have permission to change stage.", ephemeral=True)
            return

        if not interaction.response.is_done():
            await interaction.response.defer(thinking=False)

        topic = await self.discourse.fetch_topic(topic_id)
        current = list(topic.tags)
        prev_stage = self._stage_tag_from_discourse_tags(current)

        non_stage = [t for t in current if t not in STAGE_TAGS_DISCOURSE]
        next_tags = non_stage + [stage_tag]
        new_stage = "Accepted" if stage_tag == "p-file" else stage_tag

        if self.config.is_dry_run:
            await interaction.followup.send(
                f"dry-run: would set Discourse tags to: {', '.join(next_tags)}",
                ephemeral=True,
            )
            return

        await self.discourse.set_topic_tags(topic_id, next_tags)
        await self.db.set_tags_last_written(topic_id=topic_id, tags=next_tags)
        await self.handle_discourse_topic_event(topic_id=topic_id)
        await self._thread_log(
            topic_id=topic_id,
            message=f"Status (discord) changed by {interaction.user.mention}: {prev_stage} → {new_stage}",
        )
        # Update message without posting extra chatter.
        try:
            embed, view = await self._render_for_topic(topic_id=topic_id)
            await interaction.message.edit(embed=embed, view=view)
        except Exception:
            pass


def _verify_discourse_signature(*, secrets: tuple[str, ...], signature: str, raw_body: bytes) -> bool:
    if not secrets:
        return True
    sig = signature.strip()
    if sig.startswith("sha256="):
        sig = sig.split("sha256=", 1)[1].strip()
    for secret in secrets:
        if not secret:
            continue
        expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        if hmac.compare_digest(sig, expected):
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
        if not _verify_discourse_signature(
            secrets=config.discourse_webhook_secrets,
            signature=sig,
            raw_body=raw,
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
