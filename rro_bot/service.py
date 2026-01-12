from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging

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
from .views import ApplicationView, ReassignSelectView


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

    async def handle_discourse_topic_event(self, *, topic_id: int) -> None:
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
        claimed_user = None
        claimed = False
        if record and record.claimed_by_user_id:
            claimed_user = self.get_user(record.claimed_by_user_id)
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

    async def _create_thread_if_needed(self, *, message: discord.Message, topic_title: str, topic_id: int) -> int:
        record = await self.db.get_application(topic_id)
        if record and record.discord_thread_id:
            return record.discord_thread_id

        base_name = f"Application - {topic_title}".strip()
        thread_name = base_name[:100] if len(base_name) > 100 else base_name

        # Discord does not support disabling auto-archive. Prefer the maximum, but fall back
        # if the guild does not allow it.
        archive_options = (10080, 4320, 1440)
        last_error: Exception | None = None
        for duration in archive_options:
            try:
                thread = await message.create_thread(
                    name=thread_name,
                    auto_archive_duration=duration,
                )
                break
            except Exception as e:
                last_error = e
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

        thread_id = await self._create_thread_if_needed(message=msg, topic_title=topic.title, topic_id=topic_id)
        thread = await channel.fetch_thread(thread_id)
        if thread:
            await thread.send(
                f"Handler: {interaction.user.mention}\n"
                f"Topic: {topic.url}\n"
                f"Tags: {', '.join(discourse_tags_to_discord(topic.tags)) or '(none)'}"
            )

        await self.handle_discourse_topic_event(topic_id=topic_id)
        await interaction.response.send_message("Claimed and thread opened.", ephemeral=True)

    async def handle_unclaim(self, interaction: discord.Interaction, *, topic_id: int) -> None:
        try:
            self._ensure_interaction_in_target(interaction)
        except PermissionError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Unexpected user type.", ephemeral=True)
            return

        if not self._member_has_admin_permission(interaction.user):
            await interaction.response.send_message("Only override roles can unclaim.", ephemeral=True)
            return

        await self.db.force_claim(topic_id=topic_id, user_id=None)
        if self.config.is_dry_run:
            await interaction.response.send_message("dry-run: unclaimed in DB.", ephemeral=True)
            return

        await self.handle_discourse_topic_event(topic_id=topic_id)
        await interaction.response.send_message("Unclaimed.", ephemeral=True)

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

        await interaction.response.send_message(
            "Pick the new handler:",
            view=ReassignSelectView(topic_id=topic_id, service=self),
            ephemeral=True,
        )

    async def handle_force_claim(self, interaction: discord.Interaction, *, topic_id: int, new_user_id: int) -> None:
        await self.db.force_claim(topic_id=topic_id, user_id=new_user_id)
        if not self.config.is_dry_run:
            await self.handle_discourse_topic_event(topic_id=topic_id)

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

        topic = await self.discourse.fetch_topic(topic_id)
        current = list(topic.tags)

        non_stage = [t for t in current if t not in STAGE_TAGS_DISCOURSE]
        next_tags = non_stage + [stage_tag]

        if self.config.is_dry_run:
            await interaction.response.send_message(
                f"dry-run: would set Discourse tags to: {', '.join(next_tags)}",
                ephemeral=True,
            )
            return

        await self.discourse.set_topic_tags(topic_id, next_tags)
        await self.db.set_tags_last_written(topic_id=topic_id, tags=next_tags)
        await self.handle_discourse_topic_event(topic_id=topic_id)
        await interaction.response.send_message("Stage updated.", ephemeral=True)


def _verify_discourse_signature(*, secret: str, signature: str, raw_body: bytes) -> bool:
    if not secret:
        return True
    sig = signature.strip()
    if sig.startswith("sha256="):
        sig = sig.split("sha256=", 1)[1].strip()
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, expected)


async def create_web_app(*, config: BotConfig, bot: BotService) -> web.Application:
    app = web.Application()

    async def health(_: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "mode": config.discord_mode})

    async def discourse_handler(request: web.Request) -> web.Response:
        raw = await request.read()
        sig = (
            request.headers.get("X-Discourse-Event-Signature", "")
            or request.headers.get("X-Discourse-Event-Signature-SHA256", "")
            or request.headers.get("X-Discourse-Signature", "")
        )
        if not _verify_discourse_signature(secret=config.discourse_webhook_secret, signature=sig, raw_body=raw):
            return web.Response(status=403, text="Invalid signature")

        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return web.Response(status=400, text="Invalid JSON")

        topic = payload.get("topic") or {}
        topic_id = topic.get("id") or topic.get("topic_id") or payload.get("topic_id") or payload.get("id")
        try:
            topic_id_int = int(topic_id)
        except Exception:
            return web.Response(status=200, text="Ignored (no topic id)")

        task = asyncio.create_task(bot.handle_discourse_topic_event(topic_id=topic_id_int))
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
        await bot.start(config.discord_bot_token)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
