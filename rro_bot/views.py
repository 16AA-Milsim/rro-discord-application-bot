from __future__ import annotations

import discord

from .render import discord_stage_to_discourse_tag


class ReassignSelectView(discord.ui.View):
    def __init__(self, *, topic_id: int, service: "BotService"):
        super().__init__(timeout=60)
        self.add_item(_ReassignUserSelect(topic_id=topic_id, service=service))


class _ReassignUserSelect(discord.ui.UserSelect):
    def __init__(self, *, topic_id: int, service: "BotService"):
        super().__init__(
            placeholder="Select a new handler…",
            min_values=1,
            max_values=1,
            custom_id=f"app_reassign_select:{topic_id}",
        )
        self._topic_id = topic_id
        self._service = service

    async def callback(self, interaction: discord.Interaction) -> None:
        user = self.values[0]
        await self._service.handle_force_claim(interaction, topic_id=self._topic_id, new_user_id=user.id)
        await interaction.response.edit_message(content=f"Reassigned to {user.mention}.", view=None)


class ApplicationView(discord.ui.View):
    def __init__(
        self,
        *,
        topic_id: int,
        service: "BotService",
        claimed: bool,
    ):
        super().__init__(timeout=None)
        self._topic_id = topic_id
        self._service = service

        claim_button = discord.ui.Button(
            label="Claimed" if claimed else "Claim Application",
            style=discord.ButtonStyle.secondary if claimed else discord.ButtonStyle.success,
            disabled=claimed,
            custom_id=f"app_claim:{topic_id}",
        )
        claim_button.callback = self._on_claim  # type: ignore[assignment]
        self.add_item(claim_button)

        unclaim_button = discord.ui.Button(
            label="Unclaim",
            style=discord.ButtonStyle.secondary,
            custom_id=f"app_unclaim:{topic_id}",
            row=1,
        )
        unclaim_button.callback = self._on_unclaim  # type: ignore[assignment]
        self.add_item(unclaim_button)

        reassign_button = discord.ui.Button(
            label="Reassign",
            style=discord.ButtonStyle.secondary,
            custom_id=f"app_reassign:{topic_id}",
            row=1,
        )
        reassign_button.callback = self._on_reassign  # type: ignore[assignment]
        self.add_item(reassign_button)

        stage_select = discord.ui.Select(
            placeholder="Set stage tag…",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="New Application", value="new-application"),
                discord.SelectOption(label="Letter Sent", value="letter-sent"),
                discord.SelectOption(label="Interview Scheduled", value="interview-scheduled"),
                discord.SelectOption(label="Interview Held", value="interview-held"),
                discord.SelectOption(label="On Hold", value="on-hold"),
                discord.SelectOption(label="Accepted", value="Accepted"),
            ],
            custom_id=f"app_stage_select:{topic_id}",
            row=2,
        )
        async def _stage_select_cb(interaction: discord.Interaction) -> None:
            if not stage_select.values:
                await interaction.response.send_message("No stage selected.", ephemeral=True)
                return
            stage = str(stage_select.values[0])
            discourse_tag = discord_stage_to_discourse_tag(stage)
            await self._service.handle_set_stage(
                interaction,
                topic_id=self._topic_id,
                stage_tag=discourse_tag,
            )

        stage_select.callback = _stage_select_cb  # type: ignore[assignment]
        self.add_item(stage_select)

    async def _on_claim(self, interaction: discord.Interaction) -> None:
        await self._service.handle_claim(interaction, topic_id=self._topic_id)

    async def _on_unclaim(self, interaction: discord.Interaction) -> None:
        await self._service.handle_unclaim(interaction, topic_id=self._topic_id)

    async def _on_reassign(self, interaction: discord.Interaction) -> None:
        await self._service.handle_reassign(interaction, topic_id=self._topic_id)


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .service import BotService
