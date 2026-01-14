from __future__ import annotations

import discord

from .render import discord_stage_to_discourse_tag


class ApplicationView(discord.ui.View):
    def __init__(
        self,
        *,
        topic_id: int,
        service: "BotService",
        claimed: bool,
        processing: bool = False,
        processing_label: str = "Processing...",
        processing_style: discord.ButtonStyle = discord.ButtonStyle.secondary,
        show_reassign_selector: bool = False,
        reassign_options: list[tuple[int, str]] | None = None,
    ):
        super().__init__(timeout=None)
        self._topic_id = topic_id
        self._service = service
        self._show_reassign_selector = show_reassign_selector
        self._reassign_options = reassign_options or []

        if processing:
            claim_button = discord.ui.Button(
                label=processing_label,
                style=processing_style,
                disabled=True,
            )
            self.add_item(claim_button)
        elif not claimed:
            claim_button = discord.ui.Button(
                label="Claim Application",
                style=discord.ButtonStyle.success,
                custom_id=f"app_claim:{topic_id}",
            )
            claim_button.callback = self._on_claim  # type: ignore[assignment]
            self.add_item(claim_button)

        unclaim_button = discord.ui.Button(
            label="Unclaim",
            style=discord.ButtonStyle.secondary,
            disabled=not claimed or processing,
            custom_id=f"app_unclaim:{topic_id}",
            row=1,
        )
        unclaim_button.callback = self._on_unclaim  # type: ignore[assignment]
        self.add_item(unclaim_button)

        reassign_label = "Reassign" if claimed else "Assign"
        reassign_button = discord.ui.Button(
            label=reassign_label,
            style=discord.ButtonStyle.secondary,
            disabled=processing,
            custom_id=f"app_reassign:{topic_id}",
            row=1,
        )
        reassign_button.callback = self._on_reassign  # type: ignore[assignment]
        self.add_item(reassign_button)

        rename_button = discord.ui.Button(
            label="Rename Title",
            style=discord.ButtonStyle.secondary,
            disabled=processing,
            custom_id=f"app_rename:{topic_id}",
            row=1,
        )
        rename_button.callback = self._on_rename  # type: ignore[assignment]
        self.add_item(rename_button)

        if self._show_reassign_selector:
            options = [
                discord.SelectOption(label=name[:100], value=str(uid))
                for uid, name in self._reassign_options[:25]
            ]
            if options:
                placeholder = "Reassign to..." if claimed else "Assign to..."
                reassign_select = discord.ui.Select(
                    placeholder=placeholder,
                    min_values=1,
                    max_values=1,
                    options=options,
                    disabled=processing,
                    custom_id=f"app_reassign_select:{topic_id}",
                    row=2,
                )

                async def _reassign_select_cb(interaction: discord.Interaction) -> None:
                    if not reassign_select.values:
                        await interaction.response.send_message("No user selected.", ephemeral=True)
                        return
                    await self._service.handle_reassign_select(
                        interaction,
                        topic_id=self._topic_id,
                        new_user_id=int(reassign_select.values[0]),
                    )

                reassign_select.callback = _reassign_select_cb  # type: ignore[assignment]
                self.add_item(reassign_select)
            else:
                note = discord.ui.Button(
                    label="No eligible members found",
                    style=discord.ButtonStyle.secondary,
                    disabled=True,
                    row=2,
                )
                self.add_item(note)

        stage_select = discord.ui.Select(
            placeholder="Change status…",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="New Application", value="new-application"),
                discord.SelectOption(label="Letter Sent", value="letter-sent"),
                discord.SelectOption(label="Interview Scheduled", value="interview-scheduled"),
                discord.SelectOption(label="Interview Held", value="interview-held"),
                discord.SelectOption(label="On Hold", value="on-hold"),
                discord.SelectOption(label="Reject", value="reject"),
                discord.SelectOption(label="Accept", value="accept"),
            ],
            disabled=processing,
            custom_id=f"app_stage_select:{topic_id}",
            row=3 if self._show_reassign_selector else 2,
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

    async def _on_rename(self, interaction: discord.Interaction) -> None:
        await self._service.handle_rename_topic(interaction, topic_id=self._topic_id)


class RenameTopicModal(discord.ui.Modal):
    def __init__(
        self,
        *,
        service: "BotService",
        topic_id: int,
        current_title: str | None = None,
    ):
        super().__init__(title="Rename Topic")
        self._service = service
        self._topic_id = topic_id
        self._title_input = discord.ui.TextInput(
            label="New title",
            max_length=200,
            required=True,
            default=current_title or "",
        )
        self.add_item(self._title_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self._service.handle_rename_topic_submit(
            interaction,
            topic_id=self._topic_id,
            new_title=str(self._title_input.value),
        )


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .service import BotService
