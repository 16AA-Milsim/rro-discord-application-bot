from __future__ import annotations

from dataclasses import dataclass

import discord

from .discourse import DiscourseTopic


STAGE_TAGS_DISCOURSE = (
    "new-application",
    "letter-sent",
    "interview-scheduled",
    "interview-held",
    "on-hold",
    "p-file",
)


def discourse_tags_to_discord(tags: list[str]) -> list[str]:
    out: list[str] = []
    for t in tags:
        out.append("Accepted" if t == "p-file" else t)
    return out


def discord_stage_to_discourse_tag(stage: str) -> str:
    return "p-file" if stage.lower() in ("accept", "accepted") else stage


def discourse_tags_to_stage_label(tags: list[str], *, icons: dict[str, str] | None = None) -> str:
    icons = icons or {}

    def icon(name: str, fallback: str) -> str:
        return icons.get(name) or fallback

    tags_set = set(tags)
    if "p-file" in tags_set:
        return "✅ Accepted"
    if "on-hold" in tags_set:
        return f"{icon('pause', '⏸️')} On Hold"
    if "interview-held" in tags_set:
        return f"{icon('interview_held', '🟨📅')} Interview Held"
    if "interview-scheduled" in tags_set:
        return f"{icon('interview_scheduled', '🟨📅')} Interview Scheduled"
    if "letter-sent" in tags_set:
        return f"{icon('letter_sent', '🟧✉️')} Letter Sent"
    if "new-application" in tags_set:
        return f"{icon('new_application', '🔷')} New Application"
    return "Unknown"


def format_tag_list(tags: list[str]) -> str:
    return ", ".join(tags) if tags else "(none)"


@dataclass(frozen=True)
class RenderedApplication:
    embed: discord.Embed


def build_application_embed(
    *,
    topic: DiscourseTopic,
    tags_discord: list[str],
    stage_label: str,
    claimed_by: discord.abc.User | None,
) -> RenderedApplication:
    owner_value = claimed_by.mention if claimed_by else "⚠️ Unassigned"
    embed = discord.Embed(
        title=f"{topic.title}" if topic.title else "New application",
        url=topic.url,
        description=f"Submitted by **{topic.author}**",
        color=0xAFCBFF,
    )
    embed.add_field(name="Status", value=stage_label, inline=False)
    embed.add_field(
        name="Owner",
        value=owner_value,
        inline=False,
    )
    return RenderedApplication(embed=embed)
