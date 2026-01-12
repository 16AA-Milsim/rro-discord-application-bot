from __future__ import annotations

from dataclasses import dataclass
import os


def _get_env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name)
    if value is None:
        if default is None:
            raise RuntimeError(f"Missing required env var: {name}")
        return default
    return value


def _get_env_int(name: str, default: int | None = None) -> int:
    raw = os.environ.get(name)
    if raw is None:
        if default is None:
            raise RuntimeError(f"Missing required env var: {name}")
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise RuntimeError(f"Invalid int for env var {name}: {raw!r}") from e


def _split_csv(raw: str) -> list[str]:
    return [p.strip() for p in raw.split(",") if p.strip()]


@dataclass(frozen=True)
class BotConfig:
    discord_bot_token: str
    discord_mode: str
    discord_allow_prod: bool

    discord_guild_id: int
    discord_notify_channel_id: int
    discord_test_guild_id: int
    discord_test_notify_channel_id: int

    discord_allowed_role_names: tuple[str, ...]
    discord_override_role_names: tuple[str, ...]

    discourse_base_url: str
    discourse_webhook_secret: str
    discourse_api_key: str
    discourse_api_user: str

    listen_host: str
    listen_port: int

    applications_category_id: int = 328
    database_path: str = "bot.db"

    @property
    def is_dry_run(self) -> bool:
        return self.discord_mode.lower() == "dry-run"

    def target_guild_and_channel(self) -> tuple[int, int]:
        mode = self.discord_mode.lower()
        if mode in ("test", "dry-run"):
            return self.discord_test_guild_id, self.discord_test_notify_channel_id
        if mode == "prod":
            if not self.discord_allow_prod:
                raise RuntimeError(
                    "Refusing to run in DISCORD_MODE=prod without DISCORD_ALLOW_PROD=1"
                )
            if not self.discord_guild_id or not self.discord_notify_channel_id:
                raise RuntimeError(
                    "DISCORD_GUILD_ID and DISCORD_NOTIFY_CHANNEL_ID must be set for DISCORD_MODE=prod"
                )
            return self.discord_guild_id, self.discord_notify_channel_id
        raise RuntimeError(f"Invalid DISCORD_MODE: {self.discord_mode!r} (expected prod|test|dry-run)")


def load_config() -> BotConfig:
    return BotConfig(
        discord_bot_token=_get_env("DISCORD_BOT_TOKEN"),
        discord_mode=os.environ.get("DISCORD_MODE", "test").strip(),
        discord_allow_prod=os.environ.get("DISCORD_ALLOW_PROD", "0").strip() == "1",
        discord_guild_id=_get_env_int("DISCORD_GUILD_ID", 0),
        discord_notify_channel_id=_get_env_int("DISCORD_NOTIFY_CHANNEL_ID", 0),
        discord_test_guild_id=_get_env_int("DISCORD_TEST_GUILD_ID", 1068904351692771480),
        discord_test_notify_channel_id=_get_env_int(
            "DISCORD_TEST_NOTIFY_CHANNEL_ID", 1460263195292864552
        ),
        discord_allowed_role_names=tuple(
            _split_csv(os.environ.get("DISCORD_ALLOWED_ROLE_NAMES", "RRO,RRO ICs"))
        ),
        discord_override_role_names=tuple(
            _split_csv(os.environ.get("DISCORD_OVERRIDE_ROLE_NAMES", "RRO ICs,REME Discord"))
        ),
        discourse_base_url=os.environ.get("DISCOURSE_BASE_URL", "https://discourse.16aa.net").rstrip("/"),
        discourse_webhook_secret=os.environ.get("DISCOURSE_WEBHOOK_SECRET", "").strip(),
        discourse_api_key=os.environ.get("DISCOURSE_API_KEY", "").strip(),
        discourse_api_user=os.environ.get("DISCOURSE_API_USER", "").strip(),
        listen_host=os.environ.get("LISTEN_HOST", "0.0.0.0").strip(),
        listen_port=_get_env_int("LISTEN_PORT", 5055),
    )
