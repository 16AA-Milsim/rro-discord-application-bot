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


def _get_env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in ("1", "true", "yes", "y", "on"):
        return True
    if value in ("0", "false", "no", "n", "off"):
        return False
    raise RuntimeError(f"Invalid bool for env var {name}: {raw!r}")


def _split_csv(raw: str) -> list[str]:
    return [p.strip() for p in raw.split(",") if p.strip()]


@dataclass(frozen=True)
class BotConfig:
    discord_bot_token: str
    discord_mode: str
    discord_allow_prod: bool

    discord_guild_id: int
    discord_notify_channel_id: int
    discord_archive_channel_id: int
    discord_test_guild_id: int
    discord_test_notify_channel_id: int
    discord_test_archive_channel_id: int
    accepted_archive_delay_minutes: int

    discord_allowed_role_names: tuple[str, ...]
    discord_override_role_names: tuple[str, ...]
    discord_thread_autoadd_role_names: tuple[str, ...]

    discourse_base_url: str
    discourse_webhook_secrets: tuple[str, ...]
    discourse_signature_debug: bool
    discourse_api_key: str
    discourse_api_user: str
    discourse_topic_cache_ttl_seconds: int

    listen_host: str
    listen_port: int

    applications_category_id: int
    discourse_test_applications_category_id: int
    database_path: str = "bot.db"

    @property
    def is_dry_run(self) -> bool:
        return self.discord_mode.lower() == "dry-run"

    def target_guild_and_channel(self) -> tuple[int, int]:
        mode = self.discord_mode.lower()
        if mode in ("test", "dry-run"):
            if not self.discord_test_guild_id or not self.discord_test_notify_channel_id:
                raise RuntimeError(
                    "DISCORD_TEST_GUILD_ID and DISCORD_TEST_NOTIFY_CHANNEL_ID must be set for DISCORD_MODE=test or dry-run"
                )
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

    def target_archive_channel_id(self) -> int:
        mode = self.discord_mode.lower()
        if mode in ("test", "dry-run"):
            return self.discord_test_archive_channel_id
        if mode == "prod":
            return self.discord_archive_channel_id
        return 0

    def target_applications_category_id(self) -> int:
        mode = self.discord_mode.lower()
        if mode in ("test", "dry-run"):
            return self.discourse_test_applications_category_id
        return self.applications_category_id


def load_config() -> BotConfig:
    secrets_raw = os.environ.get("DISCOURSE_WEBHOOK_SECRETS", "").strip()
    if secrets_raw:
        discourse_webhook_secrets = tuple(_split_csv(secrets_raw))
    else:
        single = os.environ.get("DISCOURSE_WEBHOOK_SECRET", "").strip()
        discourse_webhook_secrets = (single,) if single else tuple()

    base_applications_category_id = _get_env_int("DISCOURSE_APPLICATIONS_CATEGORY_ID", 328)
    test_applications_category_id = _get_env_int(
        "DISCOURSE_TEST_APPLICATIONS_CATEGORY_ID",
        base_applications_category_id,
    )

    return BotConfig(
        discord_bot_token=_get_env("DISCORD_BOT_TOKEN"),
        discord_mode=os.environ.get("DISCORD_MODE", "test").strip(),
        discord_allow_prod=os.environ.get("DISCORD_ALLOW_PROD", "0").strip() == "1",
        discord_guild_id=_get_env_int("DISCORD_GUILD_ID", 0),
        discord_notify_channel_id=_get_env_int("DISCORD_NOTIFY_CHANNEL_ID", 0),
        discord_archive_channel_id=_get_env_int("DISCORD_ARCHIVE_CHANNEL_ID", 0),
        discord_test_guild_id=_get_env_int("DISCORD_TEST_GUILD_ID", 0),
        discord_test_notify_channel_id=_get_env_int("DISCORD_TEST_NOTIFY_CHANNEL_ID", 0),
        discord_test_archive_channel_id=_get_env_int("DISCORD_TEST_ARCHIVE_CHANNEL_ID", 0),
        accepted_archive_delay_minutes=max(
            0,
            _get_env_int("DISCORD_ACCEPTED_ARCHIVE_DELAY_MINUTES", 30),
        ),
        discord_allowed_role_names=tuple(
            _split_csv(os.environ.get("DISCORD_ALLOWED_ROLE_NAMES", "RRO,RRO ICs"))
        ),
        discord_override_role_names=tuple(
            _split_csv(os.environ.get("DISCORD_OVERRIDE_ROLE_NAMES", "RRO ICs,REME Discord"))
        ),
        discord_thread_autoadd_role_names=tuple(
            _split_csv(os.environ.get("DISCORD_THREAD_AUTOADD_ROLE_NAMES", "RRO,RRO ICs"))
        ),
        discourse_base_url=os.environ.get("DISCOURSE_BASE_URL", "https://discourse.16aa.net").rstrip("/"),
        discourse_webhook_secrets=discourse_webhook_secrets,
        discourse_signature_debug=_get_env_bool("DISCOURSE_SIGNATURE_DEBUG", False),
        discourse_api_key=os.environ.get("DISCOURSE_API_KEY", "").strip(),
        discourse_api_user=os.environ.get("DISCOURSE_API_USER", "").strip(),
        discourse_topic_cache_ttl_seconds=max(
            0,
            _get_env_int("DISCOURSE_TOPIC_CACHE_TTL_SECONDS", 300),
        ),
        listen_host=os.environ.get("LISTEN_HOST", "0.0.0.0").strip(),
        listen_port=_get_env_int("LISTEN_PORT", 5055),
        applications_category_id=base_applications_category_id,
        discourse_test_applications_category_id=test_applications_category_id,
    )
