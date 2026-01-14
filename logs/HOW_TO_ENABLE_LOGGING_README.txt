Logging configuration for the Discord bot

Default behavior:
- INFO level logging to both console and a rotating file at logs/bot.log.
- Rotation defaults: 10 MB max, 5 backups.

Environment variables (set in .env):
- LOG_LEVEL: DEBUG | INFO | WARNING | ERROR (default: INFO)
- LOG_FILE: path to log file (default: logs/bot.log). Set empty to disable file logging.
- LOG_MAX_BYTES: max file size before rotation (default: 10485760)
- LOG_BACKUP_COUNT: number of rotated files to keep (default: 5)
- LOG_TO_CONSOLE: 1 or 0 (default: 1)

Troubleshooting:
- For more detail, set LOG_LEVEL=DEBUG and restart the bot/service.
