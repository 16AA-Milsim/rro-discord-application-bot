## RRO Membership Applications Discord Bot

Receives Discourse webhook events for new/updated topics in the Applications category and manages an internal Discord workflow:

- Posts a notification embed into the RRO channel with the current tag set and a derived "Stage" label.
- Provides a "Claim Application" button (claim restricted to `RRO` / `RRO ICs`).
- Creates a thread named `{discourse topic title}` for internal processing (prefers no parent message; falls back to a message thread when required).
- Allows override actions (reassign/unclaim) for `RRO ICs` / `REME Discord`.
- Supports bidirectional tag sync for stage tags, with mapping: Discourse `p-file` -> Discord `Accepted`, and cleared tags -> `Rejected`.
- Allows renaming the topic title from Discord (owner or override roles).

This repo also contains the original legacy relay (`relay.py`) which posts via an incoming webhook; the bot implementation lives under `rro_bot/`.

### Endpoints

- `POST /discourse` - Discourse webhook receiver.
- `GET /health` - Health check.

### Discourse setup

In Discourse Admin -> Webhooks:

- Point the webhook to your service URL: `http(s)://<host>:5055/discourse`
- Select events:
  - `Topic is created`
  - `Topic is updated` (needed for tag updates)
- Restrict to the Applications category (optional but recommended).
- Set a webhook secret and configure `DISCOURSE_WEBHOOK_SECRET` in the service.
  - If you have multiple Discourse webhooks posting to the same Payload URL with different secrets, set `DISCOURSE_WEBHOOK_SECRETS` to a comma-separated list.

### Configuration (environment variables)

Required:

- `DISCORD_BOT_TOKEN`
- `DISCORD_MODE` (`test` | `dry-run` | `prod`)
- `DISCORD_TEST_GUILD_ID` (required for `DISCORD_MODE=test` or `dry-run`)
- `DISCORD_TEST_NOTIFY_CHANNEL_ID` (required for `DISCORD_MODE=test` or `dry-run`)
- `DISCORD_TEST_ARCHIVE_CHANNEL_ID` (optional; post accepted/rejected summaries here)
- `DISCORD_ACCEPTED_ARCHIVE_DELAY_MINUTES` (default: `30`; set `0` for immediate archive)
- `DISCOURSE_BASE_URL` (default: `https://discourse.16aa.net`)
- `DISCOURSE_APPLICATIONS_CATEGORY_ID` (production category id)
- `DISCOURSE_TEST_APPLICATIONS_CATEGORY_ID` (optional; overrides category when `DISCORD_MODE=test` or `dry-run`)
- `DISCOURSE_TOPIC_CACHE_TTL_SECONDS` (default: `300`; set `0` to disable cache)
- `LISTEN_HOST` (default: `0.0.0.0`)
- `LISTEN_PORT` (default: `5055`)

Production (only when going live):

- `DISCORD_GUILD_ID`
- `DISCORD_NOTIFY_CHANNEL_ID` (production channel id)
- `DISCORD_ARCHIVE_CHANNEL_ID` (optional; post accepted/rejected summaries here)
- `DISCORD_ALLOW_PROD=1` (required to enable `DISCORD_MODE=prod`)

Roles (by name):

- `DISCORD_ALLOWED_ROLE_NAMES` (default: `RRO,RRO ICs`)
- `DISCORD_OVERRIDE_ROLE_NAMES` (default: `RRO ICs,REME Discord`)
- `DISCORD_THREAD_AUTOADD_ROLE_NAMES` (currently unused; kept for future role auto-adds)

Discourse API (required for Discord -> Discourse tag changes; optional for read-only updates):

- `DISCOURSE_API_KEY`
- `DISCOURSE_API_USER`
- `DISCOURSE_WEBHOOK_SECRET` (or `DISCOURSE_WEBHOOK_SECRETS`)
- `DISCOURSE_SIGNATURE_DEBUG` (optional; set `1` to log signature debug info)

Logging (optional):

- `LOG_LEVEL` (DEBUG | INFO | WARNING | ERROR; default: INFO)
- `LOG_FILE` (default: `logs/bot.log`; set empty to disable file logging)
- `LOG_MAX_BYTES` (default: `10485760`)
- `LOG_BACKUP_COUNT` (default: `5`)
- `LOG_TO_CONSOLE` (default: `1`)

### Safe testing modes

- `DISCORD_MODE=test`: posts only into the configured test server/channel.
- `DISCORD_MODE=dry-run`: processes webhooks and logs actions but does not post/edit in Discord.
- `DISCORD_MODE=prod`: posts to production, but only if `DISCORD_ALLOW_PROD=1` is set.

### Running locally

1) Create and activate a virtual environment.
2) Install dependencies:

```powershell
pip install -r requirements.txt
```

3) Create a local `.env` (ignored by git) based on `.env.example` and fill in values.
4) Run the bot + webhook server:

```powershell
python .\\bot_service.py
```

### Discord bot permissions

Applications channel (or its category):

- View Channel
- Send Messages
- Send Messages in Threads
- Create Public Threads
- Manage Threads (lock/archive/delete)
- Manage Messages (delete thread system messages)
- Read Message History

Archive channel (if used):

- View Channel
- Send Messages
- Send Messages in Threads
- Create Public Threads
- Read Message History

Server-level:

- View Audit Log (used to identify manual deletions)

Notes:

- If private threads are used, the bot must be explicitly added to each thread.
- Message Content Intent must be enabled to capture manual notes in transcripts.

### Notes / limitations

- Discord threads always have an auto-archive setting; "no auto-archive" is not supported. This bot sets it to the maximum available.
- Reassign dropdown needs Discord "Server Members Intent" enabled for the bot so it can list eligible members.
- Thread membership: the bot only auto-adds the current owner to the thread.
- When status becomes Accepted (`p-file`) or Rejected (tags cleared), the bot waits the configured delay (minutes; can be `0`), then archives: disables controls in the thread, locks+archives the thread, posts a summary card to `DISCORD_TEST_ARCHIVE_CHANNEL_ID` when set, creates an archive thread containing a plain-text log transcript, removes the main channel card (falling back to a stub if needed), and then deletes the original thread.
- Manual deletion handling: if the card or thread is deleted, the bot logs an audit summary in the archive channel and cleans up its database record.
