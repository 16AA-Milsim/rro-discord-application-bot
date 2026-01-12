## RRO Membership Applications Discord Bot

Receives Discourse webhook events for new/updated topics in the Applications category and manages an internal Discord workflow:

- Posts a notification embed into the RRO channel with the current tag set and a derived “Stage” label.
- Provides a “Claim Application” button (claim restricted to `RRO` / `RRO ICs`).
- On claim, opens a thread named `Application - {discourse topic title}` for internal processing.
- Allows override actions (reassign/unclaim) for `RRO ICs` / `REME Discord`.
- Supports bidirectional tag sync for stage tags, with mapping: Discourse `p-file` ⇄ Discord `Accepted`.

This repo also contains the original legacy relay (`relay.py`) which posts via an incoming webhook; the bot implementation lives under `rro_bot/`.

### Endpoints

- `POST /discourse` — Discourse webhook receiver.
- `GET /health` — Health check.

### Discourse setup

In Discourse Admin → Webhooks:

- Point the webhook to your service URL: `http(s)://<host>:5055/discourse`
- Select events:
  - `Topic is created`
  - `Topic is updated` (needed for tag updates)
- Restrict to the Applications category (optional but recommended).
- Set a webhook secret and configure `DISCOURSE_WEBHOOK_SECRET` in the service.

### Configuration (environment variables)

Required:

- `DISCORD_BOT_TOKEN`
- `DISCORD_MODE` (`test` | `dry-run` | `prod`)
- `DISCORD_TEST_GUILD_ID` (default: `1068904351692771480`)
- `DISCORD_TEST_NOTIFY_CHANNEL_ID` (default: `1460263195292864552`)
- `DISCOURSE_BASE_URL` (default: `https://discourse.16aa.net`)
- `LISTEN_HOST` (default: `0.0.0.0`)
- `LISTEN_PORT` (default: `5055`)

Production (only when going live):

- `DISCORD_GUILD_ID`
- `DISCORD_NOTIFY_CHANNEL_ID` (production channel: `1455133649883103273`)
- `DISCORD_ALLOW_PROD=1` (required to enable `DISCORD_MODE=prod`)

Roles (by name):

- `DISCORD_ALLOWED_ROLE_NAMES` (default: `RRO,RRO ICs`)
- `DISCORD_OVERRIDE_ROLE_NAMES` (default: `RRO ICs,REME Discord`)

Discourse API (required for Discord → Discourse tag changes; optional for read-only updates):

- `DISCOURSE_API_KEY`
- `DISCOURSE_API_USER`

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

### Notes / limitations

- Discord threads always have an auto-archive setting; “no auto-archive” is not supported. This bot sets it to the maximum available.
