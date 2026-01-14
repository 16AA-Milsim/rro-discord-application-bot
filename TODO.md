# TODO: Expand Relay Into Full Discord Bot (Tickets + Tag Sync)

Goal: Replace the current “post to Discord webhook” relay with a proper Discord bot that can (1) post an application notification, (2) allow RRO to claim it and open an internal thread, and (3) keep Discourse application tags in sync between Discourse and Discord.

## 0) Confirm requirements (decisions)

- [x] Confirm target Discord channel for “new application” notifications.
  - [x] Production notify channel ID: `1455133649883103273` (do not use until go-live)
  - [x] Test guild/channel: `1068904351692771480` / `1460263195292864552`
- [x] Confirm roles with access/controls:
  - [x] Visible to: `RRO`, `RRO ICs`, `Coy Commanders`, `REME Discord`
  - [x] Can claim: `RRO`, `RRO ICs`
  - [x] Can reassign/override: `RRO ICs`, `REME Discord`
- [x] Confirm ticket implementation:
  - [x] Create a thread (no parent message)
  - [x] Thread naming convention: `{discourse topic title}`
  - [x] Auto-archive duration: set to maximum (Discord does not support “no auto-archive”)
- [x] Confirm which Discourse tags are in scope (current):
  - [x] `new-application`, `letter-sent`, `interview-scheduled`, `interview-held`, `on-hold`, `p-file`
- [x] Confirm desired Discord display format:
  - [x] Show derived “Status” label in the main channel (with `p-file` ⇄ `Accepted` mapping)
  - [x] Show Owner in the main channel (⚠️ Unassigned / @Owner)

## 1) Architecture & technology choices

- [x] Choose Discord library/runtime:
  - [x] Python `discord.py`
- [x] Choose HTTP server approach for receiving Discourse webhooks:
  - [x] Use `discord.py` + `aiohttp.web` to host `/discourse` in the same process
- [x] Choose persistence:
  - [x] SQLite (single-file DB, `bot.db`)
  - [x] Data model stores:
    - [x] `topic_id` (Discourse)
    - [x] `discord_channel_id`
    - [x] `discord_message_id` (notification message)
    - [x] `discord_thread_id` (ticket thread, if created)
    - [x] `discord_control_message_id` (thread controls message)
    - [x] `claimed_by_user_id` (Discord user)
    - [x] tags state + timestamps
- [x] Define idempotency strategy:
  - [x] Key by `topic_id` (update existing Discord message instead of creating duplicates)

## 2) Secrets & configuration

- [x] Define environment variables (minimum):
  - [x] `DISCORD_BOT_TOKEN` (set in local `.env`, do not commit)
  - [x] `DISCORD_MODE` (`prod` | `test` | `dry-run`)
  - [ ] `DISCORD_GUILD_ID` (production)
  - [ ] `DISCORD_NOTIFY_CHANNEL_ID` (production, `1455133649883103273`)
  - [x] `DISCORD_TEST_GUILD_ID` (test, `1068904351692771480`)
  - [x] `DISCORD_TEST_NOTIFY_CHANNEL_ID` (test, `1460263195292864552`)
  - [ ] `DISCORD_TEST_ARCHIVE_CHANNEL_ID` (optional; accepted summaries)
  - [x] `DISCORD_ALLOWED_ROLE_NAMES` (default: `RRO,RRO ICs`)
  - [x] `DISCORD_OVERRIDE_ROLE_NAMES` (default: `RRO ICs,REME Discord`)
  - [x] `DISCORD_ALLOW_PROD` (must be `1` to enable `DISCORD_MODE=prod`)
  - [x] `DISCOURSE_BASE_URL`
  - [x] `DISCOURSE_WEBHOOK_SECRET` / `DISCOURSE_WEBHOOK_SECRETS`
  - [x] `DISCOURSE_API_KEY` (for tag updates / topic fetch)
  - [x] `DISCOURSE_API_USER` (service account username)
  - [x] `LISTEN_HOST`, `LISTEN_PORT`
- [x] Add `.env.example` and keep `.env` ignored.
- [ ] Rotate any previously shared secrets and update env values.

## 3) Discourse integration

### Webhooks (Discourse → Bot)

- [x] Configure Discourse webhook to include:
  - [x] `Topic is created`
  - [x] `Topic is updated` / `topic_edited` (for tag changes and other updates)
  - [x] Restrict to the Applications category
- [x] Verify tag fields in payload; fetch topic state via API if needed.
- [x] Implement signature verification.

### Discourse API (Bot → Discourse)

- [x] Implement “fetch current topic state”:
  - [x] `GET /t/{topic_id}.json` (tags, slug, url, title, author)
- [x] Implement “set tags” for a topic:
  - [x] `PUT /t/{topic_id}.json` with `tags[]`
  - [x] Ensure the API user has permission to edit tags in the Applications category
- [x] Loop prevention:
  - [x] Suppress “echo” logs when webhook tags match last-written tags

## 4) Discord bot functionality

## 4.1) Safe testing / anti-spam controls

- [x] Implement a single, obvious mode switch:
  - [x] `DISCORD_MODE=test`: posts to test guild/channel only
  - [x] `DISCORD_MODE=dry-run`: no Discord API calls
  - [x] `DISCORD_MODE=prod`: requires `DISCORD_ALLOW_PROD=1`
- [x] Fail closed if guild/channel mismatch.

### Notification message

- [x] Post an embed including:
  - [x] Title, author, direct link to Discourse topic
  - [x] Status label (derived from tags; custom emoji support)
  - [x] Owner (⚠️ Unassigned / @Owner)
- [x] Store the created `message_id` for later edits.

### Thread workflow

- [x] Claim button + controls
  - [x] “Claim Application” (claim roles only)
  - [x] “Unclaim” (RRO + ICs + override)
  - [x] “Reassign” (override roles only)
  - [x] “Change status…” dropdown (allowed roles)
- [x] Thread controls message in the thread (kept in sync with main card).
- [x] Thread workflow log messages (claim/unclaim/reassign/status changes).

### Discourse-driven updates (Discourse → Discord)

- [x] Update the existing Discord embed on Discourse topic updates.
- [x] Log Discourse-driven status changes in the thread (with Discourse username when available).

## 5) Logging, observability, and safety

- [x] `/health` endpoint.
- [ ] Rate limiting / abuse protection on `/discourse`.

## 6) Deployment & migration

- [ ] Decide whether to run as a single service or split services.
- [ ] Update NSSM service config (or equivalent) for the new bot process.
- [ ] Migrate with minimal downtime:
  - [ ] Keep current relay running until the bot is verified
  - [ ] Switch Discourse webhook URL to the new bot endpoint
  - [ ] Confirm claim + tag sync works end-to-end

## 7) Accepted / archive workflow

- [x] When status becomes Accepted (`p-file`), start a 30 minute timer and log it.
- [x] If Accepted is reverted during the timer, cancel the scheduled archive.
- [x] After 30 minutes, archive:
  - [x] Main card becomes an “Accepted” stub (no controls) linking to the thread
  - [x] Thread controls disabled
  - [x] Thread locked + archived
  - [x] Optional archive summary posted to `DISCORD_TEST_ARCHIVE_CHANNEL_ID`

## 8) Validation & testing

- [ ] Add a local webhook payload replay script/command for testing.

