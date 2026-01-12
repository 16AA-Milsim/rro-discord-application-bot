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
  - [x] Create a thread from the notification message
  - [x] Thread naming convention: `Application - {discourse topic title}`
  - [x] Auto-archive duration: set to maximum (Discord does not support “no auto-archive”)
- [x] Confirm which Discourse tags are in scope (current):
  - [x] `new-application`, `letter-sent`, `interview-scheduled`, `interview-held`, `on-hold`, `p-file`
- [x] Confirm desired Discord display format:
  - [x] Show exact tag set (same as Discourse), with mapping: Discourse `p-file` ⇄ Discord `Accepted`
  - [x] Show derived “Stage” label in the main channel (required)

## 1) Architecture & technology choices

- [x] Choose Discord library/runtime:
  - [x] Python `discord.py`
- [x] Choose HTTP server approach for receiving Discourse webhooks:
  - [x] Option C: Use `discord.py` + `aiohttp.web` to host `/discourse` in the same process
- [x] Choose persistence:
  - [x] SQLite (single-file DB, `bot.db`)
  - [x] Data model stores:
    - [x] `topic_id` (Discourse)
    - [x] `discord_channel_id`
    - [x] `discord_message_id` (notification message)
    - [x] `discord_thread_id` (ticket thread, if created)
    - [x] `claimed_by_user_id` (Discord user)
    - [x] `tags_last_seen` (for loop prevention)
    - [x] timestamps (created/updated)
- [x] Define idempotency strategy:
  - [x] Key by `topic_id` (update existing Discord message instead of creating duplicates)

## 2) Secrets & configuration

- [x] Define environment variables (minimum):
  - [x] `DISCORD_BOT_TOKEN` (set in local `.env`, do not commit)
  - [ ] `DISCORD_MODE` (`prod` | `test` | `dry-run`)
  - [ ] `DISCORD_GUILD_ID` (production)
  - [ ] `DISCORD_NOTIFY_CHANNEL_ID` (production, `1455133649883103273`)
  - [ ] `DISCORD_TEST_GUILD_ID` (test, `1068904351692771480`)
  - [ ] `DISCORD_TEST_NOTIFY_CHANNEL_ID` (test, `1460263195292864552`)
  - [ ] `DISCORD_ALLOWED_ROLE_NAMES` (default: `RRO,RRO ICs`)
  - [ ] `DISCORD_OVERRIDE_ROLE_NAMES` (default: `RRO ICs,REME Discord`)
  - [ ] `DISCORD_ALLOW_PROD` (must be `1` to enable `DISCORD_MODE=prod`)
  - [ ] `DISCOURSE_BASE_URL`
  - [ ] `DISCOURSE_WEBHOOK_SECRET`
  - [ ] `DISCOURSE_API_KEY` (for tag updates / topic fetch)
  - [ ] `DISCOURSE_API_USER` (service account username)
  - [ ] `LISTEN_HOST`, `LISTEN_PORT`
- [x] Add `.env.example` and keep `.env` ignored.
- [ ] Rotate any previously shared webhook secrets and update env values.

## 3) Discourse integration

### Webhooks (Discourse → Bot)

- [ ] Configure Discourse webhook to include:
  - [ ] `Topic is created` (already used)
  - [ ] `Topic is updated` (for tag changes and other updates)
  - [ ] Restrict to the Applications category
- [ ] Verify which payload fields are present for tags; if missing, fetch tags via API.
- [x] Implement signature verification.

### Discourse API (Bot → Discourse)

- [x] Implement “fetch current topic state”:
  - [x] `GET /t/{topic_id}.json` (tags, slug, url, title, author)
- [x] Implement “set tags” for a topic:
  - [x] Update topic tags via Discourse API (`PUT /t/{topic_id}.json` with `tags[]`)
  - [ ] Ensure the API user has permission to edit tags in the Applications category
- [ ] Add loop-prevention:
  - [ ] If Discord action sets tags, ignore the immediately-following webhook update if tags match last-written state.

## 4) Discord bot functionality

## 4.1) Safe testing / anti-spam controls

- [x] Implement a single, obvious “switch” to prevent spamming production:
  - [x] `DISCORD_MODE=prod`: only allow posting to `DISCORD_GUILD_ID` + `DISCORD_NOTIFY_CHANNEL_ID` (requires `DISCORD_ALLOW_PROD=1`)
  - [x] `DISCORD_MODE=test`: only allow posting to `DISCORD_TEST_GUILD_ID` + `DISCORD_TEST_NOTIFY_CHANNEL_ID`
  - [x] `DISCORD_MODE=dry-run`: process webhooks and log actions but do not call Discord APIs
- [x] Add hard allowlist checks before any send/edit/thread action:
  - [x] If guild/channel mismatch for the current mode: refuse (fail closed)

### Notification message

- [x] Post an embed including:
  - [x] Title, author, direct link to Discourse topic
  - [x] Current tag set (exact; with `p-file` ⇄ `Accepted` mapping)
  - [x] Stage label (derived from tags)
  - [x] Claim status (“Unclaimed” / “Claimed by @User”)
- [x] Store the created `message_id` for later edits.

### Claim workflow (Discord → Bot)

- [x] Add a persistent “Claim Application” button:
  - [x] Enforce role permissions (claim roles only)
  - [x] Ensure only one claimant (atomic update in DB)
  - [x] Edit message to show claimed status and disable/replace the claim button
- [x] On claim, create a thread from the notification message:
  - [x] Thread name: `Application - {discourse topic title}`
  - [x] Auto-archive: set to maximum available
  - [x] Post initial “ticket context” message in the thread (link + tags)
- [x] Add admin actions (override roles only):
  - [x] Reassign claim to another user
  - [x] Unclaim (re-open claim)

### Tag controls (Discord → Discourse)

- [x] Provide a “Set Tag/Stage” UI in Discord:
  - [x] Use a select menu of allowed stage tags (single-select)
  - [x] Keep UI restricted to claim roles (and override roles)
  - [x] Map Discord “Accepted” ⇄ Discourse `p-file`
- [x] On change, call Discourse API to update tags, then update Discord message/thread display.

### Discourse-driven updates (Discourse → Discord)

- [x] When a relevant topic update webhook arrives:
  - [x] Lookup `topic_id` in DB
  - [x] Fetch current tags via API
  - [x] Edit the existing Discord notification embed to reflect current tag set/stage
  - [ ] Optionally post an update message into the thread (e.g., “Tags updated: …”)

## 5) Logging, observability, and safety

- [ ] Add structured logs (request id, topic id, message id, user id).
- [ ] Add rate limiting / basic abuse protections on `/discourse`.
- [ ] Handle transient failures safely:
  - [ ] Discord API failures: retry with backoff; don’t spam
  - [ ] Discourse API failures: retry/backoff; display “unknown tags” gracefully
- [x] Add a `/health` endpoint returning bot + webhook receiver status.

## 6) Deployment & migration

- [ ] Decide whether to:
  - [ ] Run as a single service (bot + webhook receiver)
  - [ ] Split webhook receiver and bot into two services
- [ ] Update NSSM service config (or equivalent) for the new bot process.
- [ ] Migrate with minimal downtime:
  - [ ] Keep current relay running until the bot is verified
  - [ ] Switch Discourse webhook URL to the new bot endpoint
  - [ ] Confirm claim + tag sync works end-to-end

## 7) Validation & testing

- [ ] Add a local “webhook payload replay” script/command for testing.
- [ ] Add lightweight unit tests for:
  - [ ] signature verification
  - [ ] payload parsing and category filtering
  - [ ] tag display formatting
  - [ ] idempotency (same topic id twice)
- [ ] Run a staging test in a private Discord channel before enabling in production.
