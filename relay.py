import os
import hmac
import hashlib
import json
from flask import Flask, request, abort
from datetime import datetime, timezone
import requests

app = Flask(__name__)

# ---- CONFIG ----
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
DISCOURSE_BASE_URL = os.environ.get("DISCOURSE_BASE_URL", "https://discourse.16aa.net").rstrip("/")
DISCOURSE_WEBHOOK_SECRET = os.environ.get("DISCOURSE_WEBHOOK_SECRET", "").strip()

# Applications category (from https://discourse.16aa.net/c/rro/applications/328)
APPLICATIONS_CATEGORY_ID = 328

LISTEN_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "5055"))

if not DISCORD_WEBHOOK_URL:
    raise RuntimeError("DISCORD_WEBHOOK_URL env var is missing")

# ---- HELPERS ----
def verify_discourse_signature(raw_body: bytes) -> None:
    """
    Discourse webhooks support a signature header. The exact header format may vary by version/config.
    We support both:
      - X-Discourse-Event-Signature: sha256=<hex>
      - X-Discourse-Event-Signature: <hex>
    If no secret is configured, we skip verification (not recommended).
    """
    if not DISCOURSE_WEBHOOK_SECRET:
        return

    sig = (
        request.headers.get("X-Discourse-Event-Signature", "")
        or request.headers.get("X-Discourse-Event-Signature-SHA256", "")
        or request.headers.get("X-Discourse-Signature", "")
    )

    if not sig:
        abort(403, "Missing signature header")

    if sig.startswith("sha256="):
        sig = sig.split("sha256=", 1)[1].strip()

    expected = hmac.new(
        DISCOURSE_WEBHOOK_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(sig, expected):
        abort(403, "Invalid signature")

def post_to_discord(title: str, url: str, author: str, category_name: str = "Applications") -> None:
    embed = {
        "title": title or "New application",
        "url": url,
        "description": f"Submitted by **{author}**",
        "color": 0x940039,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": f"Discourse • {category_name}"}
    }

    payload = {
        "content": "A new 16AA Membership Application has been submitted",
        "embeds": [embed]
    }

    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        print("Discord response:", r.status_code, r.text[:300])
        r.raise_for_status()
    except Exception as e:
        print("Discord post failed:", repr(e))
        # Do not raise; avoid Discourse retry storms during transient errors

# ---- ROUTES ----
@app.get("/health")
def health():
    return {"status": "ok"}, 200

@app.post("/discourse")
def discourse():
    raw_bytes = request.get_data(cache=True)  # read once, reuse
    verify_discourse_signature(raw_bytes)

    # Try JSON decode regardless of Content-Type
    payload = None
    try:
        payload = request.get_json(silent=True)
    except Exception:
        payload = None

    if payload is None:
        raw_str = raw_bytes.decode("utf-8", errors="replace").strip()
        if not raw_str:
            ct = request.headers.get("Content-Type", "")
            print("Empty body. Content-Type:", ct)
            return "No JSON payload", 200

        try:
            payload = json.loads(raw_str)
        except Exception:
            ct = request.headers.get("Content-Type", "")
            print("No JSON parsed. Content-Type:", ct)
            print("Raw body (first 300 chars):", raw_str[:300])
            return "No JSON payload", 200

    print("Received webhook. Payload keys:", list(payload.keys()))

    topic = payload.get("topic") or {}
    category = payload.get("category") or {}

    category_id = (
        category.get("id")
        or topic.get("category_id")
        or payload.get("category_id")
        or (payload.get("topic") or {}).get("category_id")
        or (payload.get("topic") or {}).get("category", {}).get("id")
    )

    print("Extracted category_id:", category_id)

    if int(category_id or 0) != APPLICATIONS_CATEGORY_ID:
        print("Ignored: expected", APPLICATIONS_CATEGORY_ID, "got", category_id)
        return "Ignored (not Applications category)", 200

    title = topic.get("title") or payload.get("title") or "New application"

    # URL may not be included in webhook payload; construct it if needed
    url = topic.get("url") or payload.get("url")
    if url and url.startswith("/"):
        url = f"{DISCOURSE_BASE_URL}{url}"

    if not url:
        slug = topic.get("slug")
        topic_id = topic.get("id") or topic.get("topic_id")
        if slug and topic_id:
            url = f"{DISCOURSE_BASE_URL}/t/{slug}/{topic_id}"

    created_by = topic.get("created_by") or payload.get("created_by") or {}
    author = created_by.get("username") or created_by.get("name") or "Unknown"

    if not url:
        print("No URL found/constructed. Topic keys:", list(topic.keys()))
        return "No URL found in payload", 200

    post_to_discord(
        title=title,
        url=url,
        author=author,
        category_name=category.get("name", "Applications")
    )
    return "OK", 200

if __name__ == "__main__":
    app.run(host=LISTEN_HOST, port=LISTEN_PORT)
