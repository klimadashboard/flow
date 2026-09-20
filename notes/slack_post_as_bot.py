"""
Post to Slack as the Klimadashbot app (SLACK_BOT_TOKEN) instead of via a
user-linked integration.

Used by the recurring data-freshness audit routine (see
notes/data-freshness-audit.md) to send update proposals to
#team_development without posting under David's own account.

Requires SLACK_BOT_TOKEN in the environment, with `chat:write` scope
(and either channel membership or `chat:write.public`, since
#team_development is a public channel).

Usage:
    from slack_post_as_bot import post_message, post_thread_reply

    TEAM_DEVELOPMENT = "C0237PPU1J6"
    ts = post_message(TEAM_DEVELOPMENT, "short top-line proposal")
    post_thread_reply(TEAM_DEVELOPMENT, ts, "full details, links, risks...")
"""

import os

import requests

SLACK_API = "https://slack.com/api"


def _headers() -> dict:
    token = os.getenv("SLACK_BOT_TOKEN")
    if not token:
        raise RuntimeError("SLACK_BOT_TOKEN not set in environment")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }


def post_message(channel_id: str, text: str) -> str:
    """Post a top-level message. Returns its `ts` (needed for threaded replies)."""
    resp = requests.post(
        f"{SLACK_API}/chat.postMessage",
        headers=_headers(),
        json={"channel": channel_id, "text": text, "unfurl_links": False},
    )
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack chat.postMessage failed: {data.get('error')}")
    return data["ts"]


def post_thread_reply(channel_id: str, thread_ts: str, text: str) -> None:
    """Reply in a thread under an existing message."""
    resp = requests.post(
        f"{SLACK_API}/chat.postMessage",
        headers=_headers(),
        json={
            "channel": channel_id,
            "thread_ts": thread_ts,
            "text": text,
            "unfurl_links": False,
        },
    )
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack chat.postMessage (thread reply) failed: {data.get('error')}")


if __name__ == "__main__":
    # Quick manual sanity check: confirms the token is valid and reports
    # which bot identity/team it resolves to, without posting anything.
    resp = requests.post(f"{SLACK_API}/auth.test", headers=_headers())
    print(resp.json())
