#!/usr/bin/env python3
"""Post to and read from Slack as @klimadashbot.

Unlike slack_logger.py (fire-and-forget webhook), this uses SLACK_BOT_TOKEN
so messages can be threaded, read back, and reacted to. Stdlib only — no
venv or pip install needed.

Usage:
  slack_bot.py whoami                                              -> bot identity
  slack_bot.py post    --channel C0237PPU1J6 --text "..."          -> prints ts
  slack_bot.py reply   --channel C0237PPU1J6 --thread TS --text "..."
  slack_bot.py replies --channel C0237PPU1J6 --thread TS           -> JSON
  slack_bot.py react   --channel C0237PPU1J6 --thread TS --emoji white_check_mark

Every command prints JSON on stdout and exits non-zero on failure.
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

API = "https://slack.com/api/"


def load_env():
    """Read .env from the repo root without requiring python-dotenv."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, ".env")
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and value and key not in os.environ:
                os.environ[key] = value


def call(method, payload, token):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        API + method,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            body = json.loads(res.read().decode())
    except urllib.error.URLError as e:
        return {"ok": False, "error": f"http_failure: {e}"}
    return body


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("whoami")
    for name in ("post", "reply", "replies", "react"):
        p = sub.add_parser(name)
        p.add_argument("--channel", required=True)
        p.add_argument("--text")
        p.add_argument("--thread")
        p.add_argument("--emoji", default="white_check_mark")

    args = parser.parse_args()
    load_env()
    token = os.getenv("SLACK_BOT_TOKEN")
    if not token:
        print(json.dumps({"ok": False, "error": "SLACK_BOT_TOKEN not set"}))
        return 1

    if args.command == "whoami":
        result = call("auth.test", {}, token)
    elif args.command == "post":
        result = call("chat.postMessage", {"channel": args.channel, "text": args.text}, token)
    elif args.command == "reply":
        result = call(
            "chat.postMessage",
            {"channel": args.channel, "thread_ts": args.thread, "text": args.text},
            token,
        )
    elif args.command == "replies":
        result = call(
            "conversations.replies",
            {"channel": args.channel, "ts": args.thread, "limit": 200},
            token,
        )
        if result.get("ok"):
            result = {
                "ok": True,
                "messages": [
                    {
                        "ts": m.get("ts"),
                        "user": m.get("user"),
                        "bot_id": m.get("bot_id"),
                        "text": m.get("text"),
                        "reactions": [r.get("name") for r in m.get("reactions", [])],
                    }
                    for m in result.get("messages", [])
                ],
            }
    elif args.command == "react":
        result = call(
            "reactions.add",
            {"channel": args.channel, "timestamp": args.thread, "name": args.emoji},
            token,
        )

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
