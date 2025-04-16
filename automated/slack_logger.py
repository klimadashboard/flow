# slack_logger.py
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # Ensure environment variables are loaded

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def slack_log(message, level="INFO"):
    if not SLACK_WEBHOOK_URL:
        print("⚠️ No SLACK_WEBHOOK_URL configured. Not sending any logs.")
        return

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    emoji = {
        "INFO": "ℹ️",
        "SUCCESS": "✅",
        "WARNING": "⚠️",
        "ERROR": "❌"
    }.get(level.upper(), "📌")

    payload = {
        "text": f"{emoji} *{level.upper()}* `{timestamp}`\n{message}"
    }

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            print(f"Slack error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Slack post exception: {e}")
