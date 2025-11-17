#!/usr/bin/env python3
import os
import sys
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

CAMP_API_KEY = os.getenv("CAMP_API_KEY")
CAMP_ORG_ID = os.getenv("CAMP_ORG_ID")
CAMP_MANDATE_ID = os.getenv("CAMP_MANDATE_ID")

DIRECTUS_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_API_TOKEN")

# Optional: override year via env, otherwise current year
CAMP_YEAR = int(os.getenv("CAMP_YEAR", datetime.now().year))

def assert_env():
    missing = []
    for key in ["CAMP_API_KEY", "CAMP_ORG_ID", "CAMP_MANDATE_ID", "DIRECTUS_API_URL", "DIRECTUS_API_TOKEN"]:
        if not os.getenv(key):
            missing.append(key)

    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

def get_campai_donation_cents():
    """
    Calls Campai balances endpoint and returns the donation account balance in cents.
    """
    url = f"https://cloud.campai.com/api/{CAMP_ORG_ID}/{CAMP_MANDATE_ID}/finance/accounting/balances/list"
    headers = {
        "X-API-Key": CAMP_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "range": {
            "year": CAMP_YEAR
        }
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if not resp.ok:
        print(
            "Campai API error:",
            resp.status_code,
            resp.reason,
            resp.text[:2000],
            file=sys.stderr,
        )
        resp.raise_for_status()

    data = resp.json()

    account_balances = (data or {}).get("accountBalances") or []
    donation_account = next((b for b in account_balances if b.get("account") == 40400), None)

    if not donation_account:
        raise RuntimeError("Donation account 40400 not found in Campai response")

    donation_cents = donation_account.get("balance")

    if donation_cents is None:
        raise RuntimeError(f"'balance' field not found on donation account: {json.dumps(donation_account, ensure_ascii=False)}")

    if not isinstance(donation_cents, (int, float)):
        raise RuntimeError(f"Unexpected donation balance type: {type(donation_cents)}")

    return int(round(donation_cents))


def update_directus_donation_status(amount_eur_int: int):
    url = f"{DIRECTUS_URL.rstrip('/')}/items/org_donation"
    headers = {
        "Authorization": f"Bearer {DIRECTUS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"donationStatus": amount_eur_int}

    resp = requests.patch(url, headers=headers, json=payload, timeout=30)
    if not resp.ok:
        print(
            "Directus update error:",
            resp.status_code,
            resp.reason,
            resp.text[:2000],
            file=sys.stderr,
        )
        resp.raise_for_status()

    return resp.json()


def main():
    try:
        assert_env()

        donation_cents = get_campai_donation_cents()
        # Convert cents → integer EUR
        donation_eur_int = int(round(donation_cents / 100))

        print(f"Campai donation balance: {donation_cents} cents → {donation_eur_int} €")

        updated = update_directus_donation_status(donation_eur_int)
        print("Directus org_donation updated successfully.")
        print(json.dumps(updated, indent=2, ensure_ascii=False))

    except Exception as e:
        print("Error:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
