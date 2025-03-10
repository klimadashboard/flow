import time
import html
import requests
import re
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import os

# Load from the nearest .env file (searches parent directories too)
load_dotenv()

# Initialize Slack client
slack_client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

def slack_to_html(slack_text):
    """
    Convert Slack-style text into HTML.
    """
    text = html.unescape(slack_text)
    text = re.sub(r"<@([UW][A-Za-z0-9]+)>", r"<span>@\1</span>", text)
    text = re.sub(r"<!channel>", "<span>@channel</span>", text, flags=re.IGNORECASE)
    text = re.sub(r"<!here>", "<span>@here</span>", text, flags=re.IGNORECASE)
    text = re.sub(r"<#([CW][A-Za-z0-9]+)\|([^>]+)>", r'<a href="#">\2</a>', text)
    text = re.sub(r"<(https?:\/\/[^|]+)\|([^>]+)>", r'<a href="\1">\2</a>', text)
    text = re.sub(r"<(https?:\/\/[^>]+)>", r'<a href="\1">\1</a>', text)
    text = re.sub(r"\*(.*?)\*", r"<strong>\1</strong>", text)
    text = re.sub(r"_(.*?)_", r"<em>\1</em>", text)
    text = re.sub(r"~(.*?)~", r"<del>\1</del>", text)
    return text

def get_slack_user_info(user_id):
    try:
        response = slack_client.users_info(user=user_id)
        return response['user']
    except SlackApiError as e:
        print(f"Error fetching Slack user info: {e.response['error']}")
        return None

def get_directus_user_id(email):
    try:
        response = requests.get(
            f"{os.getenv('DIRECTUS_API_URL')}/users?filter[email][_eq]={email}",
            headers={"Authorization": f"Bearer {os.getenv('DIRECTUS_API_TOKEN')}"}
        )
        response.raise_for_status()
        users = response.json().get('data', [])
        if users:
            return users[0]['id']
    except requests.RequestException as e:
        print(f"Error fetching Directus user ID: {e}")
    return None

def determine_sites(reactions):
    reaction_names = {reaction['name'] for reaction in reactions}
    has_germany = 'de' in reaction_names or 'flag-de' in reaction_names
    has_austria = 'at' in reaction_names or 'flag-at' in reaction_names
    if has_germany and has_austria:
        return "at,de"
    elif has_germany:
        return "de"
    elif has_austria:
        return "at"
    else:
        return "at,de"

def fetch_slack_messages(channel_id):
    try:
        response = slack_client.conversations_history(channel=channel_id, limit=100)
        return sorted(response['messages'], key=lambda msg: float(msg['ts']))
    except SlackApiError as e:
        print(f"Error fetching messages: {e.response['error']}")
        return []

def get_reactions(channel_id, message_ts):
    try:
        response = slack_client.reactions_get(channel=channel_id, timestamp=message_ts)
        return response['message'].get('reactions', [])
    except SlackApiError as e:
        print(f"Error fetching reactions: {e.response['error']}")
        return []

def has_multiple_thumbs_up(reactions):
    for reaction in reactions:
        if reaction['name'] == '+1' and reaction['count'] > 0:
            return True
    return False

def is_message_in_database(message_id):
    try:
        response = requests.get(
            f"{os.getenv('DIRECTUS_API_URL')}/items/news?filter[slack_message_id][_eq]={message_id}",
            headers={"Authorization": f"Bearer {os.getenv('DIRECTUS_API_TOKEN')}"}
        )
        response.raise_for_status()
        return len(response.json().get('data', [])) > 0
    except requests.RequestException as e:
        print(f"Error checking message in database: {e}")
        return False

def add_message_to_database(message_id, message_text, user_id, sites):
    try:
        html_message = slack_to_html(message_text)
        slack_user = get_slack_user_info(user_id)
        directus_author_id = None
        if slack_user and 'profile' in slack_user:
            email = slack_user['profile'].get('email')
            if email:
                directus_author_id = get_directus_user_id(email)
        payload = {
            "slack_message_id": message_id,
            "status": "published",
            "author": directus_author_id,
            "sites": sites,
            "translations": [
                {"languages_code": "de", "text": html_message}
            ]
        }
        response = requests.post(
            f"{os.getenv('DIRECTUS_API_URL')}/items/news",
            headers={"Authorization": f"Bearer {os.getenv('DIRECTUS_API_TOKEN')}"},
            json=payload
        )
        response.raise_for_status()
        print(f"Message {message_id} added to Directus with sites: {sites}.")
    except requests.RequestException as e:
        print(f"Error adding message to database: {e}")

def process_messages():
    messages = fetch_slack_messages(os.getenv("SLACK_CHANNEL_ID_NEWS"))
    for message in messages:
        message_id = message.get('ts')
        message_text = message.get('text', '')
        user_id = message.get('user')
        if not message_id or not message_text or not user_id:
            continue
        if is_message_in_database(message_id):
            print(f"Message {message_id} is already in the database. Skipping.")
            continue
        reactions = get_reactions(os.getenv("SLACK_CHANNEL_ID_NEWS"), message_id)
        if has_multiple_thumbs_up(reactions):
            sites = determine_sites(reactions)
            add_message_to_database(message_id, message_text, user_id, sites)
            break  # Process only the earliest message and exit

if __name__ == "__main__":
    print("Checking for new messages...")
    process_messages()
    print("Done.")
