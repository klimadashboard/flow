import os
import requests
import openai
import sys
import json
import html
from pathlib import Path
from time import sleep
from dotenv import load_dotenv
import os

# Load from the nearest .env file (searches parent directories too)
load_dotenv()

DIRECTUS_API_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_API_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Configuration
HEADERS = {
    "Authorization": f"Bearer {DIRECTUS_API_TOKEN}",
    "Content-Type": "application/json"
}
openai.api_key = OPENAI_API_KEY

OPENAI_MODEL = "gpt-4"

MAIN_TABLES = ["charts", "news", "glossary", "block_panel", "block_richtext", "block_teaser", "block_items", "block_toggle", "block_donation", "pages", "sites", "policies", "policies_attributes", "policies_updates"]

directus_headers = HEADERS

def sanitize_json_string(value):
    if isinstance(value, str):
        value = value.strip()
        value = html.unescape(value)
    return value

def fetch_target_languages():
    endpoint = f"{DIRECTUS_API_URL}/items/languages"
    try:
        response = requests.get(endpoint, headers=directus_headers)
        response.raise_for_status()
        data = response.json().get("data", [])
        return [lang["code"].lower() for lang in data if "code" in lang]
    except Exception as e:
        print(f"Error fetching target languages: {e}")
        return []

TARGET_LANGUAGES = fetch_target_languages()

def fetch_records(main_table):
    endpoint = f"{DIRECTUS_API_URL}/items/{main_table}"
    params = {"fields": "*,translations.*"}
    response = requests.get(endpoint, headers=directus_headers, params=params)
    response.raise_for_status()
    return response.json().get("data", [])

def find_translation(translations, target_lang):
    for t in translations:
        if t.get("languages_code", "").lower() == target_lang.lower():
            return t
    return None

def get_translatable_fields(translation_record, main_table):
    exclude_fields = {"id", "languages_code", f"{main_table}_id", "created_at", "updated_at"}
    translatable = {}
    for key, value in translation_record.items():
        if key in exclude_fields:
            continue
        if isinstance(value, str) and value.strip():
            translatable[key] = value
        elif isinstance(value, list):
            translatable[key] = value
    return translatable

def translate_json_structure(data, target_lang):
    if isinstance(data, dict):
        return {key: translate_json_structure(value, target_lang) for key, value in data.items() if key not in {"key", "link"}}
    elif isinstance(data, list):
        return [translate_json_structure(item, target_lang) for item in data]
    elif isinstance(data, str) and data.strip():
        return call_chatgpt_for_translation(data, target_lang)
    return data

def call_chatgpt_for_translation(text, target_lang):
    """ Calls OpenAI GPT to translate a single string while preserving structure. """
    system_message = (
        f"You are a translation assistant. Your task is to translate text from any language into {target_lang.upper()} while preserving formatting, placeholders, and HTML tags. "
        "Do NOT translate values where the key is 'key' or 'link'. Ensure correct spelling and grammar."
    )

    try:
        response = openai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": text}  # Ensure this is a string
            ],
            temperature=0.3,
            max_tokens=1500
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"❌ Error during translation: {e}")
        return text  # Return original text if translation fails

def translate_json_structure(data, target_lang):
    """ Recursively translates all translatable fields while preserving JSON structure. """
    if isinstance(data, dict):
        translated_data = {}
        for key, value in data.items():
            if key in {"key", "link"}:  # Do not translate these keys
                translated_data[key] = value
            else:
                translated_data[key] = translate_json_structure(value, target_lang)
        return translated_data

    elif isinstance(data, list):
        # Corrected: Translate each item individually
        translated_list = []
        for item in data:
            if isinstance(item, dict):  # If list contains dictionaries
                translated_list.append(translate_json_structure(item, target_lang))
            elif isinstance(item, str):  # If list contains strings
                translated_list.append(call_chatgpt_for_translation(item, target_lang))
            else:
                translated_list.append(item)  # Leave other types unchanged
        return translated_list

    elif isinstance(data, str) and data.strip():
        return call_chatgpt_for_translation(data, target_lang)

    return data  # Return original if not a translatable type


def insert_translation(main_table, new_translation):
    endpoint = f"{DIRECTUS_API_URL}/items/{main_table}_translations"
    response = requests.post(endpoint, headers=directus_headers, json=new_translation)
    if response.status_code in (200, 201):
        print(f"✅ Inserted translation for {main_table}: {new_translation}")
    else:
        print(f"❌ Failed to insert translation: {response.text}")

def process_table(main_table):
    records = fetch_records(main_table)
    print(f"📌 Processing table '{main_table}' with {len(records)} records.")
    
    for record in records:
        record_id = record.get("id")
        translations = record.get("translations", [])
        if not translations:
            print(f"⚠️ Record {record_id} in {main_table} has no translations; skipping.")
            continue

        base_translation = translations[0]
        for target_lang in TARGET_LANGUAGES:
            if find_translation(translations, target_lang):
                print(f"✔️ Record {record_id} already has a translation for {target_lang.upper()}.")
                continue

            print(f"🔍 Record {record_id} missing translation for {target_lang.upper()}; generating translation...")
            translatable_fields = get_translatable_fields(base_translation, main_table)
            if not translatable_fields:
                print(f"⚠️ No translatable fields found for record {record_id}; skipping.")
                continue

            translated_fields = {}
            for key, value in translatable_fields.items():
                translated_fields[key] = call_chatgpt_for_translation(value, target_lang)

            # Show translation preview
            print("\n--- TRANSLATION PREVIEW ---")
            print(json.dumps(translated_fields, indent=4, ensure_ascii=False))
            print("---------------------------\n")

            user_input = input(f"Do you want to insert this translation for {target_lang.upper()}? (y/n): ").strip().lower()
            if user_input != 'y':
                print("❌ Skipping translation.")
                continue

            new_translation = {f"{main_table}_id": record_id, "languages_code": target_lang}
            for key in translatable_fields:
                new_translation[key] = translated_fields.get(key, translatable_fields[key])
            
            insert_translation(main_table, new_translation)
            sleep(1)

def main():
    for main_table in MAIN_TABLES:
        process_table(main_table)

if __name__ == "__main__":
    main()
