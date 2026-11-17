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

MAIN_TABLES = ["charts", "news", "glossary", "block_panel", "block_richtext", "block_teaser", "block_items", "block_toggle", "block_donation", "pages", "sites", "policies", "policies_attributes", "policies_updates", "illustrations"]

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


# =============================================================================
# Native Directus Translations Support (/translations endpoint)
# =============================================================================

def fetch_native_translations():
    """
    Fetches all native Directus translations from /translations endpoint.
    These are key-value string translations used for UI strings, labels, etc.
    """
    endpoint = f"{DIRECTUS_API_URL}/translations"
    params = {"limit": -1}  # Fetch all translations
    try:
        response = requests.get(endpoint, headers=directus_headers, params=params)
        response.raise_for_status()
        return response.json().get("data", [])
    except Exception as e:
        print(f"❌ Error fetching native translations: {e}")
        return []

def group_native_translations_by_key(translations):
    """
    Groups native translations by their 'key' field.
    Returns a dict: {key: {language: translation_record, ...}, ...}
    """
    grouped = {}
    for t in translations:
        key = t.get("key")
        lang = t.get("language", "").lower()
        if key and lang:
            if key not in grouped:
                grouped[key] = {}
            grouped[key][lang] = t
    return grouped

def insert_native_translation(key, language, value):
    """
    Inserts a new native translation into the /translations endpoint.
    """
    endpoint = f"{DIRECTUS_API_URL}/translations"
    payload = {
        "key": key,
        "language": language,
        "value": value
    }
    try:
        response = requests.post(endpoint, headers=directus_headers, json=payload)
        if response.status_code in (200, 201):
            print(f"✅ Inserted native translation: key='{key}', language='{language}'")
            return True
        else:
            print(f"❌ Failed to insert native translation: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error inserting native translation: {e}")
        return False

def languages_match(lang1, lang2):
    """
    Check if two language codes refer to the same language.
    Handles variants like 'de' vs 'de-de', 'en' vs 'en-us', etc.
    """
    lang1 = lang1.lower().strip()
    lang2 = lang2.lower().strip()
    
    # Exact match
    if lang1 == lang2:
        return True
    
    # Get primary language code (before hyphen)
    primary1 = lang1.split("-")[0]
    primary2 = lang2.split("-")[0]
    
    # Match if primary codes are the same
    return primary1 == primary2

def process_native_translations(base_language="en-us"):
    """
    Processes native Directus translations (/translations endpoint).
    
    For each translation key:
    - Uses the base_language translation as the source
    - Generates translations for missing target languages
    - Prompts user for confirmation before inserting
    
    Args:
        base_language: The source language code to translate from (default: "en-us")
    """
    print("\n" + "=" * 60)
    print("📌 Processing Native Directus Translations (/translations)")
    print("=" * 60 + "\n")
    
    translations = fetch_native_translations()
    if not translations:
        print("⚠️ No native translations found or unable to fetch.")
        return
    
    print(f"📊 Found {len(translations)} native translation records.")
    
    # Group translations by key
    grouped = group_native_translations_by_key(translations)
    print(f"📊 Found {len(grouped)} unique translation keys.\n")
    
    base_lang_lower = base_language.lower()
    
    for key, lang_translations in grouped.items():
        # Check if base language exists for this key
        if base_lang_lower not in lang_translations:
            # Try to find any existing translation as base
            available_langs = list(lang_translations.keys())
            if available_langs:
                base_lang_lower_temp = available_langs[0]
                print(f"⚠️ Key '{key}' has no {base_language} translation. Using '{base_lang_lower_temp}' as base.")
                base_translation = lang_translations[base_lang_lower_temp]
            else:
                print(f"⚠️ Key '{key}' has no translations at all; skipping.")
                continue
        else:
            base_translation = lang_translations[base_lang_lower]
        
        base_value = base_translation.get("value", "")
        if not base_value or not base_value.strip():
            print(f"⚠️ Key '{key}' has empty base value; skipping.")
            continue
        
        # Check which target languages are missing
        for target_lang in TARGET_LANGUAGES:
            target_lang_lower = target_lang.lower()
            
            # Skip if this is the base language (using smart matching)
            if languages_match(target_lang_lower, base_lang_lower):
                continue
            
            # Skip if translation already exists (check all variants)
            already_exists = any(languages_match(target_lang_lower, existing_lang) 
                                for existing_lang in lang_translations.keys())
            if already_exists:
                print(f"✔️ Key '{key}' already has translation for {target_lang.upper()}.")
                continue
            
            print(f"\n🔍 Key '{key}' missing translation for {target_lang.upper()}")
            print(f"   Base ({base_lang_lower}): {base_value}")
            
            # Generate translation
            translated_value = call_chatgpt_for_translation(base_value, target_lang)
            
            # Show translation preview
            print(f"\n--- TRANSLATION PREVIEW ---")
            print(f"Key: {key}")
            print(f"Language: {target_lang}")
            print(f"Original: {base_value}")
            print(f"Translated: {translated_value}")
            print("---------------------------\n")
            
            # Prompt user for confirmation or correction
            print("Options: [y] accept, [n] skip, or type corrected translation:")
            user_input = input("> ").strip()
            
            if user_input.lower() == 'n':
                print("❌ Skipping translation.")
                continue
            elif user_input.lower() == 'y':
                final_value = translated_value
            elif user_input:
                # User provided a corrected translation
                final_value = user_input
                print(f"✏️ Using corrected translation: {final_value}")
            else:
                print("❌ Empty input, skipping translation.")
                continue
            
            # Insert the translation
            insert_native_translation(key, target_lang, final_value)
            sleep(1)

def process_native_translations_batch(base_language="en-us", auto_approve=False):
    """
    Batch process native translations with optional auto-approval.
    
    Args:
        base_language: The source language code to translate from (default: "en-us")
        auto_approve: If True, automatically insert all translations without prompting
    """
    print("\n" + "=" * 60)
    print("📌 Processing Native Directus Translations (Batch Mode)")
    print("=" * 60 + "\n")
    
    translations = fetch_native_translations()
    if not translations:
        print("⚠️ No native translations found or unable to fetch.")
        return
    
    grouped = group_native_translations_by_key(translations)
    base_lang_lower = base_language.lower()
    
    # Collect all missing translations first
    missing_translations = []
    
    for key, lang_translations in grouped.items():
        if base_lang_lower not in lang_translations:
            available_langs = list(lang_translations.keys())
            if not available_langs:
                continue
            base_translation = lang_translations[available_langs[0]]
        else:
            base_translation = lang_translations[base_lang_lower]
        
        base_value = base_translation.get("value", "")
        if not base_value or not base_value.strip():
            continue
        
        for target_lang in TARGET_LANGUAGES:
            target_lang_lower = target_lang.lower()
            
            # Skip if this is the base language (using smart matching)
            if languages_match(target_lang_lower, base_lang_lower):
                continue
            
            # Skip if translation already exists (check all variants)
            already_exists = any(languages_match(target_lang_lower, existing_lang) 
                                for existing_lang in lang_translations.keys())
            if not already_exists:
                missing_translations.append({
                    "key": key,
                    "target_lang": target_lang,
                    "base_value": base_value
                })
    
    print(f"📊 Found {len(missing_translations)} missing native translations.\n")
    
    if not missing_translations:
        print("✅ All native translations are complete!")
        return
    
    if not auto_approve:
        user_input = input(f"Do you want to process all {len(missing_translations)} missing translations? (y/n): ").strip().lower()
        if user_input != 'y':
            print("❌ Batch processing cancelled.")
            return
    
    # Process all missing translations
    for i, item in enumerate(missing_translations, 1):
        print(f"\n[{i}/{len(missing_translations)}] Translating key '{item['key']}' to {item['target_lang'].upper()}...")
        
        translated_value = call_chatgpt_for_translation(item['base_value'], item['target_lang'])
        
        print(f"   Original: {item['base_value']}")
        print(f"   Translated: {translated_value}")
        
        if not auto_approve:
            print("   Options: [y] accept, [n] skip, [a] approve all remaining, or type corrected translation:")
            user_input = input("   > ").strip()
            
            if user_input.lower() == 'a':
                auto_approve = True
                final_value = translated_value
            elif user_input.lower() == 'n':
                print("   ❌ Skipped.")
                continue
            elif user_input.lower() == 'y':
                final_value = translated_value
            elif user_input:
                # User provided a corrected translation
                final_value = user_input
                print(f"   ✏️ Using corrected translation: {final_value}")
            else:
                print("   ❌ Empty input, skipped.")
                continue
        else:
            final_value = translated_value
        
        insert_native_translation(item['key'], item['target_lang'], final_value)
        sleep(0.5)
    
    print("\n✅ Batch processing complete!")


def main():
    print("\n" + "=" * 60)
    print("Directus Translation Tool")
    print("=" * 60)
    print("\nChoose an option:")
    print("1. Process item translations (collection_translations tables)")
    print("2. Process native Directus translations (/translations)")
    print("3. Process both")
    print("4. Process native translations (batch mode)")
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == "1":
        for main_table in MAIN_TABLES:
            process_table(main_table)
    elif choice == "2":
        base_lang = input("Enter base language code (default: en-us): ").strip() or "en-us"
        process_native_translations(base_language=base_lang)
    elif choice == "3":
        # Process item translations
        for main_table in MAIN_TABLES:
            process_table(main_table)
        # Process native translations
        base_lang = input("\nEnter base language code for native translations (default: en-us): ").strip() or "en-us"
        process_native_translations(base_language=base_lang)
    elif choice == "4":
        base_lang = input("Enter base language code (default: en-us): ").strip() or "en-us"
        auto = input("Auto-approve all translations? (y/n): ").strip().lower() == 'y'
        process_native_translations_batch(base_language=base_lang, auto_approve=auto)
    else:
        print("Invalid choice. Running default (item translations only).")
        for main_table in MAIN_TABLES:
            process_table(main_table)

if __name__ == "__main__":
    main()