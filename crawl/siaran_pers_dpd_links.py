import json
import os
import sys
from pathlib import Path

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Constants
PROJECT_ROOT = Path(__file__).parent.parent
DB_ROOT = PROJECT_ROOT / 'db'
OUTPUT_LINKS_FILE = DB_ROOT / "siaran_pers_general_links.json"
INPUT_JSON_FILE = DB_ROOT / "data-202642772218.json"

SOURCE_NAME = "DPD"
BASE_URL = "https://www.dpd.go.id/berita-detail/"

def process_json(file_path):
    print(f"Processing file: {file_path}")
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return []
        
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    news_posts = data.get("data", {}).get("newsPost", [])
    items = []
    
    for post in news_posts:
        content_item_id = post.get("contentItemId")
        display_text = post.get("displayText")
        
        if content_item_id and display_text:
            link = f"{BASE_URL}{content_item_id}"
            items.append({
                "link": link,
                "title": display_text,
                "source": SOURCE_NAME
            })
            
    print(f"Extracted {len(items)} items.")
    return items

def main():
    new_links = process_json(INPUT_JSON_FILE)

    if not new_links:
        print("No links extracted.")
        return

    # Load existing links
    existing_all_links = []
    existing_links_set = set()
    if os.path.exists(OUTPUT_LINKS_FILE):
        with open(OUTPUT_LINKS_FILE, 'r', encoding='utf-8') as f:
            existing_all_links = json.load(f)
            existing_links_set = {item['link'] for item in existing_all_links}
        print(f"Loaded {len(existing_all_links)} existing links.")

    # Filter out duplicates
    unique_new_links = []
    for item in new_links:
        if item['link'] not in existing_links_set:
            unique_new_links.append(item)
            existing_links_set.add(item['link'])

    print(f"Found {len(unique_new_links)} unique new links.")

    if unique_new_links:
        # Merge: New links at the top
        final_all_links = unique_new_links + existing_all_links
        
        with open(OUTPUT_LINKS_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_all_links, f, indent=2, ensure_ascii=False)
        print(f"Updated results saved to {OUTPUT_LINKS_FILE}")
    else:
        print("No new links to add.")

if __name__ == "__main__":
    main()
