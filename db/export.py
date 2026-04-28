import sqlite3
import json
import os

db_path = r"d:\1_projects\aitf-ugm-tim3\db\siaran_pers.db"
json_path = r"d:\1_projects\aitf-ugm-tim3\db\siaran_pers_general.json"

def main():
    # Load existing data to avoid overriding/duplicates
    existing_data = []
    existing_links = set()
    
    if os.path.exists(json_path):
        print(f"Loading existing data from {os.path.basename(json_path)}...")
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                if isinstance(existing_data, list):
                    existing_links = {item['link'] for item in existing_data if 'link' in item}
                else:
                    print("Warning: Existing JSON is not a list. Resetting data.")
                    existing_data = []
            print(f"Found {len(existing_data)} existing items.")
        except json.JSONDecodeError:
            print("Warning: Existing JSON is malformed. Starting fresh.")
        except Exception as e:
            print(f"Error reading existing JSON: {e}")

    # Connect to database
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = """
    SELECT 
        u.title,
        u.url as link,
        u.source,
        t.date,
        t.text
    FROM urls u
    JOIN texts t ON u.url = t.url
    """

    print("Fetching data from database...")
    cursor.execute(query)
    rows = cursor.fetchall()

    new_items_count = 0
    for row in rows:
        link = row["link"]
        if link not in existing_links:
            existing_data.append({
                "title": row["title"],
                "link": link,
                "source": row["source"],
                "date": row["date"],
                "text": row["text"]
            })
            existing_links.add(link)
            new_items_count += 1

    if new_items_count > 0:
        print(f"Adding {new_items_count} new items. Total items: {len(existing_data)}")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
        print(f"Successfully updated {os.path.basename(json_path)}!")
    else:
        print("No new items found. JSON file is already up to date.")

    conn.close()

if __name__ == "__main__":
    main()
