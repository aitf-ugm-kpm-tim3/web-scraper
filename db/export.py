import sqlite3
import json
import os

db_path = r"d:\1_projects\aitf-ugm-tim3\db\siaran_pers.db"
json_path = r"d:\1_projects\aitf-ugm-tim3\db\siaran_pers_general.json"

def main():
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

    cursor.execute(query)
    rows = cursor.fetchall()

    data = []
    for row in rows:
        data.append({
            "title": row["title"],
            "link": row["link"],
            "source": row["source"],
            "date": row["date"],
            "text": row["text"]
        })

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Successfully exported {len(data)} records to {os.path.basename(json_path)}!")
    conn.close()

if __name__ == "__main__":
    main()
