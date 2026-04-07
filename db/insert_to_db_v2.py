import sqlite3
import json
import os
from rich.progress import track

db_path = r'd:\1_projects\aitf-ugm-tim3\db\siaran_pers.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Clean up wrong tables
cursor.execute("DROP TABLE IF EXISTS general_links")
cursor.execute("DROP TABLE IF EXISTS general_articles")
cursor.execute("DROP TABLE IF EXISTS komdigi_articles")
cursor.execute("DROP TABLE IF EXISTS komdigi_links")
cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_texts_url ON texts(url)")

def upsert_url(url, title, source):
    if not url: return
    cursor.execute('''
        INSERT INTO urls (url, title, source, status, date_created)
        VALUES (?, ?, ?, 0, datetime('now', 'localtime'))
        ON CONFLICT(url) DO UPDATE SET
            title = COALESCE(excluded.title, urls.title),
            source = COALESCE(excluded.source, urls.source)
    ''', (url, title, source))

def insert_text(url, text, date):
    if not url: return
    # Use UPSERT to handle existing entries efficiently
    cursor.execute('''
        INSERT INTO texts (url, text, date)
        VALUES (?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            text = excluded.text,
            date = excluded.date
    ''', (url, text, date))
    cursor.execute("UPDATE urls SET status = 1, date_modified = datetime('now', 'localtime') WHERE url = ?", (url,))

# 1. siaran_pers_general_links
with open(r'd:\1_projects\aitf-ugm-tim3\db\siaran_pers_general_links.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    for item in track(data, description="siaran_pers_general_links"):
        upsert_url(item.get('link'), item.get('title'), item.get('source'))

# 2. siaran_pers_general
with open(r'd:\1_projects\aitf-ugm-tim3\db\siaran_pers_general.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    for item in track(data, description="siaran_pers_general"):
        upsert_url(item.get('link'), item.get('title'), item.get('source'))
        insert_text(item.get('link'), item.get('text'), item.get('date'))

# 3. siaran_pers_komdigi_links
with open(r'd:\1_projects\aitf-ugm-tim3\db\siaran_pers_komdigi_links.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    for item in track(data, description="siaran_pers_komdigi_links"):
        for news in item.get('news_items', []):
            link = news.get('link')
            if link and not link.startswith('http'):
                link = 'https://komdigi.go.id' + link
            upsert_url(link, news.get('title'), 'KOMDIGI')

# 4. siaran_pers_komdigi_all
with open(r'd:\1_projects\aitf-ugm-tim3\db\siaran_pers_komdigi_all.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    for item in track(data, description="siaran_pers_komdigi_all"):
        link = item.get('link')
        if link and not link.startswith('http'):
            link = 'https://komdigi.go.id' + link
        upsert_url(link, item.get('title'), 'KOMDIGI')
        insert_text(link, item.get('text'), item.get('date'))

# # 5. wikipedia_links
# with open(r'd:\1_projects\aitf-ugm-tim3\db\wikipedia_links.json', 'r', encoding='utf-8') as f:
#     data = json.load(f)
#     for link in track(data, description="wikipedia_links"):
#         title = link.replace('https://id.wikipedia.org/wiki/', '').replace('_', ' ')
#         upsert_url(link, title, 'WIKIPEDIA')

# # 6. wikipedia
# with open(r'd:\1_projects\aitf-ugm-tim3\db\wikipedia.json', 'r', encoding='utf-8') as f:
#     data = json.load(f)
#     for item in track(data, description="wikipedia"):
#         upsert_url(item.get('url'), item.get('title'), 'WIKIPEDIA')
#         insert_text(item.get('url'), item.get('text'), item.get('last_modified'))

conn.commit()
conn.close()
print("Data inserted into 'urls' and 'texts' tables successfully.")
