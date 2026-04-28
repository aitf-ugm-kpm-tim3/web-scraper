import json
import os
import sys
from bs4 import BeautifulSoup
from pathlib import Path

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Constants
PROJECT_ROOT = Path(__file__).parent.parent
DB_ROOT = PROJECT_ROOT / 'db'
OUTPUT_LINKS_FILE = DB_ROOT / "siaran_pers_general_links.json"

FILES_TO_SCRAPE = [
    r"d:\1_projects\aitf-ugm-tim3\crawl\Indeks Berita - Kementerian Imigrasi dan Pemasyarakatan RI.html",
    r"d:\1_projects\aitf-ugm-tim3\crawl\Berita Kanwil - Kementerian Imigrasi dan Pemasyarakatan RI.html"
]

SOURCE_NAME = "IMIPAS"

def scrape_file(file_path):
    print(f"Scraping file: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    items = []
    
    # td.list-title a
    links = soup.select('td.list-title a')
    for link in links:
        href = link.get('href', '').strip()
        title = link.get_text(strip=True)
        
        if href and title:
            # Handle potential relative links (though they seem absolute in the HTML)
            if not href.startswith('http'):
                href = f"https://kemenimipas.go.id{href}" if href.startswith('/') else f"https://kemenimipas.go.id/{href}"
            
            items.append({
                "link": href,
                "title": title,
                "source": SOURCE_NAME
            })
            
    print(f"Found {len(items)} items.")
    return items

def main():
    all_new_links = []
    for file_path in FILES_TO_SCRAPE:
        if os.path.exists(file_path):
            all_new_links.extend(scrape_file(file_path))
        else:
            print(f"File not found: {file_path}")

    if not all_new_links:
        print("No links found.")
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
    for item in all_new_links:
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
