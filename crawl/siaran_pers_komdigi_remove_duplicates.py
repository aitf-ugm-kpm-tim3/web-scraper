import json
import os

file_path = r'siaran_pers_komdigi_links.json'

with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Use a list to keep track of seen links to maintain order and remove duplicates
seen_links = set()
unique_data = []

for item in data:
    link = item.get('page')
    if link not in seen_links:
        unique_data.append(item)
        seen_links.add(link)

print(f"Original count: {len(data)}")
print(f"Unique count: {len(unique_data)}")

with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(unique_data, f, indent=2, ensure_ascii=False)

print("Duplicates removed successfully.")
