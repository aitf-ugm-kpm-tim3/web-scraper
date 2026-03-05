import asyncio
import json
import os
from pathlib import Path
import aiohttp

# Base URL for downloads
BASE_URL = "https://peraturan.go.id"

from config import PERATURAN_CONFIG, get_all_extracted_filename

# List of JSON files to process (generated dynamically from config)
json_files = [get_all_extracted_filename(p) for p in PERATURAN_CONFIG.keys()]

# Output directory for PDFs
DOWNLOAD_DIR = Path('pdf_downloads')

async def download_file(session, url, file_path):
    """Downloads a single PDF file asynchronously."""
    if file_path.exists():
        print(f"Skipping (already exists): {file_path.name}")
        return

    try:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, 'wb') as f:
                    f.write(content)
                print(f"Downloaded: {file_path.name}")
            else:
                print(f"Failed to download {url}: Status {response.status}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

async def main():
    # 1. Create base download directory
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for json_filename in json_files:
            json_path = Path(json_filename)
            if not json_path.exists():
                print(f"Warning: {json_filename} not found.")
                continue

            print(f"Processing {json_filename}...")
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract type from filename for subfolder organization (optional but good)
            # e.g., 'uu', 'perpres', etc.
            reg_type = json_filename.split('_')[-1].replace('.json', '')
            reg_dir = DOWNLOAD_DIR / reg_type
            reg_dir.mkdir(parents=True, exist_ok=True)

            for item in data:
                relative_path = item.get('dokumen_peraturan')
                if relative_path and relative_path.endswith('.pdf'):
                    # Handle potential leading slash
                    clean_path = relative_path.lstrip('/')
                    download_url = f"{BASE_URL}/{clean_path}"
                    
                    filename = Path(clean_path).name
                    file_path = reg_dir / filename
                    
                    tasks.append(download_file(session, download_url, file_path))

        if tasks:
            print(f"Starting download of {len(tasks)} files...")
            # Using semaphore to limit concurrency if needed, but for small sets it's fine
            # For massive sets, use a semaphore or process in smaller chunks
            await asyncio.gather(*tasks)
        else:
            print("No PDF links found to download.")

if __name__ == "__main__":
    asyncio.run(main())
