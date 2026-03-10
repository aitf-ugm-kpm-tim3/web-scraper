import asyncio
import json
import os
from pathlib import Path
import aiohttp
from pypdf import PdfReader
import io
from datetime import datetime

from config import PERATURAN_CONFIG, get_all_extracted_filename, get_metadata_filename, DB_ROOT

# Base URL for downloads
BASE_URL = "https://peraturan.go.id"

# Use DB_ROOT from config
DB_DIR = DB_ROOT

def parse_pdf_date(date_str):
    """Parses PDF date format (e.g., D:20140130155254+07'00') into ISO string."""
    if not date_str:
        return None
    try:
        # Simple cleanup for pypdf date strings
        clean_date = date_str.replace("D:", "").split('+')[0].split('-')[0]
        # Format: YYYYMMDDHHMMSS
        dt = datetime.strptime(clean_date[:14], "%Y%m%d%H%M%S")
        return dt.isoformat()
    except Exception:
        return date_str

async def extract_metadata(session, url, semaphore):
    """Fetches PDF and extracts metadata."""
    async with semaphore:
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return {"error": f"HTTP {response.status}"}
                
                content = await response.read()
                file_size = len(content)
                
                # Use pypdf to read from bytes
                pdf_file = io.BytesIO(content)
                reader = PdfReader(pdf_file)
                
                meta = reader.metadata
                info = {}
                
                if meta:
                    info["title"] = meta.title
                    info["author"] = meta.author
                    info["subject"] = meta.subject
                    info["keywords"] = meta.keywords
                    info["creator"] = meta.creator
                    info["producer"] = meta.producer
                    info["creation_date"] = parse_pdf_date(meta.get("/CreationDate"))
                    info["modification_date"] = parse_pdf_date(meta.get("/ModDate"))
                
                # Technical specs
                info["pdf_version"] = reader.pdf_header
                info["file_size_bytes"] = file_size
                info["page_count"] = len(reader.pages)
                
                if len(reader.pages) > 0:
                    page = reader.pages[0]
                    # Page size in points (1 point = 1/72 inch)
                    width = float(page.mediabox.width)
                    height = float(page.mediabox.height)
                    info["page_size_points"] = {"width": width, "height": height}
                    # Conventional names (A4 is approx 595x842)
                    if 590 < width < 600 and 840 < height < 850:
                        info["page_size_name"] = "A4"
                    elif 610 < width < 615 and 790 < height < 795:
                        info["page_size_name"] = "Letter"
                
                return info
        except Exception as e:
            return {"error": str(e)}

async def process_regulation_type(session, reg_type, semaphore):
    """Processes all items for a specific regulation type."""
    input_filename = get_all_extracted_filename(reg_type)
    input_path = DB_DIR / input_filename
    
    if not input_path.exists():
        # Check local crawl dir if not in db
        input_path = Path(input_filename)
        if not input_path.exists():
            print(f"Skipping {reg_type}: {input_filename} not found.")
            return

    print(f"Processing {reg_type} from {input_path}...")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Filter to only items that need metadata
    items_to_process = []
    indices = []
    for idx, item in enumerate(data):
        meta = item.get('pdf_metadata')
        # Process if meta is missing or has error
        if not meta or "error" in meta:
            rel_path = item.get('dokumen_peraturan')
            if rel_path and rel_path.endswith('.pdf'):
                items_to_process.append(item)
                indices.append(idx)

    print(f"Total: {len(data)} | Already enriched: {len(data) - len(items_to_process)} | To process: {len(items_to_process)}")

    if not items_to_process:
        print(f"All items for {reg_type} are already enriched.")
        return

    # Extract metadata for the missing ones
    tasks = []
    for item in items_to_process:
        rel_path = item.get('dokumen_peraturan')
        url = f"{BASE_URL}/{rel_path.lstrip('/')}"
        tasks.append(extract_metadata(session, url, semaphore))

    if tasks:
        print(f"Extracting metadata for {len(tasks)} missing PDFs in {reg_type}...")
        metadata_results = await asyncio.gather(*tasks)
        
        # Merge back into original data using indices
        for idx, meta in zip(indices, metadata_results):
            data[idx]['pdf_metadata'] = meta

    output_filename = get_metadata_filename(reg_type)
    output_path = DB_DIR / output_filename
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Saved enriched data to {output_path}")

async def main():
    semaphore = asyncio.Semaphore(5) # Limit concurrency
    async with aiohttp.ClientSession() as session:
        # We can process types in sequence to avoid massive memory usage
        for reg_type in PERATURAN_CONFIG.keys():
            await process_regulation_type(session, reg_type, semaphore)

if __name__ == "__main__":
    asyncio.run(main())
