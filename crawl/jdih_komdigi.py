import sys
import io
import json
import os
import asyncio

# Set console encoding to UTF-8 to handle special characters on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

INPUT_FILE = "jdih_komdigi_abstrak.json"
OUTPUT_FILE = "jdih_komdigi.json"

schema = {
    "name": "JDIH Komdigi Detail",
    "baseSelector": "section.content-section",
    "fields": [
        {"name": "tajuk", "selector": "h1.produk-title", "type": "text"},
        {"name": "judul", "selector": "div.align-items-start > div:nth-child(1) span", "type": "text"},
        {"name": "tipe_dokumen", "selector": "div.align-items-start > div:nth-child(2) span", "type": "text"},
        {"name": "nomor", "selector": "div.align-items-start > div:nth-child(3) span", "type": "text"},
        {"name": "tahun", "selector": "div.align-items-start > div:nth-child(4) span", "type": "text"},
        {"name": "tanggal_penetapan", "selector": "div.align-items-start > div:nth-child(5) span", "type": "text"},
        {"name": "tanggal_pengundangan", "selector": "div.align-items-start > div:nth-child(6) span", "type": "text"},
        {"name": "tempat_penetapan", "selector": "div.align-items-start > div:nth-child(7) span", "type": "text"},
        {"name": "sumber", "selector": "div.align-items-start > div:nth-child(8) span", "type": "text"},
        {"name": "bahasa", "selector": "div.align-items-start > div:nth-child(9) span", "type": "text"},
        {"name": "lokasi", "selector": "div.align-items-start > div:nth-child(10) span", "type": "text"},
        {"name": "bidang_hukum", "selector": "div.align-items-start > div:nth-child(11) span", "type": "text"},
        {"name": "jenis_dokumen", "selector": "div.produk-sidebar > div:nth-child(1) ul li:nth-child(2)", "type": "text"},
        {"name": "singkatan_jenis", "selector": "div.produk-sidebar > div:nth-child(2) ul li:nth-child(2)", "type": "text"},
        {"name": "status", "selector": "div.produk-sidebar > div:nth-child(3) ul li:nth-child(2)", "type": "text"},
        {"name": "keterangan status", "selector": "div.produk-sidebar > div:nth-child(4) ul li:nth-child(2)", "type": "text"},
        {"name": "lampiran_dokumen", "selector": "div.produk-sidebar > div:nth-child(5) ul li:nth-child(2) a:nth-child(2)", "type": "attribute", "attribute": "href"},
        {"name": "teu_badan", "selector": "div.main-content > div:nth-child(4)", "type": "text"},
        {"name": "subjek", "selector": "div.subjek-tags", "type": "text"},
        {"name": "peraturan_terkait", "selector": "div.main-content > div:nth-child(6) > div div", "type": "text"},
        {"name": "dokumen_terkait", "selector": "div.main-content > div:nth-child(7) > div div", "type": "text"},
        {"name": "peraturan_pelaksanaan", "selector": "div.main-content > div:nth-child(8) > div div", "type": "text"},
        {"name": "hasil_uji_materi", "selector": "div.main-content > div:nth-child(9) > div div", "type": "text"},
        {"name": "isi_dokumen", "selector": "div#produk-content", "type": "text"},
        {"name": "produk_hukum_terkait", "selector": "div.main-content > div:nth-child(12) > div div", "type": "text"}
    ]
}

async def scrape_details():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        items = json.load(f)

    print(f"Loaded {len(items)} items from {INPUT_FILE}.")

    # Load existing results or initialize with input items
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                results = json.load(f)
                print(f"Loaded existing {len(results)} records from {OUTPUT_FILE}.")
        except Exception as e:
            print(f"Could not load existing {OUTPUT_FILE}: {e}")
            results = items.copy()
    else:
        print(f"Initializing {OUTPUT_FILE} with {len(items)} records from {INPUT_FILE}.")
        results = items.copy()
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)

    # Use item_id or url to map results for easy updating
    results_map = {item.get('url'): i for i, item in enumerate(results)}
    
    # Identify which items still need detail scraping (e.g., those without 'tajuk')
    items_to_scrape = [item for item in results if 'tajuk' not in item]
    
    print(f"Starting to scrape {len(items_to_scrape)} items that lack detailed data...")

    if not items_to_scrape:
        print("All items already have detailed data. Exiting.")
        return

    strategy = JsonCssExtractionStrategy(schema, verbose=False)
    browser_config = BrowserConfig(headless=True)
    
    progress_count = 0
    success_count = 0
    failed_count = 0
    total_to_scrape = len(items_to_scrape)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        
        async def fetch_item(item):
            nonlocal progress_count, success_count, failed_count
            url = item.get('url')
            if not url:
                progress_count += 1
                return None

            run_config = CrawlerRunConfig(
                extraction_strategy=strategy,
                cache_mode=CacheMode.BYPASS,
                page_timeout=30000
            )
            
            try:
                result = await crawler.arun(url=url, config=run_config)
                
                success = False
                extracted_data = None
                
                if result.success and result.extracted_content:
                    try:
                        data = json.loads(result.extracted_content)
                        if data and isinstance(data, list):
                            extracted_data = data[0]
                            success = True
                    except json.JSONDecodeError:
                        pass
                
                progress_count += 1
                if success:
                    success_count += 1
                    print(f"[{progress_count}/{total_to_scrape}] SUCCESS - Extracted {url}")
                    return {**item, **extracted_data}
                else:
                    failed_count += 1
                    status = getattr(result, 'status_code', 'Unknown')
                    print(f"[{progress_count}/{total_to_scrape}] FAILED - {url} (Status: {status})")
                    return None
            except Exception as e:
                progress_count += 1
                failed_count += 1
                print(f"[{progress_count}/{total_to_scrape}] ERROR - {url}: {e}")
                return None

        # Process in batches
        batch_size = 10
        for i in range(0, len(items_to_scrape), batch_size):
            batch = items_to_scrape[i:i + batch_size]
            print(f"\n--- Processing Batch {i//batch_size + 1} ({len(batch)} items) ---")
            
            tasks = [fetch_item(item) for item in batch]
            batch_results = await asyncio.gather(*tasks)
            
            new_results_count = 0
            for res in batch_results:
                if res:
                    url = res.get('url')
                    if url in results_map:
                        idx = results_map[url]
                        results[idx] = res
                        new_results_count += 1
            
            # Save progress
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=4)
            
            print(f"Batch complete. Updated {new_results_count} records. Progress saved to {OUTPUT_FILE}.")

    print(f"\nScraping complete!")
    print(f"Total Success: {success_count} | Total Failed: {failed_count}")
    print(f"Final records in {OUTPUT_FILE}: {len(results)}")

if __name__ == "__main__":
    asyncio.run(scrape_details())
