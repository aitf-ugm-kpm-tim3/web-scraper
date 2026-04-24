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

BASE_URL = "https://jdih.komdigi.go.id/produk_hukum/abstrak/"
OUTPUT_FILE = "jdih_komdigi_abstrak.json"

schema = {
    "name": "JDIH Komdigi Abstrak",
    "baseSelector": "section.katalog",
    "fields": [
        {"name": "url", "selector": "div.col-lg-8 div > a:nth-child(1)", "type": "attribute", "attribute": "href"},
        {"name": "abstrak_subjek", "selector": "div.card-body > div:nth-child(1)", "type": "text"},
        {"name": "abstrak_tahun", "selector": "div.card-body > div:nth-child(2)", "type": "text"},
        {"name": "abstrak_nomor", "selector": "div.card-body > div:nth-child(3)", "type": "text"},
        {"name": "abstrak_judul", "selector": "div.card-body > div:nth-child(4)", "type": "text"},
        {"name": "abstrak_menimbang", "selector": "table tbody tr:nth-child(1) > td:nth-child(3)", "type": "text"},
        {"name": "abstrak_dasar_hukum", "selector": "table tbody tr:nth-child(2) > td:nth-child(3)", "type": "text"},
        {"name": "abstrak_ringkasan", "selector": "table tbody tr:nth-child(3) > td:nth-child(3)", "type": "text"},
        {"name": "abstrak_catatan", "selector": "table tbody tr:nth-child(4) > td:nth-child(3)", "type": "text"}
    ]
}

async def scrape_items():
    strategy = JsonCssExtractionStrategy(schema, verbose=False)
    results = []
    existing_ids = set()
    
    # Check if file exists and load existing results to prevent re-scraping
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                results = json.load(f)
                existing_ids = {item.get('item_id') for item in results if 'item_id' in item}
        except Exception as e:
            print(f"Could not load existing {OUTPUT_FILE}: {e}")

    # Range of item IDs to scrape
    all_item_ids = list(range(1, 539))
    item_ids_to_scrape = [i for i in all_item_ids if i not in existing_ids]
    
    print(f"Found {len(existing_ids)} existing items in {OUTPUT_FILE}.")
    print(f"Starting to scrape {len(item_ids_to_scrape)} items with crawl4ai...")
    
    if not item_ids_to_scrape:
        print("All items already scraped. Exiting.")
        return

    progress_count = 0
    success_count = 0
    failed_count = 0
    total_to_scrape = len(item_ids_to_scrape)
    
    browser_config = BrowserConfig(headless=True)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        
        async def fetch_item(item_id):
            nonlocal progress_count, success_count, failed_count
            url = f"{BASE_URL}{item_id}"
            
            run_config = CrawlerRunConfig(
                extraction_strategy=strategy,
                cache_mode=CacheMode.BYPASS,
                page_timeout=30000  # 30 seconds timeout
            )
            result = await crawler.arun(url=url, config=run_config)
                
            success = False
            item_data = None
            
            if result.success and result.extracted_content:
                try:
                    data = json.loads(result.extracted_content)
                    if data and isinstance(data, list):
                        item_data = data[0]
                        item_data['item_id'] = item_id
                        success = True
                except json.JSONDecodeError:
                    pass
            
            progress_count += 1
            
            if success:
                success_count += 1
                print(f"[{progress_count}/{total_to_scrape}] SUCCESS - Extracted item {item_id}")
                return item_data
            else:
                failed_count += 1
                status = getattr(result, 'status_code', 'Unknown')
                print(f"[{progress_count}/{total_to_scrape}] FAILED - No content for item {item_id} (Status: {status})")
                return None

        # Process in batches of 10
        batch_size = 10
        for i in range(0, len(item_ids_to_scrape), batch_size):
            batch = item_ids_to_scrape[i:i + batch_size]
            print(f"\n--- Processing Batch {i//batch_size + 1} ({len(batch)} items) ---")
            
            # Gather tasks for the current batch
            tasks = [fetch_item(item_id) for item_id in batch]
            batch_results = await asyncio.gather(*tasks)
            
            # Add successful results and save progress
            new_results_count = 0
            for res in batch_results:
                if res:
                    results.append(res)
                    new_results_count += 1
            
            # Sort results by item_id
            results.sort(key=lambda x: x.get('item_id', 0))
            
            # Save progress to file after each batch
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=4)
            
            print(f"Batch {i//batch_size + 1} complete. Added {new_results_count} items. Progress saved to {OUTPUT_FILE}.")

    print(f"\nScraping complete!")
    print(f"Total Scraped This Run: {total_to_scrape} | Success: {success_count} | Failed: {failed_count}")
    print(f"Total Records Saved to {OUTPUT_FILE}: {len(results)}")

if __name__ == "__main__":
    asyncio.run(scrape_items())
