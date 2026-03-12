import asyncio
import json
import logging
import sys
import os
from pathlib import Path
from urllib.parse import urljoin

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
DB_ROOT = Path(__file__).parent.parent / 'db'
INPUT_FILE = DB_ROOT / 'peraturan_go_id_perda_links.json'
OUTPUT_FILE = DB_ROOT / 'peraturan_go_id_perda.json'

schema = {
    "name": "Peraturan",
    "baseSelector": "section#description",
    "fields": [
        {"name": "judul", "selector": "div.detail_title_1", "type": "text"},
        {"name": "jenis", "selector": "tbody tr:nth-child(1) td", "type": "text"},
        {"name": "pemrakarsa", "selector": "tbody tr:nth-child(2) td", "type": "text"},
        {"name": "nomor", "selector": "tbody tr:nth-child(3) td", "type": "text"},
        {"name": "tahun", "selector": "tbody tr:nth-child(4) td", "type": "text"},
        {"name": "tentang", "selector": "tbody tr:nth-child(5) td", "type": "text"},
        {"name": "tempat_penetapan", "selector": "tbody tr:nth-child(6) td", "type": "text"},
        {"name": "ditetapkan_tanggal", "selector": "tbody tr:nth-child(7) td", "type": "text"},
        {"name": "pejabat yang menetapkan", "selector": "tbody tr:nth-child(8) td", "type": "text"},
        {"name": "status", "selector": "tbody tr:nth-child(9) td", "type": "text"},
        {
            "name": "dokumen_peraturan",
            "selector": "tbody tr:nth-child(10) td a",
            "type": "attribute",
            "attribute": "href"
        }
    ]
}

async def run_perda_detail_scraper(status_placeholder=None, progress_bar=None):
    if not INPUT_FILE.exists():
        msg = f"Input file {INPUT_FILE} does not exist. Run links scraper first."
        logger.error(msg)
        return False, msg, None

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        links_data = json.load(f)

    # Filter out empty or invalid links
    links_data = [item for item in links_data if item.get('link')]
    
    # Load existing details if any
    existing_details = []
    scraped_urls = set()
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing_details = json.load(f)
                scraped_urls = {item.get('url') for item in existing_details if item.get('url')}
            logger.info(f"Loaded {len(existing_details)} existing details.")
        except Exception as e:
            logger.error(f"Could not load existing output: {e}")

    browser_config = BrowserConfig(headless=True, verbose=False)
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        total_items = len(links_data)
        
        # Filter urls to scrape
        urls_to_scrape = []
        for item in links_data:
            link = item.get("link")
            if not link.startswith("http"):
                link = urljoin("https://peraturan.go.id", link)
            if link not in scraped_urls:
                urls_to_scrape.append(link)
                
        batch_size = 10
        total_urls = len(urls_to_scrape)
        
        for i in range(0, total_urls, batch_size):
            batch_urls = urls_to_scrape[i:i + batch_size]
            
            logger.info(f"Scraping batch {i // batch_size + 1}/{total_urls // batch_size + 1} ({len(batch_urls)} urls)")
            if status_placeholder:
                status_placeholder.text(f"Scraping batch {i // batch_size + 1}/{total_urls // batch_size + 1}...")
            
            run_config = CrawlerRunConfig(
                extraction_strategy=JsonCssExtractionStrategy(schema),
                cache_mode=CacheMode.BYPASS,
                wait_for="section#description",
                wait_for_timeout=15000
            )
            
            try:
                results = await crawler.arun_many(batch_urls, config=run_config)
                
                for link, result in zip(batch_urls, results):
                    if not result.success:
                        logger.error(f"Failed to crawl {link}: {result.error_message}")
                        continue
                    
                    try:
                        extracted_data = json.loads(result.extracted_content)
                    except Exception as e:
                        logger.error(f"Failed to parse JSON for {link}: {e}")
                        extracted_data = {}
                    
                    if isinstance(extracted_data, list):
                        if len(extracted_data) > 0:
                            extracted_data = extracted_data[0]
                        else:
                            extracted_data = {}

                    extracted_data["url"] = link
                    existing_details.append(extracted_data)
                    scraped_urls.add(link)
                    
                    logger.info(f"Successfully extracted details from {link}")
                
                # Checkpoint
                with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                    json.dump(existing_details, f, indent=2, ensure_ascii=False)
                logger.info(f"Checkpoint saved: {len(existing_details)} details extracted.")
                
                if progress_bar:
                    progress_bar.progress(min((i + batch_size) / total_urls, 1.0))
                    
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error scraping batch {i}: {e}")
                
    # Final save
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(existing_details, f, indent=2, ensure_ascii=False)
    logger.info(f"Scraping details complete. Total details collected: {len(existing_details)}")
    logger.info(f"Data saved to {OUTPUT_FILE}")
    
    return True, f"Extracted {len(existing_details)} details.", existing_details

async def main():
    await run_perda_detail_scraper()

if __name__ == "__main__":
    asyncio.run(main())
