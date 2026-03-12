import asyncio
import json
import logging
import sys
from urllib.parse import urlparse
from pathlib import Path

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Output file
DB_ROOT = Path(__file__).parent.parent / 'db'
DB_ROOT.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = DB_ROOT / 'peraturan_go_id_perda_links.json'

schema = {
    "name": "perda_links",
    "baseSelector": "body",
    "fields": [
        {
            "name": "active_page",
            "selector": "li.active",
            "type": "text"
        },
        {
            "name": "items",
            "selector": "div.strip.grid",
            "type": "list",
            "fields": [
                {
                    "name": "link",
                    "selector": "p a[title='lihat detail']",
                    "type": "attribute",
                    "attribute": "href"
                },
                {
                    "name": "title",
                    "selector": "p a[title='lihat detail']",
                    "type": "text"
                }
            ]
        }
    ]
}

async def run_scraper(start_page=1, end_page=5, status_placeholder=None, progress_bar=None):
    base_url = "https://peraturan.go.id/perda?page={}"
    all_links = []
    
    existing_links_set = set()
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                if isinstance(existing_data, list):
                    all_links = existing_data
                    existing_links_set = {item.get('link') for item in all_links if item.get('link')}
        except Exception as e:
            logger.error(f"Failed to load existing links: {e}")
    browser_config = BrowserConfig(headless=True, verbose=False)
    
    total_pages = max(1, end_page - start_page + 1)
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        batch_size = 10
        total_pages = max(1, end_page - start_page + 1)
        pages_to_scrape = list(range(start_page, end_page + 1))
        
        for i in range(0, len(pages_to_scrape), batch_size):
            batch_pages = pages_to_scrape[i:i + batch_size]
            batch_urls = [base_url.format(p) for p in batch_pages]
            
            logger.info(f"Scraping batch {i // batch_size + 1}/{len(pages_to_scrape) // batch_size + 1} ({len(batch_urls)} urls)")
            if status_placeholder:
                status_placeholder.text(f"Scraping batch pages {batch_pages[0]} to {batch_pages[-1]}...")
                
            run_config = CrawlerRunConfig(
                extraction_strategy=JsonCssExtractionStrategy(schema),
                cache_mode=CacheMode.BYPASS,
                wait_for="div.strip.grid",
                wait_for_timeout=15000
            )
            
            try:
                results = await crawler.arun_many(batch_urls, config=run_config)
                
                for page, link_url, result in zip(batch_pages, batch_urls, results):
                    if not result.success:
                        logger.error(f"Failed to crawl page {page}: {result.error_message}")
                        continue
                    
                    try:
                        data = json.loads(result.extracted_content)
                    except Exception as e:
                        logger.error(f"Failed to parse JSON for page {page}: {e}")
                        data = {}
                    
                    if isinstance(data, list) and len(data) > 0:
                        data = data[0]
                    elif isinstance(data, list):
                        data = {}

                    active_page = data.get("active_page") or str(page)
                    
                    items = data.get("items", [])
                    links_found = 0
                    
                    for item in items:
                        href = item.get("link", "")
                        if href and '/id/' in href:
                            parsed_uri = urlparse(href)
                            path = parsed_uri.path
                            
                            if href not in existing_links_set:
                                link_data = {
                                    "title": item.get("title", ""),
                                    "link": href,
                                    "scraped_at_page": int(active_page) if str(active_page).isdigit() else page
                                }
                                
                                all_links.append(link_data)
                                existing_links_set.add(href)
                                links_found += 1
                    
                    logger.info(f"Found {links_found} new links on page {page}")
                
                # Checkpoint
                with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                    json.dump(all_links, f, indent=2, ensure_ascii=False)
                logger.info(f"Checkpoint saved: {len(all_links)} total links.")
                
                if progress_bar:
                    progress_bar.progress(min((i + len(batch_pages)) / total_pages, 1.0))
                    
                await asyncio.sleep(1) # Polite delay
                
            except Exception as e:
                logger.error(f"Error scraping batch {i}: {e}")
                
    # Final save
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_links, f, indent=2, ensure_ascii=False)
    logger.info(f"Scraping complete. Total links collected: {len(all_links)}")
    logger.info(f"Data saved to {OUTPUT_FILE}")
    
    return all_links

async def main():
    await run_scraper(1, 5)

if __name__ == "__main__":
    asyncio.run(main())
