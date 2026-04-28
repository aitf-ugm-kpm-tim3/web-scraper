import asyncio
import json
import os
import sys
import logging
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure UTF-8 output for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

class JDIHKomdigiLinksScraper:
    def __init__(self):
        # We'll save it in the current directory first, or use DB_ROOT if we can import it
        try:
            from config_general import DB_ROOT
            self.output_file = str(DB_ROOT / 'jdih_komdigi_links.json')
        except ImportError:
            self.output_file = 'jdih_komdigi_links.json'
            
        self.schema = {
            "name": "JDIH Komdigi Links",
            "baseSelector": "body",
            "fields": [
                {
                    "name": "items",
                    "selector": "div.card.card-produk-hukum-list",
                    "type": "list",
                    "fields": [
                        {"name": "judul", "selector": "h5", "type": "text"},
                        {"name": "url", "selector": "a", "type": "attribute", "attribute": "href"},
                    ],
                },
            ],
        }

    async def scrape(self, start_page=1, end_page=78):
        # Load existing progress
        all_results = []
        scraped_pages = set()
        
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    all_results = json.load(f)
                    # For links, we might just have a list of objects. 
                    # If we want to resume by page, we should store page info.
                    for item in all_results:
                        if 'page' in item:
                            scraped_pages.add(item['page'])
                logger.info(f"Loaded {len(all_results)} existing records from {self.output_file}.")
            except Exception as e:
                logger.error(f"Error loading {self.output_file}: {e}")

        browser_config = BrowserConfig(headless=True)
        strategy = JsonCssExtractionStrategy(self.schema)
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            for page in range(start_page, end_page + 1):
                if page in scraped_pages:
                    logger.info(f"Page {page} already scraped. Skipping.")
                    continue

                url = f"https://jdih.komdigi.go.id/produk_hukum/pencarian?kategori=all&tahun=all&page={page}"
                logger.info(f"Scraping Page {page}: {url}")

                run_config = CrawlerRunConfig(
                    extraction_strategy=strategy,
                    cache_mode=CacheMode.BYPASS,
                    page_timeout=60000,
                    wait_for="css:div.card.card-produk-hukum-list"
                )

                try:
                    result = await crawler.arun(url=url, config=run_config)
                    if result.success and result.extracted_content:
                        data = json.loads(result.extracted_content)
                        if data and isinstance(data, list):
                            page_data = data[0]
                            items = page_data.get('items', [])
                            
                            if items:
                                for item in items:
                                    all_results.append({
                                        "page": page,
                                        **item
                                    })
                                
                                logger.info(f"Successfully scraped {len(items)} items from page {page}.")
                                
                                # Save after each page
                                with open(self.output_file, 'w', encoding='utf-8') as f:
                                    json.dump(all_results, f, ensure_ascii=False, indent=4)
                            else:
                                logger.warning(f"No items found on page {page}.")
                        else:
                            logger.error(f"Invalid data format on page {page}.")
                    else:
                        logger.error(f"Failed to scrape page {page}: {result.error_message}")
                except Exception as e:
                    logger.error(f"Error on page {page}: {e}")
                
                # Politeness delay
                await asyncio.sleep(2)

        logger.info(f"Finished scraping. Total items: {len(all_results)}")

async def main():
    scraper = JDIHKomdigiLinksScraper()
    # User requested page start 1, end 78
    await scraper.scrape(start_page=1, end_page=78)

if __name__ == "__main__":
    asyncio.run(main())
