import asyncio
import json
import os
import sys
import logging
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

from config_general import GENERAL_SITES_CONFIG, SCRAPER_CONFIG, OUTPUT_LINKS_FILE, OUTPUT_CONTENT_FILE

# Ensure UTF-8 output for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GeneralContentScraper:
    def __init__(self, crawler):
        self.crawler = crawler
        self.semaphore = asyncio.Semaphore(SCRAPER_CONFIG["concurrency_limit"])

    def clean_date(self, date_str: str, source_name: str) -> str:
        """Cleans source-specific date formatting."""
        date_str = date_str.strip()
        if source_name == 'BGN':
            if '•' in date_str:
                date_str = date_str.split('•')[-1]
            date_str = date_str.replace('Siaran Pers', '').strip()
        elif source_name == 'ESDM':
            if ' - ' in date_str:
                date_str = date_str.split(' - ')[0].strip()
        return date_str

    async def scrape_article(self, item: dict, site_config: dict, index: int, total: int):
        async with self.semaphore:
            source_name = item['source']
            url = item['link']
            detail_config = site_config["detail"]
            
            logger.info(f"[{source_name}] [{index+1}/{total}] Crawling: {url}")
            
            run_config = CrawlerRunConfig(
                extraction_strategy=JsonCssExtractionStrategy(detail_config["schema"]),
                cache_mode=CacheMode.BYPASS,
                wait_for=f"css:{detail_config['wait_for']}",
                wait_for_timeout=SCRAPER_CONFIG["wait_timeout"]
            )
            
            try:
                result = await self.crawler.arun(url=url, config=run_config)
                
                if not result.success:
                    logger.error(f"[{source_name}] Failed: {url} - {result.error_message}")
                    return None
                
                data = json.loads(result.extracted_content)
                detail = data[0] if isinstance(data, list) and data else (data if data else {})
                
                return {
                    "title": item['title'],
                    "link": item['link'],
                    "source": source_name,
                    "date": self.clean_date(str(detail.get('date', '')), source_name),
                    "text": str(detail.get('text', '')).strip()
                }
            except Exception as e:
                logger.error(f"[{source_name}] Error processing {url}: {e}")
                return None
            finally:
                await asyncio.sleep(SCRAPER_CONFIG["polite_delay"])

async def main():
    if not os.path.exists(OUTPUT_LINKS_FILE):
        logger.error(f"Error: {OUTPUT_LINKS_FILE} not found.")
        return

    with open(OUTPUT_LINKS_FILE, 'r', encoding='utf-8') as f:
        news_items = json.load(f)
    
    # Load existing content for updatable crawling
    existing_content = []
    scraped_links = set()
    if os.path.exists(OUTPUT_CONTENT_FILE):
        logger.info(f"Loading existing content from {OUTPUT_CONTENT_FILE}...")
        with open(OUTPUT_CONTENT_FILE, 'r', encoding='utf-8') as f:
            existing_content = json.load(f)
            scraped_links = {item['link'] for item in existing_content}

    # Filter to only new items
    items_to_crawl = [item for item in news_items if item['link'] not in scraped_links]
    
    logger.info(f"Total links: {len(news_items)} | Already scraped: {len(scraped_links)} | To crawl: {len(items_to_crawl)}")
    
    if not items_to_crawl:
        logger.info("All articles are already scraped.")
        return

    browser_config = BrowserConfig(headless=True)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        scraper = GeneralContentScraper(crawler)
        
        tasks = []
        for i, item in enumerate(items_to_crawl):
            source = item.get('source')
            if source in GENERAL_SITES_CONFIG:
                tasks.append(scraper.scrape_article(item, GENERAL_SITES_CONFIG[source], i, len(items_to_crawl)))
            else:
                logger.warning(f"No config for source: {source}")
        
        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r is not None]
    
    # Merge: New results at the top
    final_content = valid_results + existing_content
    
    with open(OUTPUT_CONTENT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_content, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\nScraping complete! Added {len(valid_results)} new articles.")
    logger.info(f"Results saved to: {OUTPUT_CONTENT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
