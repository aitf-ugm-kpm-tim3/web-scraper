import asyncio
import json
import logging
import sys
from urllib.parse import urljoin

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

from config_general import GENERAL_SITES_CONFIG, SCRAPER_CONFIG, OUTPUT_LINKS_FILE

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GeneralLinksScraper:
    def __init__(self, crawler):
        self.crawler = crawler

    async def scrape_site_links(self, site_name: str, site_config: dict):
        links_config = site_config["links"]
        url_template = links_config["url_template"]
        schema = links_config["schema"]
        
        all_links = []
        page_num = 1
        consecutive_empty = 0
        
        logger.info(f"--- Starting link crawl for {site_name} ---")
        
        while page_num <= SCRAPER_CONFIG["max_pages"]:
            url = url_template.format(page=page_num)
            logger.info(f"[{site_name}] Crawling page {page_num}: {url}")
            
            run_config = CrawlerRunConfig(
                extraction_strategy=JsonCssExtractionStrategy(schema),
                cache_mode=CacheMode.BYPASS,
                wait_for=f"css:{links_config['wait_for']}",
                js_code=links_config.get("js_code"),
                wait_for_timeout=SCRAPER_CONFIG["wait_timeout"]
            )
            
            try:
                result = await self.crawler.arun(url=url, config=run_config)
                
                if not result.success:
                    logger.error(f"[{site_name}] Failed to crawl page {page_num}: {result.error_message}")
                    break
                
                data = json.loads(result.extracted_content)
                news_items = self._extract_news_items(data)
                
                if not news_items:
                    logger.warning(f"[{site_name}] No news items found on page {page_num}.")
                    consecutive_empty += 1
                    if consecutive_empty >= SCRAPER_CONFIG["max_consecutive_empty"]:
                        break
                else:
                    consecutive_empty = 0
                    processed_items = self._process_items(news_items, url, site_name, page_num)
                    all_links.extend(processed_items)
                    logger.info(f"[{site_name}] Found {len(processed_items)} items.")
                
                page_num += 1
                await asyncio.sleep(SCRAPER_CONFIG["polite_delay"])
                
            except Exception as e:
                logger.error(f"[{site_name}] Error on page {page_num}: {e}")
                break
                
        return all_links

    def _extract_news_items(self, data):
        """Extracts list of news items from nested extraction results."""
        if isinstance(data, list):
            if data and "title" in data[0]:
                return data
            if data and "news_items" in data[0]:
                return data[0].get("news_items", [])
        elif isinstance(data, dict):
            return data.get("news_items", [])
        return []

    def _process_items(self, items, base_url, source_name, page_num):
        """Cleans and annotates each news item."""
        for item in items:
            link = item.get("link", "")
            if link and not link.startswith("http"):
                item["link"] = urljoin(base_url, link)
            item["source"] = source_name
            item["scraped_at_page"] = page_num
        return items

async def main():
    browser_config = BrowserConfig(headless=True, verbose=False)
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        scraper = GeneralLinksScraper(crawler)
        all_results = []
        
        for site_name, site_config in GENERAL_SITES_CONFIG.items():
            site_links = await scraper.scrape_site_links(site_name, site_config)
            all_results.extend(site_links)
            
        logger.info(f"\nScraping complete! Total links: {len(all_results)}")
        
        with open(OUTPUT_LINKS_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        logger.info(f"Results saved to {OUTPUT_LINKS_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
