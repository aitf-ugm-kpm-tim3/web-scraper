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

class KomdigiContentScraper:
    def __init__(self, crawler):
        self.crawler = crawler
        self.base_url = "https://www.komdigi.go.id"
        self.schema = {
            "name": "Siaran Pers Detail",
            "baseSelector": "body",
            "fields": [
                {
                    "name": "date",
                    "selector": "section.flex.mt-5 div.flex-wrap span.text-body-l:not([style])",
                    "type": "text"
                },
                {
                    "name": "text",
                    "selector": "section#section_text_body",
                    "type": "text"
                }
            ]
        }

    async def scrape_content(self, max_concurrent=3):
        from config_general import DB_ROOT
        links_file = str(DB_ROOT / 'siaran_pers_komdigi_links.json')
        output_file = str(DB_ROOT / 'siaran_pers_komdigi_all.json')
        
        if not os.path.exists(links_file):
            logger.error(f"Error: {links_file} not found.")
            return []

        with open(links_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        all_news_items = []
        for page in data:
            items = page.get('news_items', [])
            all_news_items.extend(items)
        
        existing_content = []
        scraped_links = set()
        if os.path.exists(output_file):
            logger.info(f"Loading existing content from {output_file}...")
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_content = json.load(f)
                scraped_links = {item['link'] for item in existing_content}

        items_to_crawl = [item for item in all_news_items if item['link'] not in scraped_links]
        logger.info(f"Total links: {len(all_news_items)} | Already scraped: {len(scraped_links)} | To crawl: {len(items_to_crawl)}")
        
        if not items_to_crawl:
            logger.info("All articles are already scraped.")
            return existing_content

        extraction_strategy = JsonCssExtractionStrategy(self.schema)
        run_config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            cache_mode=CacheMode.BYPASS,
            wait_for="css:section#section_text_body"
        )
        
        new_results = []
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def crawl_item(item, index):
            async with semaphore:
                link = item['link']
                if not link.startswith('/'):
                    link = '/' + link
                
                url = self.base_url + link
                logger.info(f"[{index+1}/{len(items_to_crawl)}] Crawling: {url}")
                
                try:
                    result = await self.crawler.arun(url=url, config=run_config)
                    
                    if result.success:
                        extracted_data = json.loads(result.extracted_content)
                        detail = extracted_data[0] if isinstance(extracted_data, list) and extracted_data else (extracted_data if extracted_data else {})
                        
                        new_results.append({
                            "title": item['title'],
                            "link": item['link'],
                            "source": item.get('source', 'KOMDIGI'),
                            "date": str(detail.get('date', '')).strip(),
                            "text": str(detail.get('text', '')).strip()
                        })
                    else:
                        logger.error(f"Failed to crawl {url}: {result.error_message}")
                except Exception as e:
                    logger.error(f"Error processing {url}: {e}")
                
                await asyncio.sleep(0.5)

        tasks = [crawl_item(item, i) for i, item in enumerate(items_to_crawl)]
        await asyncio.gather(*tasks)
        
        final_results = new_results + existing_content
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\nScraping complete! Added {len(new_results)} new articles.")
        return final_results

async def main():
    browser_config = BrowserConfig(headless=True)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        scraper = KomdigiContentScraper(crawler)
        await scraper.scrape_content()

if __name__ == "__main__":
    asyncio.run(main())
