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

class KomdigiLinksScraper:
    def __init__(self, crawler):
        self.crawler = crawler
        self.schema = {
            "name": "News Links",
            "baseSelector": "body",
            "fields": [
                {
                    "name": "page", 
                    "selector": "button.relative.px-3.py-2.text-body-l.font-bold",
                    "type": "text"
                },
                {
                    "name": "news_items",
                    "selector": "div.flex.flex-col.gap-1",
                    "type": "list",
                    "fields": [
                        {"name": "title", "selector": "a.text-base.line-clamp-2", "type": "text"},
                        {"name": "link", "selector": "a.text-base.line-clamp-2", "type": "attribute", "attribute": "href"},
                        {"name": "source", "type": "text", "default": "KOMDIGI"}
                    ]
                }
            ]
        }

    async def scrape_links(self, max_pages=385):
        from config_general import DB_ROOT
        output_file = str(DB_ROOT / 'siaran_pers_komdigi_links.json')
        session_id = "komdigi_session"
        
        existing_all_links = []
        existing_links_set = set()
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_all_links = json.load(f)
                for page_data in existing_all_links:
                    for item in page_data.get('news_items', []):
                        existing_links_set.add(item.get('link'))

        new_links = []
        url = "https://www.komdigi.go.id/berita/siaran-pers"
        page_num = 1
        found_existing = False
        
        while page_num <= max_pages and not found_existing:
            logger.info(f"--- Updatable Scraping Page {page_num} ---")
            
            if page_num == 1:
                config = CrawlerRunConfig(
                    extraction_strategy=JsonCssExtractionStrategy(self.schema),
                    cache_mode=CacheMode.BYPASS,
                    session_id=session_id,
                    wait_for="css:a.text-base.line-clamp-2"
                )
            else:
                js_click_next = """
                const svgNext = document.querySelector('svg.chevron-right_icon');
                if (svgNext) {
                    const button = svgNext.closest('button');
                    if (button && !svgNext.classList.contains('text-netral-gray-03')) {
                        button.click();
                    }
                }
                """
                config = CrawlerRunConfig(
                    extraction_strategy=JsonCssExtractionStrategy(self.schema),
                    cache_mode=CacheMode.BYPASS,
                    session_id=session_id,
                    js_code=js_click_next,
                    js_only=True, 
                    wait_for="css:a.text-base.line-clamp-2"
                )

            result = await self.crawler.arun(url=url, config=config)

            if not result.success:
                logger.error(f"Failed to crawl page {page_num}: {result.error_message}")
                break

            try:
                page_data = json.loads(result.extracted_content)
                if isinstance(page_data, list) and page_data:
                    page_data = page_data[0]
                
                if not page_data or not page_data.get('news_items'):
                    logger.warning(f"No links found on page {page_num}. Ending.")
                    break
                
                current_page_items = page_data.get('news_items', [])
                fresh_items = []
                for item in current_page_items:
                    if item.get('link') in existing_links_set:
                        logger.info(f"Found existing link: {item.get('link')}. Stopping.")
                        found_existing = True
                        break
                    fresh_items.append(item)
                
                if fresh_items:
                    page_data['news_items'] = fresh_items
                    new_links.append(page_data)
                    logger.info(f"Found {len(fresh_items)} new links on page {page_num}.")
                
                if found_existing:
                    break

            except Exception as e:
                logger.error(f"Error parsing content on page {page_num}: {e}")
                break

            import re
            if re.search(r'chevron-right_icon[^>]*text-netral-gray-03', result.html) or \
               re.search(r'text-netral-gray-03[^>]*chevron-right_icon', result.html):
                logger.info("Reached the last page.")
                break
            
            try:
                page_num = int(current_page_items[-1]['page']) + 1 if current_page_items else page_num + 1
            except:
                page_num += 1
                
            await asyncio.sleep(5)

        final_all_links = new_links + existing_all_links
        if new_links:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(final_all_links, f, indent=2, ensure_ascii=False)
            logger.info(f"Results saved to {output_file}")
        
        return final_all_links

async def main():
    browser_config = BrowserConfig(headless=True, verbose=True)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        scraper = KomdigiLinksScraper(crawler)
        await scraper.scrape_links()

if __name__ == "__main__":
    asyncio.run(main())
