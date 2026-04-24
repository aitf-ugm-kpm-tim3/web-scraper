import asyncio
import json
import os
import sys
import logging
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

from config_general import SCRAPER_CONFIG, OUTPUT_LINKS_FILE, OUTPUT_CONTENT_FILE

# Ensure UTF-8 output for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DPRScraper:
    def __init__(self, crawler, total_to_scrape: int):
        self.crawler = crawler
        self.semaphore = asyncio.Semaphore(SCRAPER_CONFIG["concurrency_limit"])
        self.source_name = "MK"
        self.total_to_scrape = total_to_scrape
        self.success_count = 0
        self.failed_count = 0
        self.processed_count = 0
        self.schema = {
            "name": "MK_DETAIL",
            "baseSelector": "div.css-egucdg",
            "fields": [
                {"name": "title", "selector": "h1.news-title", "type": "text"},
                {"name": "date", "selector": "p.news-date", "type": "text"},
                {"name": "text", "selector": "div.news-content", "type": "text"}
            ]
        }

    async def scrape_id(self, item_id: int):
        async with self.semaphore:
            url = f"https://www.mkri.id/berita/{item_id}"
            
            run_config = CrawlerRunConfig(
                extraction_strategy=JsonCssExtractionStrategy(self.schema),
                cache_mode=CacheMode.BYPASS,
                wait_for="css:div.css-egucdg",
                wait_for_timeout=SCRAPER_CONFIG["wait_timeout"]
            )
            
            try:
                result = await self.crawler.arun(url=url, config=run_config)
                
                self.processed_count += 1
                progress = f"{self.processed_count}/{self.total_to_scrape}"
                
                if not result.success:
                    self.failed_count += 1
                    if "404" in result.error_message or "status: 404" in result.error_message.lower():
                        logger.warning(f"[{self.source_name}] ID {item_id} Not Found (404) | Progress: {progress} | Success: {self.success_count} | Failed: {self.failed_count}")
                        return {"id": item_id, "status": "not_found"}
                    logger.error(f"[{self.source_name}] ID {item_id} Failed: {result.url} | Progress: {progress} | Success: {self.success_count} | Failed: {self.failed_count}")
                    return None
                
                data = json.loads(result.extracted_content)
                detail = data[0] if isinstance(data, list) and data else (data if data else {})
                
                title = str(detail.get('title', '')).strip()
                text = str(detail.get('text', '')).strip()
                date = str(detail.get('date', '')).strip()
                
                if not title or not text:
                    self.failed_count += 1
                    logger.warning(f"[{self.source_name}] ID {item_id} Empty or invalid content | Progress: {progress} | Success: {self.success_count} | Failed: {self.failed_count}")
                    return {"id": item_id, "status": "empty"}

                self.success_count += 1
                record = {
                    "title": title,
                    "link": url,
                    "source": self.source_name,
                    "date": date,
                    "text": text
                }
                logger.info(f"[{self.source_name}] ID {item_id} Success: {record['date']}... | Progress: {progress} | Success: {self.success_count} | Failed: {self.failed_count}")
                return record
            except Exception as e:
                if 'progress' not in locals():
                    self.processed_count += 1
                    progress = f"{self.processed_count}/{self.total_to_scrape}"
                self.failed_count += 1
                logger.error(f"[{self.source_name}] ID {item_id} Error parsing json or crawl result: {e} | Progress: {progress} | Success: {self.success_count} | Failed: {self.failed_count}")
                return None
            finally:
                await asyncio.sleep(SCRAPER_CONFIG["polite_delay"])

async def main():
    # Ensure directories exist
    os.makedirs(os.path.dirname(OUTPUT_CONTENT_FILE), exist_ok=True)
    
    # Load existing content
    existing_content = []
    scraped_links = set()
    if os.path.exists(OUTPUT_CONTENT_FILE):
        try:
            with open(OUTPUT_CONTENT_FILE, 'r', encoding='utf-8') as f:
                content_data = json.load(f)
                if isinstance(content_data, list):
                    existing_content = content_data
                    scraped_links = {item['link'] for item in existing_content if 'link' in item}
            logger.info(f"Loaded {len(existing_content)} existing articles.")
        except Exception as e:
            logger.error(f"Could not load existing content: {e}")

    # Load existing links
    existing_links = []
    if os.path.exists(OUTPUT_LINKS_FILE):
        try:
            with open(OUTPUT_LINKS_FILE, 'r', encoding='utf-8') as f:
                links_data = json.load(f)
                if isinstance(links_data, list):
                    existing_links = links_data
        except Exception as e:
            logger.error(f"Could not load existing links: {e}")

    # Range: 1 to 64158
    # You might want to allow starting from a custom ID via command line
    start_id = 1
    end_id = 64158
    
    if len(sys.argv) > 1:
        try:
            start_id = int(sys.argv[1])
            if len(sys.argv) > 2:
                end_id = int(sys.argv[2])
        except ValueError:
            logger.error("Invalid start/end ID provided. Using defaults.")

    # Filter IDs that haven't been scraped yet
    ids_to_scrape = []
    for item_id in range(start_id, end_id + 1):
        url = f"https://www.mkri.id/berita/{item_id}"
        if url not in scraped_links:
            ids_to_scrape.append(item_id)

    total_to_scrape = len(ids_to_scrape)
    logger.info(f"Scraping IDs from {start_id} to {end_id}. Total to fetch: {total_to_scrape}")

    if total_to_scrape == 0:
        logger.info("No new IDs to scrape in this range.")
        return

    browser_config = BrowserConfig(headless=True)
    async with AsyncWebCrawler(config=browser_config) as crawler:
        scraper = DPRScraper(crawler, total_to_scrape)
        
        # Batch size for saving progress
        batch_size = 50 
        for i in range(0, total_to_scrape, batch_size):
            batch_ids = ids_to_scrape[i:i + batch_size]
            current_batch_num = i // batch_size + 1
            total_batches = (total_to_scrape + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {current_batch_num}/{total_batches} ({len(batch_ids)} IDs)")
            
            tasks = [scraper.scrape_id(item_id) for item_id in batch_ids]
            results = await asyncio.gather(*tasks)
            
            valid_results = [r for r in results if r and "status" not in r]
            
            if valid_results:
                # Merge new results at the top (consistent with siaran_pers_general.py)
                existing_content = valid_results + existing_content
                with open(OUTPUT_CONTENT_FILE, 'w', encoding='utf-8') as f:
                    json.dump(existing_content, f, indent=2, ensure_ascii=False)
                
                # Prepare links for the link file
                new_links = [{"source": r['source'], "title": r['title'], "link": r['link']} for r in valid_results]
                existing_links = new_links + existing_links
                with open(OUTPUT_LINKS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(existing_links, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Batch {current_batch_num} saved. Added {len(valid_results)} articles.")
            
            # Short break between batches to avoid overloading the server or getting blocked
            if i + batch_size < total_to_scrape:
                await asyncio.sleep(3)

    logger.info("DPR Scraping process completed.")

if __name__ == "__main__":
    asyncio.run(main())
