import asyncio
import json
import os
import sys
import logging
import aiohttp
from bs4 import BeautifulSoup

from config_general import SCRAPER_CONFIG, OUTPUT_LINKS_FILE, OUTPUT_CONTENT_FILE

# Ensure UTF-8 output for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BKKBNScraper:
    def __init__(self, total_pages: int):
        self.semaphore = asyncio.Semaphore(1) # Reduced to 1 to avoid 504 errors
        self.source_name = "BKKBN"
        self.total_pages = total_pages
        self.success_count = 0
        self.failed_count = 0
        self.processed_count = 0
        
    async def fetch_page(self, session, page: int):
        async with self.semaphore:
            # API endpoint provided by the user
            url = f"https://www-api.kemendukbangga.go.id/api/v1/public/posts?page={page}&category=37c2b970-a525-4884-ace6-8076b478e86b&item_per_page=100&area=40847d55-9175-45c1-92f8-ce10cbb0cb7c&keyword="
            
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                }

                async with session.get(url, headers=headers, timeout=30) as response:
                    self.processed_count += 1
                    progress = f"{self.processed_count}/{self.total_pages}"
                    
                    if response.status != 200:
                        self.failed_count += 1
                        logger.error(f"[{self.source_name}] Page {page} Failed: HTTP {response.status} | Progress: {progress} | Success: {self.success_count} | Failed: {self.failed_count}")
                        return None
                    
                    data = await response.json()
                    
                    # The API seems to return a list in the 'data' field based on standard patterns
                    items = data.get('data', [])
                    if not items:
                        # If 'data' is not a list, maybe it's the top level or another field
                        if isinstance(data, list):
                            items = data
                        elif isinstance(data.get('results'), list):
                            items = data.get('results')
                    
                    if not items:
                        self.failed_count += 1
                        logger.warning(f"[{self.source_name}] Page {page} Failed: No items found | Progress: {progress} | Success: {self.success_count} | Failed: {self.failed_count}")
                        return None
                        
                    results = []
                    
                    for item in items:
                        # Required mapping:
                        # link: "https://www.kemendukbangga.go.id/posts/" + url
                        # title: title
                        # text: content (clean html)
                        # date: created_at
                        # source: BKKBN
                        
                        slug = str(item.get('url', '')).strip()
                        title = str(item.get('title', '')).strip()
                        content_html = str(item.get('content', '')).strip()
                        created_at = str(item.get('created_at', '')).strip()
                        
                        if not slug or not title:
                            continue
                            
                        # Clean HTML tags from content
                        if "<" in content_html and ">" in content_html:
                            text = BeautifulSoup(content_html, "html.parser").get_text(separator=' ', strip=True)
                        else:
                            text = content_html
                            
                        link = f"https://www.kemendukbangga.go.id/posts/{slug}"
                        
                        record = {
                            "title": title,
                            "link": link,
                            "source": self.source_name,
                            "date": created_at,
                            "text": text
                        }
                        results.append(record)
                        
                    self.success_count += len(results)
                    logger.info(f"[{self.source_name}] Page {page} Success: Fetched {len(results)} items | Progress: {progress} | Success: {self.success_count} | Failed: {self.failed_count}")
                    return results

            except Exception as e:
                if 'progress' not in locals():
                    self.processed_count += 1
                    progress = f"{self.processed_count}/{self.total_pages}"
                self.failed_count += 1
                logger.error(f"[{self.source_name}] Page {page} Error: {str(e)} | Progress: {progress} | Success: {self.success_count} | Failed: {self.failed_count}")
                return None
            finally:
                await asyncio.sleep(SCRAPER_CONFIG.get("polite_delay", 0.5))

async def main():
    os.makedirs(os.path.dirname(OUTPUT_CONTENT_FILE), exist_ok=True)
    
    # Load existing content to avoid duplicates and preserve history
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

    # User specified page range: 1 to 27
    start_page = 1
    end_page = 27

    # Override start and end page via args if provided
    if len(sys.argv) > 1:
        try:
            start_page = int(sys.argv[1])
            if len(sys.argv) > 2:
                end_page = int(sys.argv[2])
        except ValueError:
            logger.error("Invalid start/end page provided. Using defaults.")

    pages_to_scrape = list(range(start_page, end_page + 1))
    total_pages = len(pages_to_scrape)
    logger.info(f"Scraping {scraper_name if 'scraper_name' in locals() else 'BKKBN'} from Page {start_page} to {end_page}. Total pages: {total_pages}")

    if total_pages == 0:
        logger.info("No pages to scrape.")
        return

    scraper = BKKBNScraper(total_pages)
    
    async with aiohttp.ClientSession() as session:
        batch_size = 5 # Small batch to be safe
        for i in range(0, total_pages, batch_size):
            batch_pages = pages_to_scrape[i:i + batch_size]
            current_batch_num = i // batch_size + 1
            total_batches = (total_pages + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {current_batch_num}/{total_batches} ({len(batch_pages)} pages)")
            
            tasks = [scraper.fetch_page(session, page) for page in batch_pages]
            results = await asyncio.gather(*tasks)
            
            valid_results = []
            for page_results in results:
                if page_results:
                    for r in page_results:
                        if r['link'] not in scraped_links:
                            valid_results.append(r)
                            scraped_links.add(r['link'])

            if valid_results:
                # Add new results to the beginning (most recent first pattern)
                existing_content = valid_results + existing_content
                with open(OUTPUT_CONTENT_FILE, 'w', encoding='utf-8') as f:
                    json.dump(existing_content, f, indent=2, ensure_ascii=False)
                
                new_links = [{"source": r['source'], "title": r['title'], "link": r['link']} for r in valid_results]
                existing_links = new_links + existing_links
                with open(OUTPUT_LINKS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(existing_links, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Batch {current_batch_num} saved. Added {len(valid_results)} new articles.")
            
            if i + batch_size < total_pages:
                await asyncio.sleep(2)

    logger.info("BKKBN Scraping process completed.")

if __name__ == "__main__":
    asyncio.run(main())
