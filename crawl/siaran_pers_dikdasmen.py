import asyncio
import json
import os
import sys
import logging
import re
import aiohttp
from bs4 import BeautifulSoup

from config_general import SCRAPER_CONFIG, OUTPUT_LINKS_FILE, OUTPUT_CONTENT_FILE

# Ensure UTF-8 output for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DikdasmenScraper:
    def __init__(self, total_pages: int):
        self.semaphore = asyncio.Semaphore(SCRAPER_CONFIG.get("concurrency_limit", 10))
        self.source_name = "DIKDASMEN_PERS"
        self.total_pages = total_pages
        self.success_count = 0
        self.failed_count = 0
        self.processed_count = 0
        self.csrf_token = None
        self.base_url = "https://kemendikdasmen.go.id"
        self.search_url = f"{self.base_url}/pencarian/siaran-pers/search"
        
    async def get_csrf_token(self, session):
        """Fetch the CSRF token from the search page."""
        try:
            async with session.get(self.search_url, timeout=30) as response:
                if response.status == 200:
                    html = await response.text()
                    # Look for odoo.csrf_token or csrf_token in the script
                    match = re.search(r'csrf_token:\s*"([^"]+)"', html)
                    if match:
                        self.csrf_token = match.group(1)
                        logger.info(f"[{self.source_name}] Successfully fetched CSRF token: {self.csrf_token}")
                        return self.csrf_token
                    
                    # Fallback pattern
                    match = re.search(r'"csrf_token":\s*"([^"]+)"', html)
                    if match:
                        self.csrf_token = match.group(1)
                        logger.info(f"[{self.source_name}] Successfully fetched CSRF token (fallback): {self.csrf_token}")
                        return self.csrf_token
        except Exception as e:
            logger.error(f"[{self.source_name}] Error fetching CSRF token: {e}")
        
        # User provided fallback token
        self.csrf_token = "7ffacafe9dcc678016f6bb852d89320f05132451o1808460700"
        logger.warning(f"[{self.source_name}] Could not find CSRF token in HTML. Using user-provided fallback.")
        return self.csrf_token

    async def fetch_page(self, session, page: int):
        async with self.semaphore:
            if not self.csrf_token:
                await self.get_csrf_token(session)
                
            payload = {
                "jsonrpc": "2.0",
                "method": "call",
                "params": {
                    "keyword": "",
                    "sort_order": "terbaru",
                    "kategori": [],
                    "kelompok": [],
                    "tagging": "",
                    "pengguna": [],
                    "tahun": [],
                    "page": page,
                    "limit": 100,
                    "csrf_token": self.csrf_token
                },
                "id": 542621263
            }
            
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Content-Type": "application/json",
                    "X-Requested-With": "XMLHttpRequest"
                }

                async with session.post(self.search_url, json=payload, headers=headers, timeout=30) as response:
                    self.processed_count += 1
                    progress = f"{self.processed_count}/{self.total_pages}"
                    
                    if response.status != 200:
                        self.failed_count += 1
                        logger.error(f"[{self.source_name}] Page {page} Failed: HTTP {response.status} | Progress: {progress}")
                        return None
                    
                    data = await response.json()
                    
                    # Odoo JSON-RPC structure usually has data in 'result'
                    # result might contain 'html' or 'items' depending on the implementation
                    # Based on the user request, it seems they expect direct fields from items
                    result = data.get('result', {})
                    
                    # We need to find where the list of press releases is.
                    # Commonly in Odoo search it's result.records or result.results
                    # But the user implies they know the field names: url, name, tgl_rilis, rangkuman
                    # Let's assume result is a list or contains a list named 'records'
                    items = []
                    if isinstance(result, list):
                        items = result
                    elif isinstance(result, dict):
                        # Try common keys
                        items = result.get('records', result.get('results', result.get('data', [])))

                    if not items:
                        # If we can't find items, maybe the whole result is what we need or it's empty
                        if not result:
                            logger.warning(f"[{self.source_name}] Page {page}: No items found in result.")
                            return []
                        
                    results = []
                    for item in items:
                        url_slug = item.get('url', '')
                        name = item.get('name', '')
                        tgl_rilis = item.get('tgl_rilis', '')
                        rangkuman = item.get('rangkuman', '')
                        
                        if not url_slug or not name:
                            continue
                            
                        link = f"{self.base_url}{url_slug}" if url_slug.startswith('/') else f"{self.base_url}/{url_slug}"
                        
                        # Clean rangkuman from HTML if needed
                        if rangkuman and "<" in rangkuman and ">" in rangkuman:
                            rangkuman = BeautifulSoup(rangkuman, "html.parser").get_text(separator=' ', strip=True)
                        
                        record = {
                            "link": link,
                            "title": name,
                            "source": self.source_name,
                            "date": tgl_rilis,
                            "text": rangkuman
                        }
                        results.append(record)
                        
                    self.success_count += len(results)
                    logger.info(f"[{self.source_name}] Page {page} Success: Fetched {len(results)} items | Progress: {progress}")
                    return results

            except Exception as e:
                self.failed_count += 1
                logger.error(f"[{self.source_name}] Page {page} Error: {str(e)}")
                return None
            finally:
                await asyncio.sleep(SCRAPER_CONFIG.get("polite_delay", 0.5))

async def main():
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

    start_page = 1
    # We don't know the total pages yet, let's start with a reasonable number or determine from first page
    end_page = 5 # Default to 5 pages if not specified

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
    logger.info(f"Scraping from Page {start_page} to {end_page}. Total pages to fetch: {total_pages}")

    scraper = DikdasmenScraper(total_pages)
    
    async with aiohttp.ClientSession() as session:
        # Get CSRF token once
        await scraper.get_csrf_token(session)
        
        batch_size = 5
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
                # Add new results to the beginning (assuming terbaru)
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

    logger.info("Dikdasmen Scraping process completed.")

if __name__ == "__main__":
    asyncio.run(main())
