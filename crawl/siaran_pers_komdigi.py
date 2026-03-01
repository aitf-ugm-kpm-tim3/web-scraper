import asyncio
import json
import os
import sys
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

# Ensure UTF-8 output for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

async def main():
    # 1. Load links from the previously generated JSON
    links_file = r'siaran_pers_komdigi_links.json'
    
    if not os.path.exists(links_file):
        print(f"Error: {links_file} not found.")
        return

    with open(links_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Flatten news items from all pages in the JSON
    news_items = []
    for page in data:
        items = page.get('news_items', [])
        news_items.extend(items)
    
    print(f"Total items to crawl: {len(news_items)}")
    
    # Base URL for Komdigi
    base_url = "https://www.komdigi.go.id"
    
    # 2. Define extraction schema for detail pages
    schema = {
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
    
    extraction_strategy = JsonCssExtractionStrategy(schema)
    
    # 3. Configure Browser and Crawler
    browser_config = BrowserConfig(headless=True)
    run_config = CrawlerRunConfig(
        extraction_strategy=extraction_strategy,
        cache_mode=CacheMode.BYPASS,
        wait_for="css:section#section_text_body"
    )
    
    results = []
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        # We'll use a semaphore to limit concurrency and avoid being blocked
        # Using a conservative number for reliability
        max_concurrent = 3
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def crawl_item(item, index):
            async with semaphore:
                # Ensure the link starts with /
                link = item['link']
                if not link.startswith('/'):
                    link = '/' + link
                
                url = base_url + link
                print(f"[{index+1}/{len(news_items)}] Crawling: {url}")
                
                try:
                    result = await crawler.arun(url=url, config=run_config)
                    
                    if result.success:
                        extracted_data = json.loads(result.extracted_content)
                        
                        # Handle cases where multiple items might be matched by baseSelector
                        if isinstance(extracted_data, list) and len(extracted_data) > 0:
                            detail = extracted_data[0]
                        else:
                            detail = extracted_data if extracted_data else {}
                        
                        results.append({
                            "title": item['title'],
                            "link": item['link'],
                            "date": str(detail.get('date', '')).strip(),
                            "text": str(detail.get('text', '')).strip()
                        })
                    else:
                        print(f"Failed to crawl {url}: {result.error_message}")
                except Exception as e:
                    print(f"Error processing {url}: {e}")
                
                # Small delay to be polite
                await asyncio.sleep(0.5)

        # Process items
        tasks = [crawl_item(item, i) for i, item in enumerate(news_items)]
        await asyncio.gather(*tasks)
    
    # 4. Save the results
    output_file = r'siaran_pers_komdigi_all.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nScraping complete!")
    print(f"Total results collected: {len(results)}")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    asyncio.run(main())
