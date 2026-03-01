import asyncio
import json
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

async def main():
    # 1. Define the extraction schema
    schema = {
        "name": "News Links",
        "baseSelector": "body", # Scope to body so we can grab pagination + items
        "fields": [
            {
                "name": "page", 
                "selector": "button.relative.px-3.py-2.text-body-l.font-bold", # Targets your specific button
                "type": "text"
            },
            {
                "name": "news_items",
                "selector": "div.flex.flex-col.gap-1",
                "type": "list",
                "fields": [
                    {"name": "title", "selector": "a.text-base.line-clamp-2", "type": "text"},
                    {"name": "link", "selector": "a.text-base.line-clamp-2", "type": "attribute", "attribute": "href"},
                ]
            }
        ]
    }

    # 2. Configure the Browser
    browser_config = BrowserConfig(
        headless=True,
        verbose=True
    )

    # 3. Use session_id to maintain state across page clicks
    session_id = "komdigi_session"
    all_links = []
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        url = "https://www.komdigi.go.id/berita/siaran-pers"
        page_num = 1
        max_pages = 10
        
        while page_num <= max_pages:
            print(f"\n--- Scraping Page {page_num} ---")
            
            if page_num == 1:
                # First page: simple navigation
                config = CrawlerRunConfig(
                    extraction_strategy=JsonCssExtractionStrategy(schema),
                    cache_mode=CacheMode.BYPASS,
                    session_id=session_id,
                    wait_for="css:a.text-base.line-clamp-2"
                )
            else:
                # Subsequent pages: Click 'Next' without reloading the URL
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
                    extraction_strategy=JsonCssExtractionStrategy(schema),
                    cache_mode=CacheMode.BYPASS,
                    session_id=session_id,
                    js_code=js_click_next,
                    js_only=True, 
                    wait_for="css:a.text-base.line-clamp-2"
                )

            result = await crawler.arun(url=url, config=config)

            if not result.success:
                print(f"Failed to crawl page {page_num}: {result.error_message}")
                break

            # Extract links
            try:
                links = json.loads(result.extracted_content)
                if not links:
                    print(f"No links found on page {page_num}. Ending.")
                    break
                
                print(f"Found {len(links)} links on page {page_num}.")
                all_links.extend(links)
            except Exception as e:
                print(f"Error parsing content on page {page_num}: {e}")
                break

            # Check if this was the last page (Next button is disabled)
            import re
            if re.search(r'chevron-right_icon[^>]*text-netral-gray-03', result.html) or \
               re.search(r'text-netral-gray-03[^>]*chevron-right_icon', result.html):
                print("Reached the last page (Next button is disabled).")
                break
            
            page_num = int(all_links[-1]['page']) + 1
            # Give some time for the page to settle if needed, though wait_for handles most
            await asyncio.sleep(5)

        print(f"\nScraping complete! Total links collected: {len(all_links)}")
        with open('siaran_pers_komdigi_links.json', 'w', encoding='utf-8') as f:
            json.dump(all_links, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    asyncio.run(main())
