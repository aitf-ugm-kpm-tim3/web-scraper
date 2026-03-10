import asyncio
import json
import os
from pathlib import Path
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai import JsonCssExtractionStrategy

from config import PERATURAN_CONFIG, get_rekapitulasi_filename

async def main():
    default_schema = {
        "name": "Rekapitulasi Peraturan",
        "baseSelector": "div.accordion_2 div.card",
        "fields": [
            {
                "name": "tahun", 
                "selector": "h5.mb-0 a", 
                "type": "text"
            },
            {
                "name": "jumlah_peraturan", 
                "selector": "div.card-body li:nth-child(1) small", 
                "type": "text"
            },
            {
                "name": "berlaku", 
                "selector": "div.card-body li:nth-child(2) small", 
                "type": "text"
            },
            {
                "name": "tidak_berlaku", 
                "selector": "div.card-body li:nth-child(3) small", 
                "type": "text"
            }
        ]
    }

    perda_schema = {
        "name": "Rekapitulasi Perda",
        "baseSelector": "div#accordionFlushExample div.accordion-item",
        "fields": [
            {
                "name": "tahun", 
                "selector": "h2.accordion-header button", 
                "type": "text"
            },
            {
                "name": "jumlah_peraturan", 
                "selector": "div.accordion-body li:nth-child(1) small", 
                "type": "text"
            },
            {
                "name": "berlaku", 
                "selector": "div.accordion-body li:nth-child(2) small", 
                "type": "text"
            },
            {
                "name": "tidak_berlaku", 
                "selector": "div.accordion-body li:nth-child(3) small", 
                "type": "text"
            }
        ]
    }

    async with AsyncWebCrawler() as crawler:
        for name, path in PERATURAN_CONFIG.items():
            current_schema = perda_schema if name.startswith("perda") else default_schema
            
            # Construct the file URL for the local HTML file
            url = f'https://peraturan.go.id/{path}'
            print(f"Crawling {url}...")
            
            result = await crawler.arun(
                url=url,
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    extraction_strategy=JsonCssExtractionStrategy(current_schema)
                )
            )
            
            if result.success:
                # The JSON output is stored in 'extracted_content'
                data = json.loads(result.extracted_content)
                
                # Post-process to convert strings to integers
                for item in data:
                    for key in ["tahun", "jumlah_peraturan", "berlaku", "tidak_berlaku"]:
                        if key in item and item[key]:
                            try:
                                # Strip whitespace and convert to int
                                item[key] = int(item[key].strip("."))
                            except (ValueError, TypeError):
                                # Fallback if conversion fails (keep as original or None)
                                pass

                # Output file (absolute path from config)
                output_path = get_rekapitulasi_filename(name)
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"Data saved to {output_path}")
            else:
                print(f"Extraction failed for {name}: {result.error_message}")

if __name__ == "__main__":
    asyncio.run(main())
