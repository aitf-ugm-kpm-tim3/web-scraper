import asyncio
import json
import os
from pathlib import Path
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, JsonCssExtractionStrategy

async def main():
    # 1. Load the rekapitulasi data
    rekapitulasi_path = Path(r'peraturan_go_id_rekapitulasi_uu.json')
    if not rekapitulasi_path.exists():
        print(f"Error: {rekapitulasi_path} not found.")
        return

    with open(rekapitulasi_path, 'r', encoding='utf-8') as f:
        rekap_data = json.load(f)

    # 2. Generate URLs
    # Format: https://peraturan.go.id/id/uu-no-{nomor}-tahun-{tahun}
    urls = []
    for item in rekap_data:
        tahun = item.get('tahun')
        jumlah = item.get('jumlah_peraturan', 0)
        for nomor in range(1, jumlah + 1):
            url = f"https://peraturan.go.id/id/uu-no-{nomor}-tahun-{tahun}"
            urls.append(url)

    print(f"Total URLs to crawl: {len(urls)}")

    # 3. Define the extraction schema (copied from uu.py)
    schema = {
        "name": "Undang-Undang",
        "baseSelector": "section#description",
        "fields": [
            {"name": "judul", "selector": "div.detail_title_1", "type": "text"},
            {"name": "jenis", "selector": "tbody tr:nth-child(1) td", "type": "text"},
            {"name": "pemrakarsa", "selector": "tbody tr:nth-child(2) td", "type": "text"},
            {"name": "nomor", "selector": "tbody tr:nth-child(3) td", "type": "text"},
            {"name": "tahun", "selector": "tbody tr:nth-child(4) td", "type": "text"},
            {"name": "tentang", "selector": "tbody tr:nth-child(5) td", "type": "text"},
            {"name": "tempat_penetapan", "selector": "tbody tr:nth-child(6) td", "type": "text"},
            {"name": "ditetapkan_tanggal", "selector": "tbody tr:nth-child(7) td", "type": "text"},
            {"name": "pejabat yang menetapkan", "selector": "tbody tr:nth-child(8) td", "type": "text"},
            {"name": "status", "selector": "tbody tr:nth-child(9) td", "type": "text"},
            {
                "name": "dokumen_peraturan",
                "selector": "tbody tr:nth-child(10) td a",
                "type": "attribute",
                "attribute": "href"
            }
        ]
    }

    extraction_strategy = JsonCssExtractionStrategy(schema)
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=extraction_strategy
    )

    # 4. Crawl the URLs
    all_results = []

    # Using a smaller batch or limit might be better if there are many URLs
    # For now, let's use arun_many.
    # NOTE: If there are thousands of URLs, you might want to process in batches.
    batch_size = 10  # Adjust based on server limits and system resources

    async with AsyncWebCrawler() as crawler:
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i + batch_size]
            print(f"Crawling batch {i // batch_size + 1} ({len(batch_urls)} URLs)...")

            results = await crawler.arun_many(batch_urls, config=run_config)

            for result in results:
                if result.success:
                    try:
                        data = json.loads(result.extracted_content)
                        # The extraction strategy returns a list or a single object depending on baseSelector
                        # Typically for JsonCssExtractionStrategy with baseSelector, it yields items
                        if isinstance(data, list):
                            all_results.extend(data)
                        else:
                            all_results.append(data)
                    except json.JSONDecodeError:
                        print(f"Failed to decode JSON for {result.url}")
                else:
                    print(f"Failed to crawl {result.url}: {result.error_message}")

            # Intermediate save to avoid losing data
            with open('peraturan_go_id_uu_all_extracted_partial.json', 'w', encoding='utf-8') as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)

    # 5. Save all results
    output_path = Path(r'peraturan_go_id_uu_all.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    print(f"Crawling complete. Saved {len(all_results)} records to {output_path}")

if __name__ == "__main__":
    asyncio.run(main())