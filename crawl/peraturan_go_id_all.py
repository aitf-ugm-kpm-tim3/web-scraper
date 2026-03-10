import asyncio
import json
import os
from pathlib import Path
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, JsonCssExtractionStrategy

from config import PERATURAN_CONFIG, get_rekapitulasi_filename, get_all_extracted_filename

async def main():
    async with AsyncWebCrawler() as crawler:
        for p in PERATURAN_CONFIG.keys():
            print(f"\nProcessing regulation type: {p}")
            
            # 1. Load the rekapitulasi data (path from config)
            rekapitulasi_path = Path(get_rekapitulasi_filename(p))
            if not rekapitulasi_path.exists():
                print(f"Error: {rekapitulasi_path} not found. Skipping {p}.")
                continue

            with open(rekapitulasi_path, 'r', encoding='utf-8') as f:
                rekap_data = json.load(f)

            # 2. Load existing results for updatable crawling
            output_path = get_all_extracted_filename(p)
            all_results = []
            existing_urls = set()
            if os.path.exists(output_path):
                print(f"Loading existing data from {output_path}...")
                with open(output_path, 'r', encoding='utf-8') as f:
                    all_results = json.load(f)
                    # We assume 'link' or similar identifying field exists
                    # Actually, the schema doesn't have a direct 'link' field saved, 
                    # but we can reconstruct it from nomor/tahun/slug if needed
                    # OR we can add a 'scraped_url' field when saving.
                    for item in all_results:
                        if 'scraped_url' in item:
                            existing_urls.add(item['scraped_url'])

            # 3. Generate candidate URLs
            candidate_urls = []
            for item in rekap_data:
                tahun = item.get('tahun')
                try:
                    jumlah = int(item.get('jumlah_peraturan', 0))
                except (ValueError, TypeError):
                    jumlah = 0
                for nomor in range(1, jumlah + 1):
                    url = f"https://peraturan.go.id/id/{p}-no-{nomor}-tahun-{tahun}"
                    candidate_urls.append(url)

            # Filter out already crawled URLs
            urls_to_crawl = [u for u in candidate_urls if u not in existing_urls]
            print(f"Total: {len(candidate_urls)} | Already crawled: {len(existing_urls)} | To crawl: {len(urls_to_crawl)}")

            if not urls_to_crawl:
                print(f"All items for {p} are already crawled.")
                continue

            # 4. Define the extraction schema (same as before)
            schema = {
                "name": "Peraturan",
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

            # 5. Crawl the missing URLs
            batch_size = 10 
            for i in range(0, len(urls_to_crawl), batch_size):
                batch_urls = urls_to_crawl[i:i + batch_size]
                print(f"[{p}] Crawling batch {i // batch_size + 1}/{len(urls_to_crawl)//batch_size + 1}")

                results = await crawler.arun_many(batch_urls, config=run_config)

                for result in results:
                    if result.success:
                        try:
                            data = json.loads(result.extracted_content)
                            item = data[0] if isinstance(data, list) and data else (data if data else {})
                            if item:
                                item['scraped_url'] = result.url
                                all_results.append(item)
                        except json.JSONDecodeError:
                            print(f"Failed to decode JSON for {result.url}")
                    else:
                        print(f"Failed to crawl {result.url}: {result.error_message}")

                # Intermediate save
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(all_results, f, indent=2, ensure_ascii=False)

            print(f"Crawling complete for {p}. Total records: {len(all_results)}")

if __name__ == "__main__":
    asyncio.run(main())
