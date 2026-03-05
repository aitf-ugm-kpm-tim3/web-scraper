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
            # 1. Load the rekapitulasi data
            # Assuming the JSON files are in the same directory or adjust path as needed
            rekapitulasi_path = Path(get_rekapitulasi_filename(p))
            if not rekapitulasi_path.exists():
                # Fallback to ../db/ if not found in current dir
                db_path = Path('..') / 'db' / get_rekapitulasi_filename(p)
                if db_path.exists():
                    rekapitulasi_path = db_path
                else:
                    print(f"Error: {rekapitulasi_path} (or {db_path}) not found. Skipping {p}.")
                    continue

            with open(rekapitulasi_path, 'r', encoding='utf-8') as f:
                rekap_data = json.load(f)

            # 2. Generate URLs
            urls = []
            for item in rekap_data:
                tahun = item.get('tahun')
                try:
                    jumlah = int(item.get('jumlah_peraturan', 0))
                except (ValueError, TypeError):
                    jumlah = 0
                for nomor in range(1, jumlah + 1):
                    # Use p directly as the slug (matches standardized keys in config.py)
                    url = f"https://peraturan.go.id/id/{p}-no-{nomor}-tahun-{tahun}"
                    urls.append(url)

            print(f"Total URLs to crawl for {p}: {len(urls)}")

            # 3. Define the extraction schema
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

            # 4. Crawl the URLs
            all_results = []
            batch_size = 10 

            for i in range(0, len(urls), batch_size):
                batch_urls = urls[i:i + batch_size]
                print(f"[{p}] Crawling batch {i // batch_size + 1} ({len(batch_urls)} URLs)...")

                results = await crawler.arun_many(batch_urls, config=run_config)

                for result in results:
                    if result.success:
                        try:
                            data = json.loads(result.extracted_content)
                            if isinstance(data, list):
                                all_results.extend(data)
                            else:
                                all_results.append(data)
                        except json.JSONDecodeError:
                            print(f"Failed to decode JSON for {result.url}")
                    else:
                        print(f"Failed to crawl {result.url}: {result.error_message}")

                # Intermediate save
                partial_output = Path(f'peraturan_go_id_all_{p}_extracted_partial.json')
                with open(partial_output, 'w', encoding='utf-8') as f:
                    json.dump(all_results, f, indent=2, ensure_ascii=False)

            # 5. Save all results for this type
            output_path = Path(get_all_extracted_filename(p))
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)

            print(f"Crawling complete for {p}. Saved {len(all_results)} records to {output_path}")

if __name__ == "__main__":
    asyncio.run(main())
