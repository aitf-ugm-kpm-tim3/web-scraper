import asyncio
import json
import os
import logging
from pathlib import Path
import aiohttp
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from config import PERATURAN_CONFIG, get_all_extracted_filename, PDF_ROOT

class RegulationPDFDownloader:
    def __init__(self, production=False, dev_limit=5):
        self.base_url = "https://peraturan.go.id"
        self.production = production
        self.dev_limit = dev_limit
        self.download_dir = PDF_ROOT
        
    async def download_file(self, session, url, file_path, semaphore):
        """Downloads a single PDF file asynchronously."""
        async with semaphore:
            if file_path.exists():
                logger.debug(f"Skipping (already exists): {file_path.name}")
                return False, "Exists"

            try:
                async with session.get(url, timeout=30) as response:
                    if response.status == 200:
                        content = await response.read()
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(file_path, 'wb') as f:
                            f.write(content)
                        logger.info(f"Downloaded: {file_path.name}")
                        return True, "Downloaded"
                    else:
                        logger.error(f"Failed to download {url}: Status {response.status}")
                        return False, f"Status {response.status}"
            except Exception as e:
                logger.error(f"Error downloading {url}: {e}")
                return False, str(e)

    async def run_batch_download(self, specific_type=None):
        """Runs the batch download process."""
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        mode_str = "PRODUCTION" if self.production else f"DEVELOPMENT (Limit {self.dev_limit})"
        logger.info(f">>> PDF Download MODE: {mode_str}")

        types_to_process = [specific_type] if specific_type else PERATURAN_CONFIG.keys()
        
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(5) # Limit concurrency
            tasks = []
            
            for reg_type in types_to_process:
                json_filename = get_all_extracted_filename(reg_type)
                json_path = Path(json_filename)
                
                if not json_path.exists():
                    continue

                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if not self.production:
                    data = data[:self.dev_limit]

                logger.info(f"Processing {reg_type} ({len(data)} items)...")
                
                reg_dir = self.download_dir / reg_type
                reg_dir.mkdir(parents=True, exist_ok=True)

                for item in data:
                    relative_path = item.get('dokumen_peraturan')
                    if relative_path and relative_path.endswith('.pdf'):
                        clean_path = relative_path.lstrip('/')
                        download_url = f"{self.base_url}/{clean_path}"
                        
                        filename = Path(clean_path).name
                        file_path = reg_dir / filename
                        
                        tasks.append(self.download_file(session, download_url, file_path, semaphore))

            if tasks:
                results = await asyncio.gather(*tasks)
                downloaded = sum(1 for r in results if r[0])
                skipped = sum(1 for r in results if r[1] == "Exists")
                failed = len(tasks) - downloaded - skipped
                return downloaded, skipped, failed
            else:
                return 0, 0, 0

async def main():
    # Load .env for standalone run
    PROJECT_ROOT = Path(__file__).parent.parent
    load_dotenv(PROJECT_ROOT / ".env")
    
    production = os.getenv("PRODUCTION", "false").lower() == "true"
    dev_limit = int(os.getenv("DEV_LIMIT", "5"))
    
    downloader = RegulationPDFDownloader(production=production, dev_limit=dev_limit)
    await downloader.run_batch_download()

if __name__ == "__main__":
    asyncio.run(main())
