import streamlit as st
import asyncio
import json
import os
import sys
import logging
from pathlib import Path
from datetime import datetime
import aiohttp
import io
from urllib.parse import urljoin
from pypdf import PdfReader

# Add current directory to path if needed for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import configurations
from config import PERATURAN_CONFIG, get_rekapitulasi_filename, get_all_extracted_filename, get_metadata_filename
from config_general import GENERAL_SITES_CONFIG, SCRAPER_CONFIG, OUTPUT_LINKS_FILE, OUTPUT_CONTENT_FILE

# Fix for Windows NotImplementedError (asyncio subprocess)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import nest_asyncio
nest_asyncio.apply()

# Crawl4ai imports
try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode, JsonCssExtractionStrategy
except ImportError:
    st.error("Please install crawl4ai: `pip install crawl4ai`")

# Setup logging for Streamlit
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Streamlit Log Handler ---
class StreamlitLogHandler(logging.Handler):
    def __init__(self, placeholder):
        super().__init__()
        self.placeholder = placeholder
        self.logs = []

    def emit(self, record):
        msg = self.format(record)
        self.logs.append(msg)
        # Keep only last 20 logs for performance
        if len(self.logs) > 20:
            self.logs.pop(0)
        self.placeholder.code("\n".join(self.logs))

def setup_log_capture():
    st.sidebar.subheader("实时日志 (Real-time Logs)")
    log_placeholder = st.sidebar.empty()
    handler = StreamlitLogHandler(log_placeholder)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S'))
    
    # Add handler to relevant loggers
    root_logger = logging.getLogger()
    # Remove existing handlers to avoid duplicates
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    
    # Also capture crawl4ai logs if possible
    crawl_logger = logging.getLogger("crawl4ai")
    crawl_logger.addHandler(handler)

# --- Helper Functions ---

async def run_rekapitulasi(crawler, name, path):
    default_schema = {
        "name": "Rekapitulasi Peraturan",
        "baseSelector": "div.accordion_2 div.card",
        "fields": [
            {"name": "tahun", "selector": "h5.mb-0 a", "type": "text"},
            {"name": "jumlah_peraturan", "selector": "div.card-body li:nth-child(1) small", "type": "text"},
            {"name": "berlaku", "selector": "div.card-body li:nth-child(2) small", "type": "text"},
            {"name": "tidak_berlaku", "selector": "div.card-body li:nth-child(3) small", "type": "text"}
        ]
    }
    perda_schema = {
        "name": "Rekapitulasi Perda",
        "baseSelector": "div#accordionFlushExample div.accordion-item",
        "fields": [
            {"name": "tahun", "selector": "h2.accordion-header button", "type": "text"},
            {"name": "jumlah_peraturan", "selector": "div.accordion-body li:nth-child(1) small", "type": "text"},
            {"name": "berlaku", "selector": "div.accordion-body li:nth-child(2) small", "type": "text"},
            {"name": "tidak_berlaku", "selector": "div.accordion-body li:nth-child(3) small", "type": "text"}
        ]
    }
    
    current_schema = perda_schema if name.startswith("perda") else default_schema
    url = f'https://peraturan.go.id/{path}'
    
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=JsonCssExtractionStrategy(current_schema)
        )
    )
    
    if result.success:
        data = json.loads(result.extracted_content)
        for item in data:
            for key in ["tahun", "jumlah_peraturan", "berlaku", "tidak_berlaku"]:
                if key in item and item[key]:
                    try:
                        item[key] = int(item[key].strip("."))
                    except (ValueError, TypeError):
                        pass
        
        output_path = Path(__file__).parent / get_rekapitulasi_filename(name)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True, data, output_path
    return False, result.error_message, None

async def run_extract_all(crawler, p, rekap_data, batch_size=10):
    urls = []
    for item in rekap_data:
        tahun = item.get('tahun')
        try:
            jumlah = int(item.get('jumlah_peraturan', 0))
        except (ValueError, TypeError):
            jumlah = 0
        for nomor in range(1, jumlah + 1):
            url = f"https://peraturan.go.id/id/{p}-no-{nomor}-tahun-{tahun}"
            urls.append(url)
            
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
            {"name": "dokumen_peraturan", "selector": "tbody tr:nth-child(10) td a", "type": "attribute", "attribute": "href"}
        ]
    }
    
    extraction_strategy = JsonCssExtractionStrategy(schema)
    run_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, extraction_strategy=extraction_strategy)
    
    all_results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(0, len(urls), batch_size):
        batch_urls = urls[i:i + batch_size]
        status_text.text(f"Crawling batch {i // batch_size + 1}/{len(urls)//batch_size + 1}...")
        results = await crawler.arun_many(batch_urls, config=run_config)
        
        for result in results:
            if result.success:
                try:
                    data = json.loads(result.extracted_content)
                    if isinstance(data, list): all_results.extend(data)
                    else: all_results.append(data)
                except: pass
        
        progress_bar.progress(min((i + batch_size) / len(urls), 1.0))
        
    output_path = Path(__file__).parent / get_all_extracted_filename(p)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    return all_results, output_path

# --- UI Components ---

st.set_page_config(page_title="IndoGov Crawler", layout="wide")

st.title("🏛️ Indonesian Government Data Crawler")
st.markdown("Automated scraping for regulations and press releases.")

setup_log_capture()

tab0, tab1, tab2 = st.tabs(["� Dashboard & Stats", "�📜 Peraturan Go Id", "📰 Siaran Pers General"])

with tab0:
    st.header("Data Overview & Statistics")
    db_dir = Path(__file__).parent.parent / 'db'
    
    if db_dir.exists():
        all_files = list(db_dir.glob("*.json"))
        
        # Key Metrics
        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        
        # 1. Regulation Stats
        reg_files = [f for f in all_files if "peraturan_go_id_all" in f.name]
        total_regs = 0
        reg_counts = {}
        for f in reg_files:
            try:
                with open(f, 'r', encoding='utf-8') as j:
                    data = json.load(j)
                    count = len(data)
                    total_regs += count
                    type_key = f.name.split('_')[-1].replace('.json', '')
                    reg_counts[type_key] = count
            except: pass
            
        col_m1.metric("Total Regulations", f"{total_regs:,}")
        
        # 2. Press Release Stats
        news_file = db_dir / "siaran_pers_general.json"
        total_news = 0
        news_by_source = {}
        if news_file.exists():
            try:
                with open(news_file, 'r', encoding='utf-8') as j:
                    data = json.load(j)
                    total_news = len(data)
                    for item in data:
                        src = item.get('source', 'Unknown')
                        news_by_source[src] = news_by_source.get(src, 0) + 1
            except: pass
        col_m2.metric("Press Releases", f"{total_news:,}")
        
        # 3. File Stats
        col_m3.metric("Total JSON Files", len(all_files))
        
        db_size = sum(f.stat().st_size for f in all_files) / (1024 * 1024)
        col_m4.metric("Storage Used", f"{db_size:.2f} MB")
        
        st.divider()
        
        # Visualizations
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Regulations by Type")
            if reg_counts:
                st.bar_chart(reg_counts)
            else:
                st.info("No regulation data found in /db")
                
        with c2:
            st.subheader("News by Source")
            if news_by_source:
                st.bar_chart(news_by_source)
            else:
                st.info("No news data found in /db")
                
        st.subheader("Recent Files in Database")
        file_info = []
        for f in sorted(all_files, key=lambda x: x.stat().st_mtime, reverse=True)[:10]:
            file_info.append({
                "Filename": f.name,
                "Size (KB)": round(f.stat().st_size / 1024, 2),
                "Last Modified": datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M')
            })
        st.table(file_info)
    else:
        st.error(f"Database directory not found at {db_dir}")

with tab1:
    st.header("Regulations Scraper (peraturan.go.id)")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        reg_type = st.selectbox("Select Regulation Type", list(PERATURAN_CONFIG.keys()))
        batch_size = st.slider("Batch Size", 1, 50, 10)
        
        if st.button("Step 1: Run Rekapitulasi"):
            with st.status("Fetching rekapitulasi counts...", expanded=True) as status:
                async def do_rekap():
                    async with AsyncWebCrawler() as crawler:
                        return await run_rekapitulasi(crawler, reg_type, PERATURAN_CONFIG[reg_type])
                
                success, data, path = asyncio.run(do_rekap())
                if success:
                    status.update(label=f"Rekapitulasi saved to {path.name}", state="complete")
                    st.success(f"Found {len(data)} entries.")
                    st.dataframe(data)
                else:
                    status.update(label="Failed to fetch rekapitulasi", state="error")
                    st.error(data)

    with col2:
        rekap_file = Path(__file__).parent / get_rekapitulasi_filename(reg_type)
        if rekap_file.exists():
            st.info(f"Existing rekap file found: `{rekap_file.name}`")
            if st.button("Step 2: Scrape All Details"):
                with open(rekap_file, 'r', encoding='utf-8') as f:
                    rekap_data = json.load(f)
                
                async def do_extract():
                    async with AsyncWebCrawler() as crawler:
                        return await run_extract_all(crawler, reg_type, rekap_data, batch_size)
                
                with st.spinner("Extracting detail data..."):
                    results, path = asyncio.run(do_extract())
                    st.success(f"Successfully scraped {len(results)} records to {path.name}")
                    st.json(results[:5]) # Show first 5
        else:
            st.warning("Please run Rekapitulasi first for this type.")

    st.divider()
    st.subheader("Step 3: PDF Metadata & Enrichment")
    
    col3, col4 = st.columns([1, 1])
    with col3:
        if st.button("Enrich with PDF Metadata"):
            from peraturan_go_id_pdf_metadata import process_regulation_type, parse_pdf_date
            
            async def run_metadata():
                semaphore = asyncio.Semaphore(5)
                async with aiohttp.ClientSession() as session:
                    await process_regulation_type(session, reg_type, semaphore)
            
            with st.spinner(f"Extracting metadata for {reg_type} PDFs..."):
                asyncio.run(run_metadata())
                st.success("Metadata extraction complete!")
                
    with col4:
        meta_file = Path(__file__).parent / '..' / 'db' / get_metadata_filename(reg_type)
        if meta_file.exists():
            st.info(f"Enriched file: `{meta_file.name}`")
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
            st.write(f"Record count: {len(meta_data)}")
            if st.checkbox("Show Preview"):
                st.json(meta_data[:2])

with tab2:
    st.header("Press Releases Scraper (General)")
    
    col_l, col_r = st.columns(2)
    
    with col_l:
        st.subheader("Step 1: Scrape Links")
        max_pages = st.number_input("Max Pages per Site", 1, 10, 1)
        
        if st.button("Crawl Link List"):
            from siaran_pers_general_links import GeneralLinksScraper
            
            async def run_links():
                browser_config = BrowserConfig(headless=True)
                async with AsyncWebCrawler(config=browser_config) as crawler:
                    scraper = GeneralLinksScraper(crawler)
                    # Temporarily override config max_pages
                    SCRAPER_CONFIG["max_pages"] = max_pages
                    
                    all_results = []
                    for site_name, site_config in GENERAL_SITES_CONFIG.items():
                        st.write(f"Processing {site_name}...")
                        site_links = await scraper.scrape_site_links(site_name, site_config)
                        all_results.extend(site_links)
                    
                    with open(OUTPUT_LINKS_FILE, 'w', encoding='utf-8') as f:
                        json.dump(all_results, f, indent=2, ensure_ascii=False)
                    return all_results
            
            with st.spinner("Crawling links..."):
                links = asyncio.run(run_links())
                st.success(f"Found {len(links)} total links.")
                st.dataframe(links)

    with col_r:
        st.subheader("Step 2: Scrape Content")
        if os.path.exists(OUTPUT_LINKS_FILE):
            if st.button("Crawl Full Content"):
                from siaran_pers_general import GeneralContentScraper, main as run_content_main
                
                # Note: Content scraping might be heavy, we'll use a simplified version for Streamlit
                async def run_content():
                    with open(OUTPUT_LINKS_FILE, 'r', encoding='utf-8') as f:
                        news_items = json.load(f)
                    
                    st.write(f"Total articles: {len(news_items)}")
                    browser_config = BrowserConfig(headless=True)
                    async with AsyncWebCrawler(config=browser_config) as crawler:
                        scraper = GeneralContentScraper(crawler)
                        tasks = []
                        for i, item in enumerate(news_items):
                            source = item.get('source')
                            if source in GENERAL_SITES_CONFIG:
                                tasks.append(scraper.scrape_article(item, GENERAL_SITES_CONFIG[source], i, len(news_items)))
                        
                        results = await asyncio.gather(*tasks)
                        valid_results = [r for r in results if r is not None]
                        
                        with open(OUTPUT_CONTENT_FILE, 'w', encoding='utf-8') as f:
                            json.dump(valid_results, f, indent=2, ensure_ascii=False)
                        return valid_results

                with st.spinner("Extracting article body text..."):
                    content_results = asyncio.run(run_content())
                    st.success(f"Collected {len(content_results)} articles.")
                    st.dataframe(content_results[:10])
        else:
            st.info("Run Link Scraper first to generate the queue.")

st.sidebar.markdown("---")
st.sidebar.info("Designed for AITF UGM Team 3")
st.sidebar.write(f"Last update: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
