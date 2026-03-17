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
from dotenv import load_dotenv
import plotly.express as px
import pandas as pd

# Load environment variables
load_dotenv(Path(__file__).parent.parent / ".env")

# Global Constants
TOTAL_REGULATIONS_GOAL = 61815

# Add current directory to path if needed for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import configurations
from config import PERATURAN_CONFIG, get_rekapitulasi_filename, get_all_extracted_filename, get_metadata_filename, DB_ROOT, PDF_ROOT
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
        
        # Output file (absolute path from config)
        output_path = get_rekapitulasi_filename(name)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True, data, Path(output_path)
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
        
    # Output file (absolute path from config)
    output_path = get_all_extracted_filename(p)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    return all_results, Path(output_path)

# --- UI Components ---

st.set_page_config(page_title="IndoGov Crawler", layout="wide")

st.title("🏛️ Indonesian Government Data Crawler")
st.markdown("Automated scraping for regulations and press releases.")

setup_log_capture()

tab0, tab1, tab1b, tab2, tab3 = st.tabs(["📊 Dashboard & Stats", "📑📜 Peraturan Go Id", "📑 Perda Explorer", "📰 Siaran Pers General", "🏛️ Siaran Pers Komdigi"])

with tab0:
    st.header("Data Overview & Statistics")
    db_dir = DB_ROOT
    
    if db_dir.exists():
        all_files = list(db_dir.glob("*.json"))
        
        # Key Metrics
        m_col1, m_col2, m_col3 = st.columns(3)
        m_col4, m_col5, m_col6 = st.columns(3)
        
        # 1. Regulation Stats
        reg_files = [f for f in all_files if "peraturan_go_id_all" in f.name or f.name == "peraturan_go_id_perda.json"]
        total_regs, total_docs = 0, 0
        reg_counts = {}
        for f in reg_files:
            try:
                with open(f, 'r', encoding='utf-8') as j:
                    data = json.load(j)
                    total_regs += len(data)
                    for item in data:
                        doc_link = item.get('dokumen_peraturan')
                        if doc_link and isinstance(doc_link, str) and doc_link.strip():
                            total_docs += 1
                            
                    type_key = f.name.split('_')[-1].replace('.json', '')
                    reg_counts[type_key] = reg_counts.get(type_key, 0) + len(data)
            except: pass
            
        progress_pct = (total_regs / TOTAL_REGULATIONS_GOAL) * 100 if total_regs > 0 else 0
        m_col1.metric("Crawled Regulations", f"{total_regs:,}")
        m_col2.metric("Valid Documents", f"{total_docs:,}")
        m_col3.metric("Crawl Progress", f"{progress_pct:.2f}%")
        
        # 2. Press Release Stats
        news_file = db_dir / "siaran_pers_general.json"
        komdigi_file = db_dir / "siaran_pers_komdigi_all.json"
        total_news = 0
        news_by_source = {}
        
        # General news
        if news_file.exists():
            try:
                with open(news_file, 'r', encoding='utf-8') as j:
                    data = json.load(j)
                    total_news += len(data)
                    for item in data:
                        src = item.get('source', 'Unknown')
                        news_by_source[src] = news_by_source.get(src, 0) + 1
            except: pass
            
        # Komdigi news
        if komdigi_file.exists():
            try:
                with open(komdigi_file, 'r', encoding='utf-8') as j:
                    data = json.load(j)
                    total_news += len(data)
                    news_by_source['KOMDIGI'] = len(data)
            except: pass
            
        m_col4.metric("Press Releases", f"{total_news:,}")
        
        # 3. PDF Stats
        pdf_dir = PDF_ROOT
        all_pdfs = list(pdf_dir.glob("**/*.pdf")) if pdf_dir.exists() else []
        pdf_size = sum(f.stat().st_size for f in all_pdfs) / (1024 * 1024) if all_pdfs else 0
        m_col5.metric("Downloaded PDF Files", f"{len(all_pdfs):,}")

        # 4. Storage Stats
        m_col6.metric("PDF Storage", f"{pdf_size:.2f} MB")
        
        st.divider()
        
        # Visualizations
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Regulations by Type")
            if reg_counts:
                df_reg = pd.DataFrame(list(reg_counts.items()), columns=['Type', 'Count'])
                df_reg['Root'] = 'Regulations'
                fig_reg = px.treemap(df_reg, path=['Root', 'Type'], values='Count')
                st.plotly_chart(fig_reg, width='stretch')
            else:
                st.info("No regulation data found in /db")
                
        with c2:
            st.subheader("News by Source")
            if news_by_source:
                df_news = pd.DataFrame(list(news_by_source.items()), columns=['Source', 'Count'])
                df_news['Root'] = 'News'
                fig_news = px.treemap(df_news, path=['Root', 'Source'], values='Count')
                st.plotly_chart(fig_news, width='stretch')
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
        rekap_file = Path(get_rekapitulasi_filename(reg_type))
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
        meta_file = Path(get_metadata_filename(reg_type))
        if meta_file.exists():
            st.info(f"Enriched file: `{meta_file.name}`")
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
            st.write(f"Record count: {len(meta_data)}")
            if st.checkbox("Show Preview"):
                st.json(meta_data[:2])

    st.divider()
    st.subheader("Step 4: PDF Downloader")
    col5, col6 = st.columns([1, 1])
    
    with col5:
        st.info("Download PDFs for the selected regulation type.")
        is_prod = st.toggle("Production Mode (Full Download)", value=os.getenv("PRODUCTION", "false").lower() == "true")
        dev_lim = st.number_input("Dev Limit (if not Production)", 1, 100, int(os.getenv("DEV_LIMIT", "5")))
        
        if st.button("Start Batch Download"):
            from peraturan_go_id_batch_pdf_download import RegulationPDFDownloader
            
            async def run_pdf_download():
                downloader = RegulationPDFDownloader(production=is_prod, dev_limit=dev_lim)
                return await downloader.run_batch_download(specific_type=reg_type)
            
            with st.spinner(f"Downloading PDFs for {reg_type}..."):
                downloaded, skipped, failed = asyncio.run(run_pdf_download())
                st.success(f"Finished! Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")
    
    with col6:
        type_pdf_dir = PDF_ROOT / reg_type
        if type_pdf_dir.exists():
            pdf_files = list(type_pdf_dir.glob("*.pdf"))
            st.write(f"Files in `{reg_type}/` folder: **{len(pdf_files)}**")
            if pdf_files:
                st.write("Latest 5 files:")
                for f in sorted(pdf_files, key=lambda x: x.stat().st_mtime, reverse=True)[:5]:
                    st.text(f"📄 {f.name}")

with tab1b:
    st.header("Peraturan Daerah Scraper (peraturan.go.id/perda)")
    
    col_perda1, col_perda2 = st.columns([1, 1])
    
    with col_perda1:
        st.subheader("Step 1: Scrape Perda Links")
        start_page = st.number_input("Start Page", 1, 1500, 1)
        end_page = st.number_input("End Page", 1, 1500, 5)
        
        if st.button("Start Scraping Links"):
            from peraturan_go_id_perda_links import run_scraper as run_perda_links
            
            with st.spinner(f"Scraping links from page {start_page} to {end_page}..."):
                status_holder = st.empty()
                prog_bar = st.progress(0)
                
                async def do_scrape_links():
                    return await run_perda_links(start_page, end_page, status_holder, prog_bar)
                    
                links_data = asyncio.run(do_scrape_links())
                st.success(f"Successfully collected {len(links_data)} link entries.")
                st.dataframe(links_data)
                
    with col_perda2:
        st.subheader("Step 2: Scrape Perda Details")
        
        input_perda_json = DB_ROOT / 'peraturan_go_id_perda_links.json'
        if input_perda_json.exists():
            st.info(f"Link list found. Proceed to detail extraction.")
            
            if st.button("Start Detail Scraping"):
                from peraturan_go_id_perda import run_perda_detail_scraper
                
                with st.spinner("Scraping details..."):
                    status_holder_det = st.empty()
                    prog_bar_det = st.progress(0)
                    
                    async def do_scrapedet():
                        return await run_perda_detail_scraper(status_holder_det, prog_bar_det)
                        
                    success, msg, details_data = asyncio.run(do_scrapedet())
                    
                    if success:
                        st.success(msg)
                        if details_data:
                            st.dataframe(details_data[:5])
                    else:
                        st.error(msg)
        else:
            st.warning("Please run Step 1 (Link Scraper) first to generate the list of URLs.")

with tab2:
    st.header("Press Releases Scraper (General)")
    
    selected_sites = st.multiselect(
        "Select Sites to Scrape", 
        options=list(GENERAL_SITES_CONFIG.keys()), 
        default=list(GENERAL_SITES_CONFIG.keys())
    )
    
    col_l, col_r = st.columns(2)
    
    with col_l:
        st.subheader("Step 1: Scrape Links")
        max_pages = st.number_input("Max Pages per Site", 1, 300, 1)
        
        if st.button("Crawl Link List"):
            from siaran_pers_general_links import GeneralLinksScraper
            
            async def run_links():
                browser_config = BrowserConfig(headless=True)
                async with AsyncWebCrawler(config=browser_config) as crawler:
                    scraper = GeneralLinksScraper(crawler)
                    # Temporarily override config max_pages
                    SCRAPER_CONFIG["max_pages"] = max_pages
                    
                    # Load existing links to avoid duplicates
                    existing_all_links = []
                    existing_links_set = set()
                    if os.path.exists(OUTPUT_LINKS_FILE):
                        try:
                            with open(OUTPUT_LINKS_FILE, 'r', encoding='utf-8') as f:
                                existing_all_links = json.load(f)
                                existing_links_set = {item['link'] for item in existing_all_links}
                        except: pass

                    new_results = []
                    for site_name in selected_sites:
                        if site_name in GENERAL_SITES_CONFIG:
                            site_config = GENERAL_SITES_CONFIG[site_name]
                            st.write(f"Processing {site_name}...")
                            site_links = await scraper.scrape_site_links(site_name, site_config, existing_links_set)
                            new_results.extend(site_links)
                    
                    # Merge and save
                    final_all_links = new_results + existing_all_links
                    with open(OUTPUT_LINKS_FILE, 'w', encoding='utf-8') as f:
                        json.dump(final_all_links, f, indent=2, ensure_ascii=False)
                    return final_all_links
            
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
                    
                    # Load existing content for updatable crawling
                    existing_content = []
                    scraped_links = set()
                    if os.path.exists(OUTPUT_CONTENT_FILE):
                        try:
                            with open(OUTPUT_CONTENT_FILE, 'r', encoding='utf-8') as f:
                                existing_content = json.load(f)
                                scraped_links = {item['link'] for item in existing_content if 'link' in item}
                            st.info(f"Loaded {len(existing_content)} existing articles.")
                        except Exception as e:
                            st.error(f"Could not load existing output: {e}")

                    # Filter to only new items and selected sites
                    items_to_crawl = [
                        item for item in news_items 
                        if item.get('link') not in scraped_links and item.get('source') in selected_sites
                    ]
                    total_to_crawl = len(items_to_crawl)
                    
                    st.write(f"Total links to crawl: {total_to_crawl} (Total in list: {len(news_items)})")
                    
                    if total_to_crawl == 0:
                        st.success("All articles for selected sites are already scraped.")
                        return existing_content

                    browser_config = BrowserConfig(headless=True)
                    async with AsyncWebCrawler(config=browser_config) as crawler:
                        scraper = GeneralContentScraper(crawler)
                        
                        batch_size = 10
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        for i in range(0, total_to_crawl, batch_size):
                            batch = items_to_crawl[i:i + batch_size]
                            current_batch_num = i // batch_size + 1
                            total_batches = (total_to_crawl + batch_size - 1) // batch_size
                            
                            status_text.text(f"Processing batch {current_batch_num}/{total_batches} ({len(batch)} items)...")
                            
                            tasks = []
                            for j, item in enumerate(batch):
                                source = item.get('source')
                                if source in GENERAL_SITES_CONFIG:
                                    tasks.append(scraper.scrape_article(item, GENERAL_SITES_CONFIG[source], i + j, total_to_crawl))
                                else:
                                    logger.warning(f"No config for source: {source}")
                            
                            results = await asyncio.gather(*tasks)
                            valid_results = [r for r in results if r is not None]
                            
                            if valid_results:
                                # Merge: New results at the top
                                existing_content = valid_results + existing_content
                                
                                # Save checkpoint after each batch
                                with open(OUTPUT_CONTENT_FILE, 'w', encoding='utf-8') as f:
                                    json.dump(existing_content, f, indent=2, ensure_ascii=False)
                            
                            progress_bar.progress(min((i + batch_size) / total_to_crawl, 1.0))
                            
                            # Polite delay between batches
                            if i + batch_size < total_to_crawl:
                                await asyncio.sleep(1)

                        status_text.text("Scraping complete!")
                        return existing_content

                with st.spinner("Extracting article body text..."):
                    content_results = asyncio.run(run_content())
                    st.success(f"Collected {len(content_results)} articles.")
                    st.dataframe(content_results[:10])
        else:
            st.info("Run Link Scraper first to generate the queue.")

with tab3:
    st.header("Press Releases Scraper (Komdigi)")
    
    col_k1, col_k2 = st.columns(2)
    
    with col_k1:
        st.subheader("Step 1: Scrape Links")
        max_pages_komdigi = st.number_input("Max Pages (Komdigi)", 1, 500, 1)
        
        if st.button("Crawl Komdigi Links"):
            from siaran_pers_komdigi_links import KomdigiLinksScraper
            
            async def run_komdigi_links():
                browser_config = BrowserConfig(headless=True)
                async with AsyncWebCrawler(config=browser_config) as crawler:
                    scraper = KomdigiLinksScraper(crawler)
                    return await scraper.scrape_links(max_pages=max_pages_komdigi)
            
            with st.spinner("Crawling Komdigi links..."):
                links_data = asyncio.run(run_komdigi_links())
                total_items = sum(len(p.get('news_items', [])) for p in links_data)
                st.success(f"Total entries available: {total_items}")
                if links_data:
                    st.json(links_data[0].get('news_items', [])[:5])

    with col_k2:
        st.subheader("Step 2: Scrape Content")
        if (DB_ROOT / 'siaran_pers_komdigi_links.json').exists():
            if st.button("Crawl Komdigi Content"):
                from siaran_pers_komdigi import KomdigiContentScraper
                
                async def run_komdigi_content():
                    browser_config = BrowserConfig(headless=True)
                    async with AsyncWebCrawler(config=browser_config) as crawler:
                        scraper = KomdigiContentScraper(crawler)
                        return await scraper.scrape_content()
                
                with st.spinner("Extracting article content..."):
                    content_data = asyncio.run(run_komdigi_content())
                    st.success(f"Database now has {len(content_data)} Komdigi articles.")
                    if content_data:
                        st.dataframe(content_data[:10])
        else:
            st.info("Run Link Scraper first to generate the queue.")

st.sidebar.markdown("---")
st.sidebar.info("Designed for AITF UGM Team 3")
st.sidebar.write(f"Last update: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
