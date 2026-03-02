# 🏛️ AITF UGM Tim 3: Law & News Scraper

![Project Banner](assets/banner.png)

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Crawl4AI](https://img.shields.io/badge/powered%20by-crawl4ai-orange.svg)](https://github.com/unclecode/crawl4ai)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance web scraping suite designed for extracting Indonesian legal regulations from **peraturan.go.id** and public news from **komdigi.go.id**. Built using the powerful `crawl4ai` library for efficient, automated data extraction.

---

## 📑 Table of Contents

- [🚀 Key Features](#-key-features)
- [🛠️ Installation](#️-installation)
- [📖 Usage Guide](#-usage-guide)
  - [1. Peraturan.go.id (Regulations)](#1-peraturangoid-regulations)
  - [2. Komdigi.go.id (News)](#2-komdigigoid-news)
- [📂 Directory Structure](#-directory-structure)
- [📦 Data Schema](#-data-schema)
- [🛡️ License](#️-license)

---

## 🚀 Key Features

- **Comprehensive Scrapes**: Supports UU, Perpres, Perppu, Penpres, Keppres, and Inpres.
- **Smart Extraction**: Uses CSS-based JSON extraction strategies.
- **Batch Processing**: Handles multi-page navigation and batch PDF downloads.
- **Data Integrity**: Includes deduplication scripts to ensure clean datasets.
- **Asynchronous**: Built on `asyncio` for high-speed concurrent crawling.

---

## 🛠️ Installation

### 1. Prerequisite

Ensure you have **Python 3.8+** installed.

### 2. Setup Environment

```bash
# Clone the repository
git clone https://github.com/zidanarsyad/aitf-ugm-tim3.git
cd aitf-ugm-tim3

# Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/scripts/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install crawl4ai aiohttp
```

### 3. Initialize Crawl4AI

```bash
crawl4ai-setup
crawl4ai-doctor  # Verify the installation
```

---

## 📖 Usage Guide

### 1. Peraturan.go.id (Regulations)

The workflow for scraping regulations involves three main steps:

#### **Step A: Generate Rekapitulasi**

First, gather the metadata and counts of regulations per year.

```bash
python crawl/peraturan_go_id_rekapitulasi.py
```

_Output: `db/peraturan_go_id_rekapitulasi_{type}.json`_

#### **Step B: Scrape Detailed Data**

Use the rekapitulasi data to crawl individual regulation pages.

```bash
python crawl/peraturan_go_id_all.py
```

_Output: `db/peraturan_go_id_all_{type}.json`_

#### **Step C: Download PDFs**

Download the actual legal documents in PDF format.

```bash
python crawl/peraturan_go_id_batch_pdf_download.py
```

_Output: `pdf_downloads/{type}/_.pdf`\*

---

### 2. Komdigi.go.id (News)

The workflow for scraping Komdigi news articles:

#### **Step A: Extract Links**

Collect all article links from the news archive.

```bash
python crawl/siaran_pers_komdigi_links.py
```

_Output: `db/siaran_pers_komdigi_links.json`_

#### **Step B: Clean Data**

Remove duplicate links if necessary.

```bash
python crawl/siaran_pers_komdigi_remove_duplicates.py
```

#### **Step C: Scrape Content**

Extract full content from the collected links.

```bash
python crawl/siaran_pers_komdigi.py
```

_Output: `db/siaran_pers_komdigi_all.json`_

---

## 📂 Directory Structure

```text
.
├── assets/           # Media assets for README
├── crawl/            # Python scraping scripts
├── db/               # JSON output files
├── pdf_downloads/    # Downloaded PDF documents
└── README.md         # Project documentation
```

---

## 📦 Data Schema

Example JSON output structure for a regulation:

```json
{
  "judul": "UNDANG-UNDANG REPUBLIK INDONESIA NOMOR 1 TAHUN 2024",
  "jenis": "Undang-Undang",
  "nomor": "1",
  "tahun": "2024",
  "tentang": "PERUBAHAN KEDUA ATAS UNDANG-UNDANG NOMOR 11 TAHUN 2008 TENTANG INFORMASI DAN TRANSAKSI ELEKTRONIK",
  "status": "Berlaku",
  "dokumen_peraturan": "https://peraturan.go.id/common/dokumen/ln/2024/uu1-2024.pdf"
}
```

---

## 🛡️ License

Distributed under the MIT License. See `LICENSE` for more information.

---

**Developed by AITF UGM Tim 3** 🚀
