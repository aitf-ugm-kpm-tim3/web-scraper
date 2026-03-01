## Installation

```bash
pip install crawl4ai
crawl4ai-setup
crawl4ai-doctor
```

## Usage

### peraturan.go.id

#### 1. Rekapitulasi

```bash
python peraturan_go_id_rekapitulasi.py
```

output: `peraturan_go_id_rekapitulasi_{peraturan}.json`

#### 2. Scraping Data

input: `peraturan_go_id_rekapitulasi_uu.json`

```bash
python peraturan_go_id_uu.py
```

output: `peraturan_go_id_uu_all.json`
backup: `peraturan_go_id_uu_all_extracted_partial.json`

### www.komdigi.go.id/berita/siaran-pers

#### 1. Links

```bash
python siaran_pers_komdigi_links.py
```

output: `siaran_pers_komdigi_links.json`

#### 2. Scraping Data

input: `siaran_pers_komdigi_links.json`

```bash
python siaran_pers_komdigi.py
```

output: `siaran_pers_komdigi_all.json`
