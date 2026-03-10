from pathlib import Path

# Project directories (consistent with config.py)
PROJECT_ROOT = Path(__file__).parent.parent
DB_ROOT = PROJECT_ROOT / 'db'

# Ensure directories exist
DB_ROOT.mkdir(parents=True, exist_ok=True)

# Configuration for general press releases (BAPPENAS, BGN, ESDM)

GENERAL_SITES_CONFIG = {
    "BAPPENAS": {
        "links": {
            "url_template": "https://www.bappenas.go.id/kategori-berita/207?page={page}",
            "schema": {
                "name": "BAPPENAS_LINKS",
                "baseSelector": "div.blog-posts",
                "fields": [
                    {
                        "name": "page", 
                        "selector": "ul.pagination li.page-item.active", 
                        "type": "text"
                    },
                    {
                        "name": "news_items",
                        "selector": "article.post.post-medium",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "div.post-content a.text-decoration-none", "type": "attribute", "attribute": "title"},
                            {"name": "link", "selector": "div.post-content a.text-decoration-none", "type": "attribute", "attribute": "href"},
                        ]
                    }
                ]
            },
            "wait_for": "article.post.post-medium"
        },
        "detail": {
            "schema": {
                "name": "BAPPENAS_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "div.col-md-8 span", "type": "text"},
                    {"name": "text", "selector": "div.moskie", "type": "text"}
                ]
            },
            "wait_for": "div.moskie"
        }
    },
    "BGN": {
        "links": {
            "url_template": "https://www.bgn.go.id/news/siaran-pers/?page={page}",
            "schema": {
                "name": "BGN_LINKS",
                "baseSelector": "section.grid > div",
                "fields": [
                    {"name": "title", "selector": "a h3", "type": "text"},
                    {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                ]
            },
            "wait_for": "section.grid h3",
            "js_code": "window.scrollTo(0, 1000);"
        },
        "detail": {
            "schema": {
                "name": "BGN_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "h3.text-gray-500", "type": "text"},
                    {"name": "text", "selector": "section.prose", "type": "text"}
                ]
            },
            "wait_for": "section.prose"
        }
    },
    "ESDM": {
        "links": {
            "url_template": "https://www.esdm.go.id/id/media-center/siaran-pers?page={page}",
            "schema": {
                "name": "ESDM_LINKS",
                "baseSelector": "div.row.list-berita",
                "fields": [
                    {
                        "name": "page", 
                        "selector": "li.page.page-item.active a", 
                        "type": "text"
                    },
                    {
                        "name": "news_items",
                        "selector": "div.berita-item",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h4.title a", "type": "text"},
                            {"name": "link", "selector": "h4.title a", "type": "attribute", "attribute": "href"},
                        ]
                    }
                ]
            },
            "wait_for": "div.berita-item"
        },
        "detail": {
            "schema": {
                "name": "ESDM_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "div.date.mb-3 small", "type": "text"},
                    {"name": "text", "selector": "div.news-read", "type": "text"}
                ]
            },
            "wait_for": "div.news-read"
        }
    }
}

SCRAPER_CONFIG = {
    "max_pages": 1,
    "max_consecutive_empty": 1,
    "concurrency_limit": 5,
    "polite_delay": 0.5,
    "wait_timeout": 30000
}

OUTPUT_LINKS_FILE = str(DB_ROOT / "siaran_pers_general_links.json")
OUTPUT_CONTENT_FILE = str(DB_ROOT / "siaran_pers_general.json")
