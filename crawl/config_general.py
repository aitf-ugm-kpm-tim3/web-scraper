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
    },
    "BUMN": {
        "links": {
            "url_template": "https://www.bumn.go.id/publikasi/berita/rilis?page={page}",
            "schema": {
                "name": "BUMN_LINKS",
                "baseSelector": "body",
                "fields": [
                    {
                        "name": "page", 
                        "selector": "div.content-pagination a.active", 
                        "type": "text"
                    },
                    {
                        "name": "news_items",
                        "selector": "div.content_rilis",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "label", "type": "text"},
                            {"name": "link", "selector": "a.all_read", "type": "attribute", "attribute": "href"},
                        ]
                    }
                ]
            },
            "wait_for": "div.data_content"
        },
        "detail": {
            "schema": {
                "name": "BUMN_DETAIL",
                "baseSelector": "section.content_rilis",
                "fields": [
                    {"name": "date", "selector": "div.date span", "type": "text"},
                    {"name": "text", "selector": "div.informasi", "type": "text"}
                ]
            },
            "wait_for": "div.informasi"
        }
    },
    "PU": {
        "links": {
            "url_template": "https://pu.go.id/berita/kanal?page={page}",
            "schema": {
                "name": "PU_LINKS",
                "baseSelector": "div.col-md-8.col-lg-8.order-1.mb-5.mb-md-0",
                "fields": [
                    {
                        "name": "page", 
                        "selector": "ul.pagination li.active", 
                        "type": "text"
                    },
                    {
                        "name": "news_items",
                        "selector": "article",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "a h2", "type": "text"},
                            {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                        ]
                    }
                ]
            },
            "wait_for": "article"
        },
        "detail": {
            "schema": {
                "name": "PU_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "article.blog-post span.post-date", "type": "text"},
                    {"name": "text", "selector": "article.blog-post", "type": "text"}
                ]
            },
            "wait_for": "article.blog-post"
        }
    },
    "EKON": {
        "links": {
            "url_template": "https://www.ekon.go.id/publikasi/1/siaran-pers?page={page}",
            "schema": {
                "name": "EKON_LINKS",
                "baseSelector": "body",
                "fields": [
                    {
                        "name": "page", 
                        "selector": "ul.pagination li.active", 
                        "type": "text"
                    },
                    {
                        "name": "news_items",
                        "selector": "div.row.m-0.mb-3",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "div.col-md-8 a", "type": "text"},
                            {"name": "link", "selector": "div.col-md-8 a", "type": "attribute", "attribute": "href"},
                        ]
                    }
                ]
            },
            "wait_for": "div.row.m-0.mb-3"
        },
        "detail": {
            "schema": {
                "name": "EKON_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "span.mb-3", "type": "text"},
                    {"name": "text", "selector": "div.col-12.mt-3.p-0", "type": "text"}
                ]
            },
            "wait_for": "div.col-12.mt-3.p-0"
        }
    },
    "KEMENSOS": {
        "links": {
            "url_template": "https://kemensos.go.id/berita-terkini/{page}",
            "schema": {
                "name": "KEMENSOS_LINKS",
                "baseSelector": "body",
                "fields": [
                    {
                        "name": "page", 
                        "selector": "ul.pagination li.active", 
                        "type": "text"
                    },
                    {
                        "name": "news_items",
                        "selector": "div.col-lg-4.col-md-12.pb-5",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "a", "type": "text"},
                            {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                        ]
                    }
                ]
            },
            "wait_for": "div.col-lg-4.col-md-12.pb-5"
        },
        "detail": {
            "schema": {
                "name": "KEMENSOS_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "div.container.mt-3.mb-2", "type": "text"},
                    {"name": "text", "selector": "h5.container.text-content.text-justify", "type": "text"}
                ]
            },
            "wait_for": "h5.container.text-content.text-justify"
        }
    }
}

SCRAPER_CONFIG = {
    "max_pages": 50,
    "max_consecutive_empty": 1,
    "concurrency_limit": 5,
    "polite_delay": 0.5,
    "wait_timeout": 30000
}

OUTPUT_LINKS_FILE = str(DB_ROOT / "siaran_pers_general_links.json")
OUTPUT_CONTENT_FILE = str(DB_ROOT / "siaran_pers_general.json")
