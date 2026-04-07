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
    },
    "POLRI": {
        "links": {
            "url_template": "https://humas.polri.go.id/news/all?page={page}",
            "schema": {
                "name": "POLRI_LINKS",
                "baseSelector": "div.bg-white.border-b-1",
                "fields": [
                    {
                        "name": "page", 
                        "selector": "nav", 
                        "type": "attribute", 
                        "attribute": "data-active-page"
                    },
                    {
                        "name": "news_items",
                        "selector": "section#content > a:nth-child(-n+9)",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "div.font-semibold.text-lg", "type": "text"},
                            {"name": "link", "type": "attribute", "attribute": "href"},
                        ]
                    }
                ]
            },
            "wait_for": "section#content"
        },
        "detail": {
            "schema": {
                "name": "POLRI_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "div.flex.flex-row.items-center.gap-1:nth-child(2) p", "type": "text"},
                    {"name": "text", "selector": "div.text-sm.text-justify.space-y-4", "type": "text"}
                ]
            },
            "wait_for": "div.text-sm.text-justify.space-y-4"
        }
    },
    "SETKAB": {
        "links": {
            "url_template": "https://setkab.go.id/category/berita/page/{page}/",
            "schema": {
                "name": "SETKAB_LINKS",
                "baseSelector": "body",
                "fields": [
                    {
                        "name": "page", 
                        "selector": "div.nav-links span.page-numbers.current", 
                        "type": "text"
                    },
                    {
                        "name": "news_items",
                        "selector": "article.card_search",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "a h2", "type": "text"},
                            {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                        ]
                    }
                ]
            },
            "wait_for": "article.card_search"
        },
        "detail": {
            "schema": {
                "name": "SETKAB_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "div.info.fl:nth-child(2)", "type": "text"},
                    {"name": "text", "selector": "div.reading_text", "type": "text"}
                ]
            },
            "wait_for": "div.reading_text"
        }
    },
    "KEMHAN": {
        "links": {
            "url_template": "https://www.kemhan.go.id/category/berita/page/{page}",
            "schema": {
                "name": "KEMHAN_LINKS",
                "baseSelector": "body",
                "fields": [
                    {
                        "name": "page", 
                        "selector": "div.paging ul li.active", 
                        "type": "text"
                    },
                    {
                        "name": "news_items",
                        "selector": "ul.news-index li",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h4", "type": "text"},
                            {"name": "link", "selector": "h4 a", "type": "attribute", "attribute": "href"},
                        ]
                    }
                ]
            },
            "wait_for": "ul.news-index"
        },
        "detail": {
            "schema": {
                "name": "KEMHAN_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "small", "type": "text"},
                    {"name": "text", "selector": "div.def-page.article", "type": "text"}
                ]
            },
            "wait_for": "div.def-page.article"
        }
    },
    "WAPRES": {
        "links": {
            "url_template": "https://www.wapresri.go.id/press-release/page/{page}/",
            "schema": {
                "name": "WAPRES_LINKS",
                "baseSelector": "body",
                "fields": [
                    {"name": "page", "selector": "span.page-numbers.current", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.post.type-post",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h3.title.page-title", "type": "text"},
                            {"name": "link", "selector": "h3.title.page-title a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "div.post.type-post",
        },
        "detail": {
            "schema": {
                "name": "WAPRES_DETAIL",
                "baseSelector": "div.content",
                "fields": [
                    {"name": "date", "selector": "div.post-article span.post-meta-date", "type": "text"},
                    {"name": "text", "selector": "div.post-article", "type": "text"},
                ],
            },
            "wait_for": "div.content",
        },
    },
    "PMK": {
        "links": {
            "url_template": "https://www.kemenkopmk.go.id/index.php/kolom/berita?page={page}",
            "schema": {
                "name": "PMK_LINKS",
                "baseSelector": "body",
                "fields": [
                    {"name": "page", "selector": "nav.pager ul li.is-active", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.item-post",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "div.post-title span", "type": "text"},
                            {"name": "link", "selector": "div.post-title a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "div.item-post",
        },
        "detail": {
            "schema": {
                "name": "PMK_DETAIL",
                "baseSelector": "div.article-detail div.block",
                "fields": [
                    {"name": "date", "selector": "span.post-created", "type": "text"},
                    {"name": "text", "selector": "div.post-content div.field", "type": "text"},
                ],
            },
            "wait_for": "span.post-created",
        },
    },
    "SETNEG_PERS": {
        "links": {
            "url_template": "https://www.setneg.go.id/listcontent/listberita/siaran_pers_kemensetneg/{page}0",
            "schema": {
                "name": "SETNEG_PERS_LINKS",
                "baseSelector": "body",
                "fields": [
                    {"name": "page", "selector": "div.page-content nav ul li.active", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.page-content div.media",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h4.media-heading a", "type": "text"},
                            {"name": "link", "selector": "h4.media-heading a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "div.media",
        },
        "detail": {
            "schema": {
                "name": "SETNEG_PERS_DETAIL",
                "baseSelector": "div.article-content",
                "fields": [
                    {"name": "date", "selector": "p.widget-metas", "type": "text"},
                    {"name": "text", "selector": "div.my-content-text", "type": "text"},
                ],
            },
            "wait_for": "div.my-content-text",
        },
    },
    "SETNEG_BERITA": {
        "links": {
            "url_template": "https://www.setneg.go.id/listcontent/listberita/berita_kemensetneg/{page}0",
            "schema": {
                "name": "SETNEG_BERITA_LINKS",
                "baseSelector": "body",
                "fields": [
                    {"name": "page", "selector": "div.page-content nav ul li.active", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.page-content div.media",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h4.media-heading a", "type": "text"},
                            {"name": "link", "selector": "h4.media-heading a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "div.media",
        },
        "detail": {
            "schema": {
                "name": "SETNEG_BERITA_DETAIL",
                "baseSelector": "div.article-content",
                "fields": [
                    {"name": "date", "selector": "p.widget-metas", "type": "text"},
                    {"name": "text", "selector": "div.my-content-text", "type": "text"},
                ],
            },
            "wait_for": "div.my-content-text",
        },
    },
    "SETNEG_PRESIDEN": {
        "links": {
            "url_template": "https://www.setneg.go.id/listcontent/listberita/berita_presiden_dan_pemerintah/{page}0",
            "schema": {
                "name": "SETNEG_PRESIDEN_LINKS",
                "baseSelector": "body",
                "fields": [
                    {"name": "page", "selector": "div.page-content nav ul li.active", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.page-content div.media",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h4.media-heading a", "type": "text"},
                            {"name": "link", "selector": "h4.media-heading a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "div.media",
        },
        "detail": {
            "schema": {
                "name": "SETNEG_PRESIDEN_DETAIL",
                "baseSelector": "div.article-content",
                "fields": [
                    {"name": "date", "selector": "p.widget-metas", "type": "text"},
                    {"name": "text", "selector": "div.my-content-text", "type": "text"},
                ],
            },
            "wait_for": "div.my-content-text",
        },
    },
    "SETNEG_WAPRES": {
        "links": {
            "url_template": "https://www.setneg.go.id/listcontent/listberita/berita_wakil_presiden/{page}0",
            "schema": {
                "name": "SETNEG_WAPRES_LINKS",
                "baseSelector": "body",
                "fields": [
                    {"name": "page", "selector": "div.page-content nav ul li.active", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.page-content div.media",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h4.media-heading a", "type": "text"},
                            {"name": "link", "selector": "h4.media-heading a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "div.media",
        },
        "detail": {
            "schema": {
                "name": "SETNEG_WAPRES_DETAIL",
                "baseSelector": "div.article-content",
                "fields": [
                    {"name": "date", "selector": "p.widget-metas", "type": "text"},
                    {"name": "text", "selector": "div.my-content-text", "type": "text"},
                ],
            },
            "wait_for": "div.my-content-text",
        },
    },
    "SETNEG_PIDATO": {
        "links": {
            "url_template": "https://www.setneg.go.id/listcontent/listberita/pidato_presiden/{page}0",
            "schema": {
                "name": "SETNEG_PIDATO_LINKS",
                "baseSelector": "body",
                "fields": [
                    {"name": "page", "selector": "div.page-content nav ul li.active", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.page-content div.media",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h4.media-heading a", "type": "text"},
                            {"name": "link", "selector": "h4.media-heading a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "div.media",
        },
        "detail": {
            "schema": {
                "name": "SETNEG_PIDATO_DETAIL",
                "baseSelector": "div.article-content",
                "fields": [
                    {"name": "date", "selector": "p.widget-metas", "type": "text"},
                    {"name": "text", "selector": "section", "type": "text"},
                ],
            },
            "wait_for": "p.widget-metas",
        },
    },
    "MPR": {
        "links": {
            "url_template": "https://www.mpr.go.id/berita?page={page}",
            "schema": {
                "name": "MPR_LINKS",
                "baseSelector": "div.pimpinan-layout",
                "fields": [
                    {"name": "page", "selector": "span.page-info strong", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.news-item",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h3", "type": "text"},
                            {"name": "link", "type": "attribute", "attribute": "onclick"},
                        ],
                    },
                ],
            },
            "wait_for": "div.news-item",
        },
        "detail": {
            "schema": {
                "name": "MPR_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "div.banner-meta", "type": "text"},
                    {"name": "text", "selector": "div.detail-card", "type": "text"},
                ],
            },
            "wait_for": "div.detail-card",
        },
    },
    "KEHUTANAN_PERS": {
        "links": {
            "url_template": "https://www.kehutanan.go.id/pers?page={page}",
            "schema": {
                "name": "KEHUTANAN_PERS_LINKS",
                "baseSelector": "div.main-container",
                "fields": [
                    {"name": "page", "selector": "a.border-green.text-green", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.grid.grid-cols-2 > div:nth-child(-n+12)",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h3", "type": "text"},
                            {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "a.border-green.text-green",
        },
        "detail": {
            "schema": {
                "name": "KEHUTANAN_PERS_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "p.text-gray-600.mb-4", "type": "text"},
                    {"name": "text", "selector": "div.prose.max-w-none", "type": "text"},
                ],
            },
            "wait_for": "div.prose.max-w-none",
        },
    },
    "KEHUTANAN_BERITA": {
        "links": {
            "url_template": "https://www.kehutanan.go.id/news?page={page}",
            "schema": {
                "name": "KEHUTANAN_BERITA_LINKS",
                "baseSelector": "div.main-container",
                "fields": [
                    {"name": "page", "selector": "a.border-green.text-green", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.grid.grid-cols-2 > div:nth-child(-n+12)",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h3", "type": "text"},
                            {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "a.border-green.text-green",
        },
        "detail": {
            "schema": {
                "name": "KEHUTANAN_BERITA_DETAIL",
                "baseSelector": "body",
                "fields": [
                    {"name": "date", "selector": "p.text-gray-600.mb-4", "type": "text"},
                    {"name": "text", "selector": "div.prose.max-w-none", "type": "text"},
                ],
            },
            "wait_for": "div.prose.max-w-none",
        },
    },
    "KEMKES": {
        "links": {
            "url_template": "https://www.kemkes.go.id/id/category/rilis-berita/{page}",
            "schema": {
                "name": "KEMKES_LINKS",
                "baseSelector": "div.content-wrapper",
                "fields": [
                    {"name": "page", "selector": "a.page-link.active", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "div.col-md-4.mb-4",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "h4", "type": "text"},
                            {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "a.page-link.active",
        },
        "detail": {
            "schema": {
                "name": "KEMKES_DETAIL",
                "baseSelector": "div.col-md-8.mb-4",
                "fields": [
                    {"name": "date", "selector": "time", "type": "text"},
                    {"name": "text", "selector": "div.isi-konten", "type": "text"},
                ],
            },
            "wait_for": "div.isi-konten",
        },
    },
    "KY": {
        "links": {
            "url_template": "https://komisiyudisial.go.id/frontend/pers_release?page={page}",
            "schema": {
                "name": "KY_LINKS",
                "baseSelector": "div.content",
                "fields": [
                    {"name": "page", "selector": "li.page-item.active", "type": "text"},
                    {
                        "name": "news_items",
                        "selector": "ul.latest-posts > li",
                        "type": "list",
                        "fields": [
                            {"name": "title", "selector": "a", "type": "text"},
                            {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                        ],
                    },
                ],
            },
            "wait_for": "div.content",
        },
        "detail": {
            "schema": {
                "name": "KY_DETAIL",
                "baseSelector": "section#main",
                "fields": [
                    {"name": "date", "selector": "footer.entry-meta", "type": "text"},
                    {"name": "text", "selector": "div.entry-content", "type": "text"},
                ],
            },
            "wait_for": "div.entry-content",
        },
    },
}

SCRAPER_CONFIG = {
    "max_pages": 3000,
    "max_consecutive_empty": 10,
    "concurrency_limit": 10,
    "polite_delay": 0.5,    
    "wait_timeout": 10000
}

OUTPUT_LINKS_FILE = str(DB_ROOT / "siaran_pers_general_links.json")
OUTPUT_CONTENT_FILE = str(DB_ROOT / "siaran_pers_general.json")
