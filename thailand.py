# countries/th.py chnage for url
"""
FDA THAI – Food and Drug Administration, Thailand
------------------------------------------------
Medical Devices Guidance Documents
URL (from countries.json): https://en.fda.moph.go.th/...
Pagination: ?ppp=20&page=N
"""

import logging
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup

from utils.file_helper import normalize_date, clean_title


BASE_URL = "https://en.fda.moph.go.th"


def _build_page_url(page: int = 1) -> str:
    """Create the full URL for a given page (ppp=20)."""
    params = {"ppp": 20, "page": page}
    return f"{BASE_URL}/cat2-health-products/category/health-products-medical-devices?{urlencode(params)}"


def scrape_data(cfg, logger: logging.Logger):
    """
    Scrape **all** pages of Thailand FDA medical-device guidance PDFs.

    Uses the `url` from `countries.json` as the *first* page.
    Stops when a page returns no rows or no PDFs.
    """
    logger.info("Scraping FDA THAI – Medical Devices Guidance")

    session = requests.Session()
    session.headers.update(
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    )

    items = []
    page = 1
    total_pdfs = 0

    while True:
        url = _build_page_url(page)
        logger.debug(f"Fetching page {page}: {url}")

        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Page {page} request failed: {e}")
            break

        soup = BeautifulSoup(resp.text, "lxml")
        rows = soup.select("table tbody tr")

        if not rows:
            logger.info(f"No rows on page {page} – ending pagination")
            break

        page_has_pdf = False
        for row_idx, row in enumerate(rows, 1):
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            # ---- Title (column 1) ----
            title = cols[1].get_text(strip=True).strip()
            if not title:
                continue

            # ---- PDF link (column 3) ----
            pdf_anchor = cols[3].select_one("a[href$='.pdf']")
            if not pdf_anchor:
                continue

            pdf_href = pdf_anchor.get("href")
            pdf_url = urljoin(BASE_URL, pdf_href)  # full media.php URL

            # ---- Date (column 0 – sometimes a number, sometimes a date) ----
            date_raw = cols[0].get_text(strip=True)
            publish_date = normalize_date(date_raw) or None
            modify_date = publish_date

            total_pdfs += 1
            page_has_pdf = True

            logger.info(f"[{total_pdfs}] {title[:70]}...")

            # -----------------------------
            # UPDATED: url is now pdf_url
            # -----------------------------
            items.append(
                {
                    "title": clean_title(title)[: cfg.get("max_title_length", 200)],
                    "url": pdf_url,                   # Updated field
                    "download_link": pdf_url,
                    "doc_format": "PDF",
                    "file_extension": "pdf",
                    "publish_date": publish_date,
                    "modify_date": modify_date,
                    "abstract": f"TFDA guidance: {title}",
                    "atom_id": pdf_url,               # unique for deduplication
                }
            )

        if not page_has_pdf:
            logger.info(f"No PDFs on page {page} – stopping")
            break

        page += 1

    logger.info(f"FDA THAI scraping complete – {len(items)} PDFs collected")
    return items











# # countries/th.py wokring good
# """ 
# FDA THAI – Food and Drug Administration, Thailand
# ------------------------------------------------
# Medical Devices Guidance Documents
# URL (from countries.json): https://en.fda.moph.go.th/...
# Pagination: ?ppp=20&page=N
# """

# import logging
# from urllib.parse import urljoin, urlencode

# import requests
# from bs4 import BeautifulSoup

# from utils.file_helper import normalize_date, clean_title


# BASE_URL = "https://en.fda.moph.go.th"


# def _build_page_url(page: int = 1) -> str:
#     """Create the full URL for a given page (ppp=20)."""
#     params = {"ppp": 20, "page": page}
#     return f"{BASE_URL}/cat2-health-products/category/health-products-medical-devices?{urlencode(params)}"


# def scrape_data(cfg, logger: logging.Logger):
#     """
#     Scrape **all** pages of Thailand FDA medical-device guidance PDFs.

#     Uses the `url` from `countries.json` as the *first* page.
#     Stops when a page returns no rows or no PDFs.
#     """
#     logger.info("Scraping FDA THAI – Medical Devices Guidance")

#     session = requests.Session()
#     session.headers.update(
#         {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
#     )

#     items = []
#     page = 1
#     total_pdfs = 0

#     while True:
#         url = _build_page_url(page)
#         logger.debug(f"Fetching page {page}: {url}")

#         try:
#             resp = session.get(url, timeout=30)
#             resp.raise_for_status()
#         except Exception as e:
#             logger.error(f"Page {page} request failed: {e}")
#             break

#         soup = BeautifulSoup(resp.text, "lxml")
#         rows = soup.select("table tbody tr")

#         if not rows:
#             logger.info(f"No rows on page {page} – ending pagination")
#             break

#         page_has_pdf = False
#         for row_idx, row in enumerate(rows, 1):
#             cols = row.find_all("td")
#             if len(cols) < 4:
#                 continue

#             # ---- Title (column 1) ----
#             title = cols[1].get_text(strip=True).strip()
#             if not title:
#                 continue

#             # ---- PDF link (column 3) ----
#             pdf_anchor = cols[3].select_one("a[href$='.pdf']")
#             if not pdf_anchor:
#                 continue

#             pdf_href = pdf_anchor.get("href")
#             pdf_url = urljoin(BASE_URL, pdf_href)   # full media.php URL

#             # ---- Date (column 0 – sometimes a number, sometimes a date) ----
#             date_raw = cols[0].get_text(strip=True)
#             publish_date = normalize_date(date_raw) or None
#             modify_date = publish_date

#             total_pdfs += 1
#             page_has_pdf = True

#             logger.info(f"[{total_pdfs}] {title[:70]}...")

#             items.append(
#                 {
#                     "title": clean_title(title)[: cfg.get("max_title_length", 200)],
#                     "url": url,                     # source page (for reference)
#                     "download_link": pdf_url,
#                     "doc_format": "PDF",
#                     "file_extension": "pdf",
#                     "publish_date": publish_date,
#                     "modify_date": modify_date,
#                     "abstract": f"TFDA guidance: {title}",
#                     "atom_id": pdf_url,             # unique for deduplication
#                 }
#             )

#         if not page_has_pdf:
#             logger.info(f"No PDFs on page {page} – stopping")
#             break

#         page += 1

#     logger.info(f"FDA THAI scraping complete – {len(items)} PDFs collected")
#     return items

    
