# countries/hong_kong.py 
"""
Hong Kong – Medical Device Division (MDD)
Scrapes all guidance documents under MDACS (Medical Device Administrative Control System)
URL: https://www.mdd.gov.hk/en/useful-information/issued-documents-under-mdacs/index.html
"""

import re
import logging
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://www.mdd.gov.hk"


def _clean_title(raw_title: str) -> str:
    """Remove prefix like [GN-01], [TR-005], [COP-03] etc."""
    return re.sub(r'^\[[A-Za-z0-9\-]+\]\s*', '', raw_title).strip()


def scrape_data(config, logger: logging.Logger):
    """
    Scrape Hong Kong MDD guidance documents.
    Returns list of items compatible with the main pipeline.
    """
    url = config["url"]
    logger.info(f"Scraping Hong Kong – MDD ({url})")

    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch Hong Kong MDD page: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    items = []

    # Each document is inside div.item under div.fileListWrap
    for item in soup.select("div.fileListWrap div.item"):
        try:
            # Title is the first text node inside the first div
            first_div = item.find("div")
            if not first_div or not first_div.contents:
                continue

            raw_title = first_div.contents[0].get_text(strip=True)
            if not raw_title:
                continue

            clean_title = _clean_title(raw_title)

            # Find PDF link
            pdf_tag = item.select_one(".iconWrap a[href$='.pdf']")
            if not pdf_tag:
                logger.debug(f"No PDF link found for: {clean_title}")
                continue

            pdf_href = pdf_tag["href"]
            pdf_url = urljoin(BASE_URL, pdf_href)

            # Use PDF URL as unique atom_id to avoid duplicates
            atom_id = pdf_url

            document = {
                "title": clean_title[: config.get("max_title_length", 250)],
                "url": pdf_url,                       # <<<<<< FIXED: use PDF URL
                "download_link": pdf_url,             # direct PDF
                "doc_format": "PDF",
                "file_extension": "pdf",
                "publish_date": None,
                "modify_date": None,
                "abstract": f"Hong Kong MDD Guidance: {clean_title}",
                "atom_id": atom_id,                   # deduplication
            }

            items.append(document)
            logger.info(f"Found: {clean_title[:80]}...")

        except Exception as e:
            logger.warning(f"Error processing item: {e}")
            continue

    logger.info(f"Hong Kong scraping complete – {len(items)} documents found")
    return items











# # countries/hong_kong.py  working
# """  
# Hong Kong – Medical Device Division (MDD)
# Scrapes all guidance documents under MDACS (Medical Device Administrative Control System)
# URL: https://www.mdd.gov.hk/en/useful-information/issued-documents-under-mdacs/index.html
# """

# import re
# import logging
# from urllib.parse import urljoin

# import requests
# from bs4 import BeautifulSoup
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# BASE_URL = "https://www.mdd.gov.hk"


# def _clean_title(raw_title: str) -> str:
#     """Remove prefix like [GN-01], [TR-005], [COP-03] etc."""
#     return re.sub(r'^\[[A-Za-z0-9\-]+\]\s*', '', raw_title).strip()


# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape Hong Kong MDD guidance documents.
#     Returns list of items compatible with the main pipeline.
#     """
#     url = config["url"]
#     logger.info(f"Scraping Hong Kong – MDD ({url})")

#     session = requests.Session()
#     retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
#     session.mount("https://", HTTPAdapter(max_retries=retry))
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
#         "Accept-Language": "en-US,en;q=0.9",
#     })

#     try:
#         resp = session.get(url, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch Hong Kong MDD page: {e}")
#         return []

#     soup = BeautifulSoup(resp.text, "html.parser")
#     items = []

#     # Each document is inside div.item under div.fileListWrap
#     for item in soup.select("div.fileListWrap div.item"):
#         try:
#             # Title is the first text node inside the first div
#             first_div = item.find("div")
#             if not first_div or not first_div.contents:
#                 continue

#             raw_title = first_div.contents[0].get_text(strip=True)
#             if not raw_title:
#                 continue

#             clean_title = _clean_title(raw_title)

#             # Find PDF link
#             pdf_tag = item.select_one(".iconWrap a[href$='.pdf']")
#             if not pdf_tag:
#                 logger.debug(f"No PDF link found for: {clean_title}")
#                 continue

#             pdf_href = pdf_tag["href"]
#             pdf_url = urljoin(BASE_URL, pdf_href)

#             # Use PDF URL as unique atom_id to avoid duplicates
#             atom_id = pdf_url

#             document = {
#                 "title": clean_title[: config.get("max_title_length", 250)],
#                 "url": url,                              # page URL
#                 "download_link": pdf_url,                # direct PDF
#                 "doc_format": "PDF",
#                 "file_extension": "pdf",
#                 "publish_date": None,                    # no date on page → will use today or leave null
#                 "modify_date": None,
#                 "abstract": f"Hong Kong MDD Guidance: {clean_title}",
#                 "atom_id": atom_id,                      # critical for deduplication
#             }

#             items.append(document)
#             logger.info(f"Found: {clean_title[:80]}...")

#         except Exception as e:
#             logger.warning(f"Error processing item: {e}")
#             continue

#     logger.info(f"Hong Kong scraping complete – {len(items)} documents found")
#     return items