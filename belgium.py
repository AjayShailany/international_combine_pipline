# countries/be.py
"""
Belgium – AFMPS (Federal Agency for Medicines and Health Products) scraper.
Fetches the medical-devices guidance page and extracts all PDF links.
"""

import os
import re
import logging
from datetime import datetime
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ----------------------------------------------------------------------
# Helper – safe date parsing (DD/MM/YYYY -> YYYY-MM-DD)
# ----------------------------------------------------------------------
def _parse_date(date_str: str) -> str:
    """Convert '20/05/2025' -> '2025-05-20'. Fallback to today."""
    if not date_str or not date_str.strip():
        return datetime.now().strftime("%Y-%m-%d")

    try:
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return datetime.now().strftime("%Y-%m-%d")


# ----------------------------------------------------------------------
# Main entry point required by the pipeline
# ----------------------------------------------------------------------
def scrape_data(config, logger: logging.Logger):
    """
    Scrape AFMPS medical devices guidance page.

    Expected config keys (add to countries.json):
        - url                : page URL
        - docket_prefix      : e.g. "AFMPS-MD"
        - document_type      : int
        - agency_id          : int
        - program_id         : int
        - s3_country_folder  : folder name in S3
        - agency_sub         : sub-folder under country
        - max_title_length   : optional, default 250
    """
    base_url = config["url"]
    logger.info(f"Scraping Belgium – AFMPS ({base_url})")

    items = []

    # ------------------------------------------------------------------
    # 1. Session with retry
    # ------------------------------------------------------------------
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "fr,en;q=0.9",
    })

    # ------------------------------------------------------------------
    # 2. Fetch page
    # ------------------------------------------------------------------
    try:
        resp = session.get(base_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch AFMPS page: {e}")
        return items

    soup = BeautifulSoup(resp.text, "html.parser")

    # ------------------------------------------------------------------
    # 3. Extract page update date (optional)
    # ------------------------------------------------------------------
    date_elem = soup.find("time", class_="datetime")
    raw_date = date_elem.get_text(strip=True) if date_elem else None
    page_date = _parse_date(raw_date)

    # ------------------------------------------------------------------
    # 4. Find all PDF links
    # ------------------------------------------------------------------
    pdf_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            pdf_url = urljoin(base_url, href)
            title = a.get_text(strip=True)

            if not title:
                # Fallback: use filename from URL
                title = unquote(os.path.basename(href))
                title = re.sub(r"\.pdf$", "", title, flags=re.I)

            pdf_links.append((title.strip(), pdf_url))

    if not pdf_links:
        logger.warning("No PDFs found on AFMPS page")
        return items

    logger.info(f"Found {len(pdf_links)} PDF(s)")

    # ------------------------------------------------------------------
    # 5. Build pipeline-compatible items
    # ------------------------------------------------------------------
    for idx, (title, pdf_url) in enumerate(pdf_links, start=1):
        try:
            clean_title = re.sub(r'[<>:"/\\|?*]', '_', title)
            clean_title = re.sub(r"\s+", " ", clean_title).strip()
            if len(clean_title) > 150:
                clean_title = clean_title[:150]

            items.append({
                "title": clean_title[: config.get("max_title_length", 250)],
                "url": pdf_url,                     # <--- UPDATED HERE
                "download_link": pdf_url,
                "doc_format": "PDF",
                "file_extension": "pdf",
                "publish_date": page_date,
                "modify_date": page_date,
                "abstract": f"AFMPS guidance: {clean_title}",
                "atom_id": pdf_url,
            })

            logger.info(f"[{idx}] {clean_title[:70]}...")

        except Exception as e:
            logger.warning(f"Error processing PDF link: {e}")
            continue

    logger.info(f"Belgium scraping complete – {len(items)} valid documents")
    return items






# # countries/be.py  working 
# """
# Belgium – AFMPS (Federal Agency for Medicines and Health Products) scraper.
# Fetches the medical-devices guidance page and extracts all PDF links.
# """

# import os
# import re
# import logging
# from datetime import datetime
# from urllib.parse import urljoin, unquote

# import requests
# from bs4 import BeautifulSoup
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ----------------------------------------------------------------------
# # Helper – safe date parsing (DD/MM/YYYY ->YYYY-MM-DD)
# # ----------------------------------------------------------------------
# def _parse_date(date_str: str) -> str:
#     """Convert '20/05/2025' ->'2025-05-20'. Fallback to today."""
#     if not date_str or not date_str.strip():
#         return datetime.now().strftime("%Y-%m-%d")

#     try:
#         dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
#         return dt.strftime("%Y-%m-%d")
#     except ValueError:
#         return datetime.now().strftime("%Y-%m-%d")


# # ----------------------------------------------------------------------
# # Main entry point required by the pipeline
# # ----------------------------------------------------------------------
# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape AFMPS medical devices guidance page.

#     Expected config keys (add to countries.json):
#         - url                : page URL
#         - docket_prefix      : e.g. "AFMPS-MD"
#         - document_type      : int
#         - agency_id          : int
#         - program_id         : int
#         - s3_country_folder  : folder name in S3
#         - agency_sub         : sub-folder under country
#         - max_title_length   : optional, default 250
#     """
#     base_url = config["url"]
#     logger.info(f"Scraping Belgium – AFMPS ({base_url})")

#     items = []

#     # ------------------------------------------------------------------
#     # 1. Session with retry
#     # ------------------------------------------------------------------
#     session = requests.Session()
#     retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
#     session.mount("https://", HTTPAdapter(max_retries=retry))
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
#         "Accept-Language": "fr,en;q=0.9",
#     })

#     # ------------------------------------------------------------------
#     # 2. Fetch page
#     # ------------------------------------------------------------------
#     try:
#         resp = session.get(base_url, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch AFMPS page: {e}")
#         return items

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # ------------------------------------------------------------------
#     # 3. Extract page update date (optional)
#     # ------------------------------------------------------------------
#     date_elem = soup.find("time", class_="datetime")
#     raw_date = date_elem.get_text(strip=True) if date_elem else None
#     page_date = _parse_date(raw_date)

#     # ------------------------------------------------------------------
#     # 4. Find all PDF links
#     # ------------------------------------------------------------------
#     pdf_links = []
#     for a in soup.find_all("a", href=True):
#         href = a["href"]
#         if href.lower().endswith(".pdf"):
#             pdf_url = urljoin(base_url, href)
#             title = a.get_text(strip=True)
#             if not title:
#                 # Fallback: use filename from URL
#                 title = unquote(os.path.basename(href))
#                 title = re.sub(r"\.pdf$", "", title, flags=re.I)
#             pdf_links.append((title.strip(), pdf_url))

#     if not pdf_links:
#         logger.warning("No PDFs found on AFMPS page")
#         return items

#     logger.info(f"Found {len(pdf_links)} PDF(s)")

#     # ------------------------------------------------------------------
#     # 5. Build pipeline-compatible items
#     # ------------------------------------------------------------------
#     for idx, (title, pdf_url) in enumerate(pdf_links, start=1):
#         try:
#             clean_title = re.sub(r'[<>:"/\\|?*]', '_', title)
#             clean_title = re.sub(r"\s+", " ", clean_title).strip()
#             if len(clean_title) > 150:
#                 clean_title = clean_title[:150]

#             items.append({
#                 "title": clean_title[: config.get("max_title_length", 250)],
#                 "url": base_url,
#                 "download_link": pdf_url,
#                 "doc_format": "PDF",
#                 "file_extension": "pdf",
#                 "publish_date": page_date,
#                 "modify_date": page_date,
#                 "abstract": f"AFMPS guidance: {clean_title}",
#                 "atom_id": pdf_url,  # unique identifier
#             })

#             logger.info(f"[{idx}] {clean_title[:70]}...")

#         except Exception as e:
#             logger.warning(f"Error processing PDF link: {e}")
#             continue

#     logger.info(f"Belgium scraping complete – {len(items)} valid documents")
#     return items