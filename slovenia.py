# countries/si.py 
"""
Slovenia – JAZMP (Agency for Medicinal Products and Medical Devices) scraper.
Fetches the JAZMP Guidelines page and extracts all PDF links.
"""

import os
import re
import logging
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ----------------------------------------------------------------------
# Helper – safe date fallback
# ----------------------------------------------------------------------
def _today_iso():
    return datetime.now().strftime("%Y-%m-%d")


# ----------------------------------------------------------------------
# Main entry point required by the pipeline
# ----------------------------------------------------------------------
def scrape_data(config, logger: logging.Logger):
    """
    Scrape JAZMP guidelines page for PDFs.

    Expected config keys (add to countries.json):
        - url                : page URL
        - docket_prefix      : e.g. "JAZMP-MD"
        - document_type      : int
        - agency_id          : int
        - program_id         : int
        - s3_country_folder  : folder name in S3
        - agency_sub         : sub-folder under country
        - max_title_length   : optional, default 250
    """
    base_url = config["url"]
    logger.info(f"Scraping Slovenia – JAZMP ({base_url})")

    items = []

    # ------------------------------------------------------------------
    # 1. Session with retry
    # ------------------------------------------------------------------
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "en-US,en;q=0.9,sl;q=0.8",
    })

    # ------------------------------------------------------------------
    # 2. Fetch page
    # ------------------------------------------------------------------
    try:
        resp = session.get(base_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch JAZMP page: {e}")
        return items

    soup = BeautifulSoup(resp.text, "html.parser")

    # ------------------------------------------------------------------
    # 3. Find all PDF links
    # ------------------------------------------------------------------
    pdf_links = soup.find_all("a", href=lambda h: h and h.lower().endswith(".pdf"))
    if not pdf_links:
        logger.warning("No PDFs found on JAZMP page")
        return items

    logger.info(f"Found {len(pdf_links)} PDF(s)")

    # ------------------------------------------------------------------
    # 4. Process each link
    # ------------------------------------------------------------------
    for idx, link in enumerate(pdf_links, start=1):
        try:
            title = link.get_text(strip=True)
            if not title:
                # Fallback: extract from URL
                filename = os.path.basename(link["href"].split("?")[0])
                title = re.sub(r"\.pdf$", "", filename, flags=re.I)
                title = re.sub(r"[_-]", " ", title)

            pdf_url = urljoin(base_url, link["href"])

            # Clean title
            clean_title = re.sub(r'[<>:"/\\|?*]', '_', title)
            clean_title = re.sub(r"\s+", " ", clean_title).strip()
            if len(clean_title) > 150:
                clean_title = clean_title[:150]

            # ------------------------------------------------------------------
            # IMPORTANT: Updated `url` to use the actual PDF link
            # ------------------------------------------------------------------
            items.append({
                "title": clean_title[: config.get("max_title_length", 250)],
                "url": pdf_url,                  # <-- FIXED (was base_url)
                "download_link": pdf_url,
                "doc_format": "PDF",
                "file_extension": "pdf",
                "publish_date": None,            # no date on page
                "modify_date": None,
                "abstract": f"JAZMP guideline: {clean_title}",
                "atom_id": pdf_url,              # unique ID
            })

            logger.info(f"[{idx}] {clean_title[:70]}...")

        except Exception as e:
            logger.warning(f"Error processing PDF link {idx}: {e}")
            continue

    logger.info(f"Slovenia scraping complete – {len(items)} valid documents")
    return items









# # countries/si.py wokring good 
# """ 
# Slovenia – JAZMP (Agency for Medicinal Products and Medical Devices) scraper.
# Fetches the JAZMP Guidelines page and extracts all PDF links.
# """

# import os
# import re
# import logging
# from datetime import datetime
# from urllib.parse import urljoin

# import requests
# from bs4 import BeautifulSoup
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ----------------------------------------------------------------------
# # Helper – safe date fallback
# # ----------------------------------------------------------------------
# def _today_iso():
#     return datetime.now().strftime("%Y-%m-%d")


# # ----------------------------------------------------------------------
# # Main entry point required by the pipeline
# # ----------------------------------------------------------------------
# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape JAZMP guidelines page for PDFs.

#     Expected config keys (add to countries.json):
#         - url                : page URL
#         - docket_prefix      : e.g. "JAZMP-MD"
#         - document_type      : int
#         - agency_id          : int
#         - program_id         : int
#         - s3_country_folder  : folder name in S3
#         - agency_sub         : sub-folder under country
#         - max_title_length   : optional, default 250
#     """
#     base_url = config["url"]
#     logger.info(f"Scraping Slovenia – JAZMP ({base_url})")

#     items = []

#     # ------------------------------------------------------------------
#     # 1. Session with retry
#     # ------------------------------------------------------------------
#     session = requests.Session()
#     retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
#     session.mount("https://", HTTPAdapter(max_retries=retry))
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
#         "Accept-Language": "en-US,en;q=0.9,sl;q=0.8",
#     })

#     # ------------------------------------------------------------------
#     # 2. Fetch page
#     # ------------------------------------------------------------------
#     try:
#         resp = session.get(base_url, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch JAZMP page: {e}")
#         return items

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # ------------------------------------------------------------------
#     # 3. Find all PDF links
#     # ------------------------------------------------------------------
#     pdf_links = soup.find_all("a", href=lambda h: h and h.lower().endswith(".pdf"))
#     if not pdf_links:
#         logger.warning("No PDFs found on JAZMP page")
#         return items

#     logger.info(f"Found {len(pdf_links)} PDF(s)")

#     # ------------------------------------------------------------------
#     # 4. Process each link
#     # ------------------------------------------------------------------
#     for idx, link in enumerate(pdf_links, start=1):
#         try:
#             title = link.get_text(strip=True)
#             if not title:
#                 # Fallback: extract from URL
#                 filename = os.path.basename(link["href"].split("?")[0])
#                 title = re.sub(r"\.pdf$", "", filename, flags=re.I)
#                 title = re.sub(r"[_-]", " ", title)

#             pdf_url = urljoin(base_url, link["href"])

#             # Clean title
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
#                 "publish_date": None,   # no date on page ->use today
#                 "modify_date": None,
#                 "abstract": f"JAZMP guideline: {clean_title}",
#                 "atom_id": pdf_url,             # unique ID
#             })

#             logger.info(f"[{idx}] {clean_title[:70]}...")

#         except Exception as e:
#             logger.warning(f"Error processing PDF link {idx}: {e}")
#             continue

#     logger.info(f"Slovenia scraping complete – {len(items)} valid documents")
#     return items