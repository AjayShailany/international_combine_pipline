# countries/japan.py
"""
Japan – PMDA (Pharmaceuticals and Medical Devices Agency)
Scrapes guidance documents from:
https://www.pmda.go.jp/english/review-services/regulatory-info/0021.html
"""

import re
import logging
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ----------------------------------------------------------------------
# Helper: Clean title by removing file size indicators like [123KB]
# ----------------------------------------------------------------------
def _clean_title(text: str) -> str:
    return re.sub(r"\[\s*\d+(\.\d+)?\s*(KB|MB|kB|mb|Kb|Mb)\s*\]", "", text).strip()


# ----------------------------------------------------------------------
# Helper: Convert various English date formats → YYYY-MM-DD
# ----------------------------------------------------------------------
def _parse_date(date_text: str) -> str:
    if not date_text:
        return datetime.now().strftime("%Y-%m-%d")

    date_text = date_text.replace("(1)", "").strip()

    formats = [
        "%B %d, %Y",   # July 31, 2017
        "%B %d, %Y",
        "%B, %Y",      # August, 2024 → first day of month
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_text, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Fallback: if only year/month, use 1st of month
    try:
        dt = datetime.strptime(date_text, "%B, %Y")
        return dt.strftime("%Y-%m-01")
    except:
        pass

    return datetime.now().strftime("%Y-%m-%d")


# ----------------------------------------------------------------------
# Main scraper function – MUST be named scrape_data
# ----------------------------------------------------------------------
def scrape_data(config, logger: logging.Logger):
    """
    Scrape PMDA English guidance documents (Medical Devices & IVDs).

    Required config keys:
        - url
        - docket_prefix
        - document_type
        - agency_id
        - program_id
        - s3_country_folder
        - agency_sub
        - max_title_length (optional)
    """
    url = config.get("url") or "https://www.pmda.go.jp/english/review-services/regulatory-info/0021.html"
    base_url = "https://www.pmda.go.jp"
    logger.info(f"Scraping Japan – PMDA ({url})")

    items = []

    # ------------------------------------------------------------------
    # Session with retry & proper headers
    # ------------------------------------------------------------------
    session = requests.Session()
    retry = Retry(total=6, backoff_factor=1.5,
                  status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })

    try:
        resp = session.get(url, timeout=40)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch PMDA page: {e}")
        return items

    soup = BeautifulSoup(resp.text, "html.parser")

    # ------------------------------------------------------------------
    # Find all PDF links inside table rows
    # ------------------------------------------------------------------
    pdf_links = soup.select("a[href$='.pdf']")

    if not pdf_links:
        logger.warning("No PDF links found on PMDA guidance page")
        return items

    logger.info(f"Found {len(pdf_links)} PDF document(s)")

    for idx, link in enumerate(pdf_links, start=1):
        try:
            raw_title = link.get_text(strip=True)
            title = _clean_title(raw_title)
            if not title:
                title = "PMDA Guidance Document"

            # Build absolute PDF URL
            pdf_href = link["href"]
            pdf_url = urljoin(base_url, pdf_href)

            # Extract date from the next <td>
            date_str = ""
            tr = link.find_parent("tr")
            if tr:
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    date_str = tds[1].get_text(strip=True)

            publish_date = _parse_date(date_str)

            # Truncate title if needed
            max_len = config.get("max_title_length", 250)
            if len(title) > max_len:
                title = title[: max_len - 3] + "..."

            # ----------------------------------------------------------
            # Updated: Use PDF URL as main URL (your requirement)
            # ----------------------------------------------------------
            item = {
                "title": title,
                "url": pdf_url,                  # direct document URL
                "download_link": pdf_url,
                "doc_format": "PDF",
                "file_extension": "pdf",
                "publish_date": publish_date,
                "modify_date": publish_date,
                "abstract": f"PMDA guidance document: {title}",
                "atom_id": pdf_url,
            }

            items.append(item)
            logger.info(f"[{idx}] {title[:80]}... | {publish_date}")

        except Exception as e:
            logger.warning(f"Error processing link {idx}: {e}")
            continue

    logger.info(f"Japan (PMDA) scraping completed – {len(items)} documents collected")
    return items










# # countries/japan.py woking good 
# """
# Japan – PMDA (Pharmaceuticals and Medical Devices Agency)
# Scrapes guidance documents from:
# https://www.pmda.go.jp/english/review-services/regulatory-info/0021.html
# """

# import re
# import logging
# from datetime import datetime
# from urllib.parse import urljoin

# import requests
# from bs4 import BeautifulSoup
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ----------------------------------------------------------------------
# # Helper: Clean title by removing file size indicators like [123KB]
# # ----------------------------------------------------------------------
# def _clean_title(text: str) -> str:
#     return re.sub(r"\[\s*\d+(\.\d+)?\s*(KB|MB|kB|mb|Kb|Mb)\s*\]", "", text).strip()


# # ----------------------------------------------------------------------
# # Helper: Convert various English date formats → YYYY-MM-DD
# # ----------------------------------------------------------------------
# def _parse_date(date_text: str) -> str:
#     if not date_text:
#         return datetime.now().strftime("%Y-%m-%d")

#     date_text = date_text.replace("(1)", "").strip()

#     formats = [
#         "%B %d, %Y",   # July 31, 2017
#         "%B %d, %Y",   # same
#         "%B, %Y",      # August, 2024 → first day of month
#     ]

#     for fmt in formats:
#         try:
#             dt = datetime.strptime(date_text, fmt)
#             return dt.strftime("%Y-%m-%d")
#         except ValueError:
#             continue

#     # Fallback: if only year/month, use 1st of month
#     try:
#         dt = datetime.strptime(date_text, "%B, %Y")
#         return dt.strftime("%Y-%m-01")
#     except:
#         pass

#     # If all fails, use today
#     return datetime.now().strftime("%Y-%m-%d")


# # ----------------------------------------------------------------------
# # Main scraper function – MUST be named scrape_data
# # ----------------------------------------------------------------------
# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape PMDA English guidance documents (Medical Devices & IVDs).

#     Required config keys in countries.json:
#         - url (or we'll hardcode the known one)
#         - docket_prefix      → e.g. "PMDA-MD"
#         - document_type
#         - agency_id
#         - program_id
#         - s3_country_folder → "JAPAN"
#         - agency_sub         → "PMDA"
#         - max_title_length   → optional
#     """
#     url = config.get("url") or "https://www.pmda.go.jp/english/review-services/regulatory-info/0021.html"
#     base_url = "https://www.pmda.go.jp"
#     logger.info(f"Scraping Japan – PMDA ({url})")

#     items = []

#     # ------------------------------------------------------------------
#     # Session with retry & proper headers
#     # ------------------------------------------------------------------
#     session = requests.Session()
#     retry = Retry(total=6, backoff_factor=1.5,
#                   status_forcelist=[429, 500, 502, 503, 504])
#     session.mount("https://", HTTPAdapter(max_retries=retry))
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                       "AppleWebKit/537.36 (KHTML, like Gecko) "
#                       "Chrome/130.0 Safari/537.36",
#         "Accept-Language": "en-US,en;q=0.9",
#     })

#     try:
#         resp = session.get(url, timeout=40)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch PMDA page: {e}")
#         return items

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # ------------------------------------------------------------------
#     # Find all PDF links inside table rows
#     # ------------------------------------------------------------------
#     pdf_links = soup.select("a[href$='.pdf']")

#     if not pdf_links:
#         logger.warning("No PDF links found on PMDA guidance page")
#         return items

#     logger.info(f"Found {len(pdf_links)} PDF document(s)")

#     for idx, link in enumerate(pdf_links, start=1):
#         try:
#             raw_title = link.get_text(strip=True)
#             title = _clean_title(raw_title)
#             if not title:
#                 title = "PMDA Guidance Document"

#             # Build absolute PDF URL
#             pdf_href = link["href"]
#             pdf_url = urljoin(base_url, pdf_href)

#             # Extract date from the next <td> (usually the second column)
#             date_str = ""
#             tr = link.find_parent("tr")
#             if tr:
#                 tds = tr.find_all("td")
#                 if len(tds) >= 2:
#                     date_str = tds[1].get_text(strip=True)

#             publish_date = _parse_date(date_str)

#             # Truncate title if needed
#             max_len = config.get("max_title_length", 250)
#             if len(title) > max_len:
#                 title = title[: max_len - 3] + "..."

#             item = {
#                 "title": title,
#                 "url": url,                          # source page
#                 "download_link": pdf_url,            # direct PDF
#                 "doc_format": "PDF",
#                 "file_extension": "pdf",
#                 "publish_date": None,
#                 "modify_date": None,
#                 "abstract": f"PMDA guidance document: {title}",
#                 "atom_id": pdf_url,                  # unique identifier for deduplication
#             }

#             items.append(item)
#             logger.info(f"[{idx}] {title[:80]}... | {publish_date}")

#         except Exception as e:
#             logger.warning(f"Error processing link {idx}: {e}")
#             continue

#     logger.info(f"Japan (PMDA) scraping completed – {len(items)} documents collected")
#     return items