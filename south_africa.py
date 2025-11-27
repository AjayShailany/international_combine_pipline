# countries/za.py
"""
South Africa – SAHPRA Medical Devices & IVD Guidelines scraper.
Drops into the existing pipeline (run.py -> countries.za -> scrape_data).
"""

import os
import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging
from datetime import datetime


# ----------------------------------------------------------------------
# Helper – safe date parsing (falls back to today)
# ----------------------------------------------------------------------
def _parse_date(date_str: str):
    """Try a few common formats; return YYYY-MM-DD or today."""
    if not date_str or not date_str.strip():
        return datetime.now().strftime("%Y-%m-%d")

    date_str = date_str.strip()
    # SAHPRA table uses DD/MM/YYYY
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # fallback
    return datetime.now().strftime("%Y-%m-%d")


# ----------------------------------------------------------------------
# Main entry point required by the pipeline
# ----------------------------------------------------------------------
def scrape_data(config, logger: logging.Logger):
    """
    Scrape the SAHPRA guidelines table and return a list of dicts.

    Expected config keys (add them to countries.json):
        - url                : page URL
        - docket_prefix      : e.g. "SAHPRA-MD"    (not used here but in pipeline)
        - document_type      : int (pipeline code)
        - agency_id          : int
        - program_id         : int
        - s3_country_folder  : folder name in S3
        - agency_sub         : sub-folder under country
        - max_title_length   : optional, default 250
    """
    base_url = config["url"]
    logger.info(f"Scraping South Africa – SAHPRA ({base_url})")

    items = []

    # ------------------------------------------------------------------
    # 1. Fetch page
    # ------------------------------------------------------------------
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        }
    )
    try:
        resp = session.get(base_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch SAHPRA page: {e}")
        return items

    soup = BeautifulSoup(resp.text, "html.parser")

    # ------------------------------------------------------------------
    # 2. Locate the document table (id contains "dlp_")
    # ------------------------------------------------------------------
    table = soup.find("table", {"id": lambda x: x and "dlp_" in x})
    if not table:
        logger.error("Could not locate document table (id with 'dlp_')")
        return items

    tbody = table.find("tbody")
    if tbody:
        rows = tbody.find_all("tr")
    else:
        # fallback: use all rows except header
        all_rows = table.find_all("tr")
        rows = all_rows[1:] if len(all_rows) > 1 else []

    logger.info(f"Found {len(rows)} rows in the table")

    # ------------------------------------------------------------------
    # 3. Process rows
    # ------------------------------------------------------------------
    for idx, row in enumerate(rows, start=1):
        try:
            cols = row.find_all("td")
            # Expect at least up to "Units" + "Link"
            if len(cols) < 7:
                logger.debug(f"Row {idx} has only {len(cols)} columns, skipping")
                continue

            doc_number   = cols[0].get_text(strip=True)
            title        = cols[1].get_text(strip=True)
            category     = cols[2].get_text(strip=True)
            date_updated = cols[3].get_text(strip=True)
            version      = cols[4].get_text(strip=True)
            unit         = cols[5].get_text(strip=True)

            # ------------------------------------------------------------------
            # Download link (absolute)
            # Column 6/7 usually contains the "Download" anchor
            # ------------------------------------------------------------------
            link_tag = cols[6].find("a", href=True) if len(cols) >= 7 else None
            if not link_tag:
                # Extra safety: search in entire row for "Download" anchor
                link_tag = row.find("a", href=True, string=lambda t: t and "Download" in t)

            if not link_tag:
                logger.info(f"[{idx}] No download link found, skipping row")
                continue

            download_link = urljoin(base_url, link_tag["href"])

            # ------------------------------------------------------------------
            # Determine file extension from the href
            # ------------------------------------------------------------------
            href_path = link_tag["href"].split("?", 1)[0].split("#", 1)[0]
            ext = ""
            if "." in href_path:
                ext = href_path.rsplit(".", 1)[1].lower()

            if ext not in {"pdf", "doc", "docx"}:
                logger.info(
                    f"[{idx}] Unsupported or missing extension or document might be archived '{ext}' "
                    f"for title: {title[:70]}"
                )
                continue

            fmt = ext.upper()

            # ------------------------------------------------------------------
            # Build the dict expected by the pipeline
            # ------------------------------------------------------------------
            item = {
                "title": title[: config.get("max_title_length", 250)],
                "url": base_url,
                "download_link": download_link,
                "doc_format": fmt,
                "file_extension": ext,
                "publish_date": _parse_date(date_updated),
                "modify_date": _parse_date(date_updated),  # same as publish for now
                "abstract": f"SAHPRA {category}: {title}",
                "atom_id": download_link,  # unique identifier
                # optional meta that may be useful later
                "doc_number": doc_number,
                "version": version,
                "unit": unit,
            }

            items.append(item)
            logger.info(f"[{idx}] {title[:70]}... | {date_updated} | {ext}")

        except Exception as e:
            logger.warning(f"Row {idx} processing error: {e}")
            continue

    logger.info(f"South Africa scraping complete – {len(items)} valid documents")
    return items












# """
# South Africa – SAHPRA Medical Devices & IVD Guidelines scraper.
# Drops into the existing pipeline (run.py -> countries.za -> scrape_data).
# """

# import os
# import requests
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# import logging
# from datetime import datetime


# def _parse_date(date_str: str):
#     """Try common formats used by SAHPRA (mostly DD/MM/YYYY)."""
#     if not date_str or not date_str.strip():
#         return datetime.now().strftime("%Y-%m-%d")

#     date_str = date_str.strip()
#     for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y", "%B %d, %Y"):
#         try:
#             return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
#         except ValueError:
#             continue
#     return datetime.now().strftime("%Y-%m-%d")


# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape SAHPRA Medical Devices & IVD Guidelines table.
#     """
#     base_url = config["url"]
#     logger.info(f"Scraping South Africa – SAHPRA ({base_url})")

#     items = []

#     session = requests.Session()
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0 Safari/537.36"
#     })

#     try:
#         resp = session.get(base_url, timeout=40)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch SAHPRA page: {e}")
#         return items

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # Find table with id containing "dlp_" (dynamic ID like dlp_abc123)
#     table = soup.find("table", {"id": lambda x: x and "dlp_" in x})
#     if not table:
#         logger.error("Could not find document table (id contains 'dlp_')")
#         return items

#     rows = table.find_all("tr")
#     if not rows:
#         logger.warning("No rows found in SAHPRA table")
#         return items

#     logger.info(f"Found {len(rows)} rows (including header)")

#     for idx, row in enumerate(rows):
#         cols = row.find_all("td")
#         if len(cols) < 8:
#             continue  # skip header or malformed rows

#         try:
#             doc_number   = cols[0].get_text(strip=True)
#             title        = cols[1].get_text(strip=True)
#             category     = cols[2].get_text(strip=True)
#             date_updated = cols[3].get_text(strip=True)
#             version      = cols[4].get_text(strip=True)
#             unit         = cols[5].get_text(strip=True)
#             file_type    = cols[6].get_text(strip=True).lower().strip()

#             link_tag = cols[7].find("a", href=True)
#             if not link_tag or not link_tag.get("href"):
#                 continue

#             download_link = urljoin(base_url, link_tag["href"])

#             if file_type not in {"pdf", "doc", "docx"}:
#                 continue

#             ext = "pdf" if file_type == "pdf" else "docx" if file_type in ("docx", "doc") else file_type
#             fmt = ext.upper()

#             clean_title = title.strip()
#             max_len = config.get("max_title_length", 250)
#             if len(clean_title) > max_len:
#                 clean_title = clean_title[:max_len-3] + "..."

#             item = {
#                 "title": clean_title,
#                 "url": base_url,
#                 "download_link": download_link,
#                 "doc_format": fmt,
#                 "file_extension": ext,
#                 "publish_date": _parse_date(date_updated),
#                 "modify_date": _parse_date(date_updated),
#                 "abstract": f"SAHPRA {category}: {clean_title}",
#                 "atom_id": download_link,
#                 "doc_number": doc_number,
#                 "version": version,
#                 "unit": unit,
#             }

#             items.append(item)
#             logger.info(f"[{idx}] {clean_title[:70]} | {date_updated} | {fmt}")

#         except Exception as e:
#             logger.warning(f"Error processing row {idx}: {e}")
#             continue

#     logger.info(f"South Africa SAHPRA scraping complete → {len(items)} documents")
#     return items








# # countries/za.py abhi
# """
# South Africa – SAHPRA Medical Devices & IVD Guidelines scraper.
# Drops into the existing pipeline (run.py ->countries.za ->scrape_data).
# """

# import os
# import csv
# import requests
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# import logging
# from datetime import datetime

# # ----------------------------------------------------------------------
# # Helper – safe date parsing (falls back to today)
# # ----------------------------------------------------------------------
# def _parse_date(date_str: str):
#     """Try a few common formats; return YYYY-MM-DD or today."""
#     if not date_str or not date_str.strip():
#         return datetime.now().strftime("%Y-%m-%d")

#     date_str = date_str.strip()
#     # SAHPRA table uses DD/MM/YYYY
#     for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y"):
#         try:
#             return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
#         except ValueError:
#             continue
#     # fallback
#     return datetime.now().strftime("%Y-%m-%d")


# # ----------------------------------------------------------------------
# # Main entry point required by the pipeline
# # ----------------------------------------------------------------------
# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape the SAHPRA guidelines table and return a list of dicts.

#     Expected config keys (add them to countries.json):
#         - url                : page URL
#         - docket_prefix      : e.g. "SAHPRA-MD"
#         - document_type      : int (pipeline code)
#         - agency_id          : int
#         - program_id         : int
#         - s3_country_folder  : folder name in S3
#         - agency_sub         : sub-folder under country
#         - max_title_length   : optional, default 250
#     """
#     base_url = config["url"]
#     logger.info(f"Scraping South Africa – SAHPRA ({base_url})")

#     items = []

#     # ------------------------------------------------------------------
#     # 1. Fetch page
#     # ------------------------------------------------------------------
#     session = requests.Session()
#     session.headers.update(
#         {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
#     )
#     try:
#         resp = session.get(base_url, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch SAHPRA page: {e}")
#         return items

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # ------------------------------------------------------------------
#     # 2. Locate the document table (id contains "dlp_")
#     # ------------------------------------------------------------------
#     table = soup.find("table", {"id": lambda x: x and "dlp_" in x})
#     if not table:
#         logger.error("Could not locate document table (id with 'dlp_')")
#         return items

#     rows = table.find("tbody").find_all("tr")
#     logger.info(f"Found {len(rows)} rows in the table")

#     # ------------------------------------------------------------------
#     # 3. Process rows
#     # ------------------------------------------------------------------
#     for idx, row in enumerate(rows, start=1):
#         try:
#             cols = row.find_all("td")
#             if len(cols) < 8:
#                 continue

#             doc_number   = cols[0].get_text(strip=True)
#             title        = cols[1].get_text(strip=True)
#             category     = cols[2].get_text(strip=True)
#             date_updated = cols[3].get_text(strip=True)
#             version      = cols[4].get_text(strip=True)
#             unit         = cols[5].get_text(strip=True)
#             file_type    = cols[6].get_text(strip=True).lower()

#             # Download link (absolute)
#             link_tag = cols[7].find("a", href=True)
#             download_link = urljoin(base_url, link_tag["href"]) if link_tag else ""

#             # ------------------------------------------------------------------
#             # Keep only PDF/DOC/DOCX – the pipeline expects file_extension
#             # ------------------------------------------------------------------
#             if file_type not in {"pdf", "doc", "docx"}:
#                 continue

#             ext = file_type
#             fmt = ext.upper()

#             # ------------------------------------------------------------------
#             # Build the dict expected by the pipeline
#             # ------------------------------------------------------------------
#             items.append(
#                 {
#                     "title": title[: config.get("max_title_length", 250)],
#                     "url": base_url,
#                     "download_link": download_link,
#                     "doc_format": fmt,
#                     "file_extension": ext,
#                     "publish_date": _parse_date(date_updated),
#                     "modify_date": _parse_date(date_updated),   # same as publish
#                     "abstract": f"SAHPRA {category}: {title}",
#                     "atom_id": download_link,                  # unique identifier
#                     # optional meta that may be useful later
#                     "doc_number": doc_number,
#                     "version": version,
#                     "unit": unit,
#                 }
#             )

#             logger.info(f"[{idx}] {title[:70]}... | {date_updated}")

#         except Exception as e:
#             logger.warning(f"Row {idx} processing error: {e}")
#             continue

#     logger.info(f"South Africa scraping complete – {len(items)} valid documents")
#     return items










# # countries/za.py wokring good 25/11 not wokring 
# """
# South Africa – SAHPRA Medical Devices & IVD Guidelines scraper.
# Drops into the existing pipeline (run.py ->countries.za ->scrape_data).
# """

# import os
# import csv
# import requests
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# import logging
# from datetime import datetime

# # ----------------------------------------------------------------------
# # Helper – safe date parsing (falls back to today)
# # ----------------------------------------------------------------------
# def _parse_date(date_str: str):
#     """Try a few common formats; return YYYY-MM-DD or today."""
#     if not date_str or not date_str.strip():
#         return datetime.now().strftime("%Y-%m-%d")

#     date_str = date_str.strip()
#     # SAHPRA table uses DD/MM/YYYY
#     for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y"):
#         try:
#             return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
#         except ValueError:
#             continue
#     # fallback
#     return datetime.now().strftime("%Y-%m-%d")


# # ----------------------------------------------------------------------
# # Main entry point required by the pipeline
# # ----------------------------------------------------------------------
# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape the SAHPRA guidelines table and return a list of dicts.

#     Expected config keys (add them to countries.json):
#         - url                : page URL
#         - docket_prefix      : e.g. "SAHPRA-MD"
#         - document_type      : int (pipeline code)
#         - agency_id          : int
#         - program_id         : int
#         - s3_country_folder  : folder name in S3
#         - agency_sub         : sub-folder under country
#         - max_title_length   : optional, default 250
#     """
#     base_url = config["url"]
#     logger.info(f"Scraping South Africa – SAHPRA ({base_url})")

#     items = []

#     # ------------------------------------------------------------------
#     # 1. Fetch page
#     # ------------------------------------------------------------------
#     session = requests.Session()
#     session.headers.update(
#         {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
#     )
#     try:
#         resp = session.get(base_url, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch SAHPRA page: {e}")
#         return items

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # ------------------------------------------------------------------
#     # 2. Locate the document table (id contains "dlp_")
#     # ------------------------------------------------------------------
#     table = soup.find("table", {"id": lambda x: x and "dlp_" in x})
#     if not table:
#         logger.error("Could not locate document table (id with 'dlp_')")
#         return items

#     rows = table.find("tbody").find_all("tr")
#     logger.info(f"Found {len(rows)} rows in the table")

#     # ------------------------------------------------------------------
#     # 3. Process rows
#     # ------------------------------------------------------------------
#     for idx, row in enumerate(rows, start=1):
#         try:
#             cols = row.find_all("td")
#             if len(cols) < 8:
#                 continue

#             doc_number   = cols[0].get_text(strip=True)
#             title        = cols[1].get_text(strip=True)
#             category     = cols[2].get_text(strip=True)
#             date_updated = cols[3].get_text(strip=True)
#             version      = cols[4].get_text(strip=True)
#             unit         = cols[5].get_text(strip=True)
#             file_type    = cols[6].get_text(strip=True).lower()

#             # Download link (absolute)
#             link_tag = cols[7].find("a", href=True)
#             download_link = urljoin(base_url, link_tag["href"]) if link_tag else ""

#             # ------------------------------------------------------------------
#             # Keep only PDF/DOC/DOCX – the pipeline expects file_extension
#             # ------------------------------------------------------------------
#             if file_type not in {"pdf", "doc", "docx"}:
#                 continue

#             ext = file_type
#             fmt = ext.upper()

#             # ------------------------------------------------------------------
#             # Build the dict expected by the pipeline
#             # ------------------------------------------------------------------
#             items.append(
#                 {
#                     "title": title[: config.get("max_title_length", 250)],
#                     "url": base_url,
#                     "download_link": download_link,
#                     "doc_format": fmt,
#                     "file_extension": ext,
#                     "publish_date": _parse_date(date_updated),
#                     "modify_date": _parse_date(date_updated),   # same as publish
#                     "abstract": f"SAHPRA {category}: {title}",
#                     "atom_id": download_link,                  # unique identifier
#                     # optional meta that may be useful later
#                     "doc_number": doc_number,
#                     "version": version,
#                     "unit": unit,
#                 }
#             )

#             logger.info(f"[{idx}] {title[:70]}... | {date_updated}")

#         except Exception as e:
#             logger.warning(f"Row {idx} processing error: {e}")
#             continue

#     logger.info(f"South Africa scraping complete – {len(items)} valid documents")
#     return items













# # countries/za.py not working
# """ 
# South Africa – SAHPRA Medical Devices & IVD Guidelines scraper.
# Drops into the existing pipeline (run.py -> countries.za -> scrape_data).
# """

# import os
# import csv
# import requests
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# import logging
# from datetime import datetime

# # ----------------------------------------------------------------------
# # Helper – safe date parsing (falls back to today)
# # ----------------------------------------------------------------------
# def _parse_date(date_str: str):
#     """Try a few common formats; return YYYY-MM-DD or today."""
#     if not date_str or not date_str.strip():
#         return datetime.now().strftime("%Y-%m-%d")

#     date_str = date_str.strip()
#     # SAHPRA table uses DD/MM/YYYY
#     for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y"):
#         try:
#             return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
#         except ValueError:
#             continue
#     # fallback
#     return datetime.now().strftime("%Y-%m-%d")


# # ----------------------------------------------------------------------
# # Main entry point required by the pipeline
# # ----------------------------------------------------------------------
# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape the SAHPRA guidelines table and return a list of dicts.

#     Expected config keys (add them to countries.json):
#         - url
#         - docket_prefix
#         - document_type
#         - agency_id
#         - program_id
#         - s3_country_folder
#         - agency_sub
#         - max_title_length
#     """
#     base_url = config["url"]
#     logger.info(f"Scraping South Africa – SAHPRA ({base_url})")

#     items = []

#     # ------------------------------------------------------------------
#     # 1. Fetch page
#     # ------------------------------------------------------------------
#     session = requests.Session()
#     session.headers.update(
#         {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
#     )
#     try:
#         resp = session.get(base_url, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch SAHPRA page: {e}")
#         return items

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # ------------------------------------------------------------------
#     # 2. Locate the document table (id contains "dlp_")
#     # ------------------------------------------------------------------
#     table = soup.find("table", {"id": lambda x: x and "dlp_" in x})
#     if not table:
#         logger.error("Could not locate document table (id with 'dlp_')")
#         return items

#     rows = table.find("tbody").find_all("tr")
#     logger.info(f"Found {len(rows)} rows in the table")

#     # ------------------------------------------------------------------
#     # 3. Process rows
#     # ------------------------------------------------------------------
#     for idx, row in enumerate(rows, start=1):
#         try:
#             cols = row.find_all("td")
#             if len(cols) < 8:
#                 continue

#             doc_number   = cols[0].get_text(strip=True)
#             title        = cols[1].get_text(strip=True)
#             category     = cols[2].get_text(strip=True)
#             date_updated = cols[3].get_text(strip=True)
#             version      = cols[4].get_text(strip=True)
#             unit         = cols[5].get_text(strip=True)
#             file_type    = cols[6].get_text(strip=True).lower()

#             # Download link (absolute)
#             link_tag = cols[7].find("a", href=True)
#             download_link = urljoin(base_url, link_tag["href"]) if link_tag else ""

#             # ------------------------------------------------------------------
#             # Keep only PDF/DOC/DOCX
#             # ------------------------------------------------------------------
#             if file_type not in {"pdf", "doc", "docx"}:
#                 continue

#             ext = file_type
#             fmt = ext.upper()

#             # ------------------------------------------------------------------
#             # Build the dict expected by the pipeline
#             # url = PDF URL (requested update)
#             # ------------------------------------------------------------------
#             items.append(
#                 {
#                     "title": title[: config.get("max_title_length", 250)],
#                     "url": download_link,                 # <<< UPDATED HERE
#                     "download_link": download_link,
#                     "doc_format": fmt,
#                     "file_extension": ext,
#                     "publish_date": _parse_date(date_updated),
#                     "modify_date": _parse_date(date_updated),
#                     "abstract": f"SAHPRA {category}: {title}",
#                     "atom_id": download_link,
#                     "doc_number": doc_number,
#                     "version": version,
#                     "unit": unit,
#                 }
#             )

#             logger.info(f"[{idx}] {title[:70]}... | {date_updated}")

#         except Exception as e:
#             logger.warning(f"Row {idx} processing error: {e}")
#             continue

#     logger.info(f"South Africa scraping complete – {len(items)} valid documents")
#     return items


