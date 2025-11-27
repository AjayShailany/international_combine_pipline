import requests
from bs4 import BeautifulSoup
import logging
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse


def get_doc_format(link):
    """Detect extension safely."""
    if not link:
        return "pdf"

    parsed = urlparse(link)
    ext = os.path.splitext(parsed.path.lower())[1].replace(".", "")
    if ext in ["pdf", "doc", "docx", "xls", "xlsx"]:
        return ext
    return "pdf"


def extract_pdf_url(page_url, logger):
    """
    Visit inside page → extract real PDF URL.
    Protected from binary / non-HTML errors.
    """
    try:
        resp = requests.get(page_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to open document page {page_url}: {e}")
        return page_url

    # BLOCK non-HTML content
    content_type = resp.headers.get("Content-Type", "").lower()
    if "html" not in content_type:
        logger.warning(f"Non-HTML returned for inner page {page_url}, skipping parse.")
        return page_url

    # Safe HTML parsing
    try:
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        try:
            soup = BeautifulSoup(resp.content, "lxml")
        except Exception as e:
            logger.error(f"Parser failed on inner page {page_url}: {e}")
            return page_url

    # Look for PDF/DOC links
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if any(href.endswith(ext) for ext in [".pdf", ".doc", ".docx"]):
            return urljoin(page_url, a["href"])

    return page_url


def scrape_data(cfg, logger):
    """
    Scrape Nigeria – fully safe version.
    """
    url = cfg.get("url")
    if not url:
        logger.error("No URL defined in countries.json for NIGERIA")
        return []

    logger.info(f"Scraping NIGERIA (NAFDAC) from {url}")

    # Fetch main page safely
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch page: {e}")
        return []

    # HARD FILTER → Avoid Lightsail binary responses
    content_type = response.headers.get("Content-Type", "").lower()
    if "html" not in content_type:
        logger.error("Nigeria returned NON-HTML page. Skipping Nigeria scraping.")
        return []

    # Safe parsing of main page
    try:
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception:
        try:
            soup = BeautifulSoup(response.content, "lxml")
        except Exception as e:
            logger.error(f"Nigeria main page parsing failed: {e}")
            return []

    rows = soup.select("tr")
    raw_items = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        title_tag = cols[0].find("a")
        if not title_tag:
            continue

        title = title_tag.get_text(strip=True)
        relative_link = title_tag.get("href")
        if not relative_link:
            continue

        # Build full link
        document_page_url = urljoin(url, relative_link)

        # Extract actual document link
        pdf_url = extract_pdf_url(document_page_url, logger)

        product_type = cols[1].get_text(strip=True)
        category = cols[2].get_text(strip=True)

        if (
            "Medical Devices" in product_type
            and ("Guidance Document" in category or "Registration Requirement" in category)
        ):
            item = {
                "title": title,
                "url": pdf_url,
                "document_type_id": cfg["document_type"],
                "agency_id": cfg["agency_id"],
                "program_id": cfg["program_id"],
                "docket_prefix": cfg["docket_prefix"],
                "publish_date": None,
                "raw_content": None,
                "doc_format": get_doc_format(pdf_url),
            }

            raw_items.append(item)
            logger.info(f"Found Nigeria document: {title}")

    logger.info(f"Total Nigeria documents: {len(raw_items)}")
    return raw_items












# import requests
# from bs4 import BeautifulSoup
# import logging
# import os
# from datetime import datetime
# from urllib.parse import urljoin, urlparse


# def get_doc_format(link):
#     """
#     Detect file extension (pdf, docx, etc.) from URL.
#     Falls back to 'pdf' if none found.
#     """
#     if not link:
#         return "pdf"

#     parsed = urlparse(link)
#     path = parsed.path.lower()
#     ext = os.path.splitext(path)[1].replace('.', '')
#     if ext in ["pdf", "doc", "docx", "xls", "xlsx"]:
#         return ext
#     return "pdf"


# def extract_pdf_url(page_url, logger):
#     """
#     Visit the page and extract the real PDF URL.
#     If no PDF found, return the original link.
#     """
#     try:
#         resp = requests.get(page_url, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to open document page {page_url}: {e}")
#         return page_url

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # Look for PDF or document links
#     for a in soup.find_all("a", href=True):
#         href = a["href"].lower()
#         if href.endswith(".pdf") or href.endswith(".doc") or href.endswith(".docx"):
#             return urljoin(page_url, a["href"])

#     return page_url


# def scrape_data(cfg, logger):
#     """
#     Scrape NAFDAC Vaccines & Biologicals Guidelines page
#     Filter only Medical Devices + Guidance/Registration documents
#     Return list of raw items compatible with DatabaseManager & S3Manager
#     """
#     url = cfg.get("url")
#     if not url:
#         logger.error("No URL defined in countries.json for NIGERIA")
#         return []

#     logger.info(f"Scraping NIGERIA (NAFDAC) from {url}")

#     try:
#         response = requests.get(url, timeout=30)
#         response.raise_for_status()
#     except requests.RequestException as e:
#         logger.error(f"Failed to fetch page: {e}")
#         return []

#     soup = BeautifulSoup(response.text, "html.parser")
#     rows = soup.select("tr")

#     raw_items = []

#     for row in rows:
#         cols = row.find_all("td")
#         if len(cols) < 3:
#             continue

#         title_tag = cols[0].find("a")
#         if not title_tag:
#             continue

#         title = title_tag.get_text(strip=True)
#         relative_link = title_tag.get("href")
#         if not relative_link:
#             continue

#         # Step 1: Full page URL
#         document_page_url = urljoin(url, relative_link)

#         # Step 2: Extract real PDF URL from inside page
#         pdf_url = extract_pdf_url(document_page_url, logger)

#         product_type = cols[1].get_text(strip=True)
#         category = cols[2].get_text(strip=True)

#         # Strict filter: Only Medical Devices + Guidance/Registration
#         if ("Medical Devices" in product_type and 
#             ("Guidance Document" in category or "Registration Requirement" in category)):

#             item = {
#                 "title": title,
#                 "url": pdf_url,  # REAL PDF FILE URL
#                 "document_type_id": cfg["document_type"],
#                 "agency_id": cfg["agency_id"],
#                 "program_id": cfg["program_id"],
#                 "docket_prefix": cfg["docket_prefix"],
#                 "publish_date": None,
#                 "raw_content": None,
#                 "doc_format": get_doc_format(pdf_url),
#             }

#             raw_items.append(item)
#             logger.debug(f"Found: {title} -> PDF: {pdf_url}")

#     logger.info(f"Found {len(raw_items)} NAFDAC Medical Device documents (PDF extracted)")
#     return raw_items









#woking good
# import requests 
# from bs4 import BeautifulSoup
# import logging
# import os
# from datetime import datetime
# from urllib.parse import urljoin, urlparse

# def get_doc_format(link):
#     """
#     Detect file extension (pdf, docx, etc.) from URL.
#     Falls back to 'pdf' if none found.
#     """
#     if not link:
#         return "pdf"

#     parsed = urlparse(link)
#     path = parsed.path.lower()
#     ext = os.path.splitext(path)[1].replace('.', '')
#     if ext in ["pdf", "doc", "docx", "xls", "xlsx"]:
#         return ext
#     return "pdf"

# def scrape_data(cfg, logger):
#     """
#     Scrape NAFDAC Vaccines & Biologicals Guidelines page
#     Filter only Medical Devices + Guidance/Registration documents
#     Return list of raw items compatible with DatabaseManager & S3Manager
#     """
#     url = cfg.get("url")  # from countries.json
#     if not url:
#         logger.error("No URL defined in countries.json for NIGERIA")
#         return []

#     logger.info(f"Scraping NIGERIA (NAFDAC) from {url}")

#     try:
#         response = requests.get(url, timeout=30)
#         response.raise_for_status()
#     except requests.RequestException as e:
#         logger.error(f"Failed to fetch page: {e}")
#         return []

#     soup = BeautifulSoup(response.text, "html.parser")
#     rows = soup.select("tr")

#     raw_items = []

#     for row in rows:
#         cols = row.find_all("td")
#         if len(cols) < 3:
#             continue

#         title_tag = cols[0].find("a")
#         if not title_tag:
#             continue

#         title = title_tag.get_text(strip=True)
#         relative_link = title_tag.get("href")
#         if not relative_link:
#             continue

#         # Resolve relative URLs
#         link = urljoin(url, relative_link)
#         product_type = cols[1].get_text(strip=True)
#         category = cols[2].get_text(strip=True)

#         # Strict filter: Only Medical Devices + Guidance/Registration
#         if ("Medical Devices" in product_type and 
#             ("Guidance Document" in category or "Registration Requirement" in category)):

#             item = {
#                 "title": title,
#                 "url": link,
#                 "document_type_id": cfg["document_type"],  # 7 from countries.json
#                 "agency_id": cfg["agency_id"],            # 20
#                 "program_id": cfg["program_id"],          # 1
#                 "docket_prefix": cfg["docket_prefix"],    # NAFDAC
#                 "publish_date": None,
#                 "raw_content": None,
#                 "doc_format": get_doc_format(link),       
#             }

#             raw_items.append(item)
#             logger.debug(f"Found: {title} ({item['doc_format']})")

#     logger.info(f"Found {len(raw_items)} NAFDAC Medical Device documents")
#     return raw_items


