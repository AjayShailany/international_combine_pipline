
import requests
from bs4 import BeautifulSoup
import logging
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse


def get_doc_format(link):
    """
    Detect file extension (pdf, docx, etc.) from URL.
    Falls back to 'pdf' if none found.
    """
    if not link:
        return "pdf"

    parsed = urlparse(link)
    path = parsed.path.lower()
    ext = os.path.splitext(path)[1].replace('.', '')
    if ext in ["pdf", "doc", "docx", "xls", "xlsx"]:
        return ext
    return "pdf"


def extract_pdf_url(page_url, logger):
    """
    Visit the page and extract the real PDF URL.
    If no PDF found, return the original link.
    """
    try:
        resp = requests.get(page_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to open document page {page_url}: {e}")
        return page_url

    soup = BeautifulSoup(resp.text, "html.parser")

    # Look for PDF or document links
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if href.endswith(".pdf") or href.endswith(".doc") or href.endswith(".docx"):
            return urljoin(page_url, a["href"])

    return page_url


def scrape_data(cfg, logger):
    """
    Scrape NAFDAC Vaccines & Biologicals Guidelines page
    Filter only Medical Devices + Guidance/Registration documents
    Return list of raw items compatible with DatabaseManager & S3Manager
    """
    url = cfg.get("url")
    if not url:
        logger.error("No URL defined in countries.json for NIGERIA")
        return []

    logger.info(f"Scraping NIGERIA (NAFDAC) from {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch page: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
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

        # Step 1: Full page URL
        document_page_url = urljoin(url, relative_link)

        # Step 2: Extract real PDF URL from inside page
        pdf_url = extract_pdf_url(document_page_url, logger)

        product_type = cols[1].get_text(strip=True)
        category = cols[2].get_text(strip=True)

        # Strict filter: Only Medical Devices + Guidance/Registration
        if ("Medical Devices" in product_type and 
            ("Guidance Document" in category or "Registration Requirement" in category)):

            item = {
                "title": title,
                "url": pdf_url,  # REAL PDF FILE URL
                "document_type_id": cfg["document_type"],
                "agency_id": cfg["agency_id"],
                "program_id": cfg["program_id"],
                "docket_prefix": cfg["docket_prefix"],
                "publish_date": None,
                "raw_content": None,
                "doc_format": get_doc_format(pdf_url),
            }

            raw_items.append(item)
            logger.debug(f"Found: {title} -> PDF: {pdf_url}")

    logger.info(f"Found {len(raw_items)} NAFDAC Medical Device documents (PDF extracted)")
    return raw_items









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

