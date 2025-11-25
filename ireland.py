# countries/ie.py 
import requests
from bs4 import BeautifulSoup
import re
import logging
from urllib.parse import urljoin, urlparse

try:
    from utils.file_helper import normalize_date
except ImportError:
    def normalize_date(date_str):
        return date_str


BASE_URL = "https://www.hpra.ie"
PAGE_URL = "https://www.hpra.ie/regulation/medical-devices/documents-and-guidance/guidance-documents"


def scrape_data(config, logger):
    logger.info(f"Scraping Ireland – HPRA ({PAGE_URL})")
    items = []
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    # -------------------------
    # Load main guidance page
    # -------------------------
    try:
        resp = session.get(PAGE_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch page: {e}")
        return items

    soup = BeautifulSoup(resp.text, "lxml")

    # Find all <a> tags containing .pdf
    pdf_links = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
    logger.info(f"Found {len(pdf_links)} <a> tags with .pdf in href")

    for idx, a in enumerate(pdf_links, 1):
        try:
            href = a.get("href")
            if not href:
                continue

            parsed = urlparse(href)
            path = parsed.path.lower()

            # Ensure it is really a PDF
            if not path.endswith(".pdf"):
                continue

            # Skip any tracked-changes PDFs
            if "tracked" in href.lower():
                continue

            # -------------------------
            # Extract Title
            # -------------------------
            title_tag = a.find("h2", class_="title")
            if not title_tag:
                # sometimes heading is above
                title_tag = a.find_previous("h2")

            title = title_tag.get_text(strip=True) if title_tag else "Untitled PDF"

            # Build full download link
            download_link = urljoin(BASE_URL, href)

            # -------------------------
            # Extract Date (from span.info)
            # -------------------------
            info_span = a.find_next("span", class_="info")
            raw_date = None

            if info_span:
                # Pattern handles both 12/02/2023 or 12_02_2023
                date_match = re.search(r"(\d{1,2}[\/_]\d{1,2}[\/_]\d{4})", info_span.get_text())
                if date_match:
                    raw_date = date_match.group(1).replace("_", "/")

            parsed_date = normalize_date(raw_date) if raw_date else None

            # -------------------------
            # Save item
            # URL is now the actual PDF URL
            # -------------------------
            items.append({
                "title": title[:config.get("max_title_length", 200)],
                "url": download_link,          # ✔ Corrected
                "download_link": download_link,  # ✔ PDF link
                "doc_format": "PDF",
                "file_extension": "pdf",
                "publish_date": parsed_date,
                "modify_date": parsed_date,
                "abstract": f"Ireland HPRA: {title}",
                "atom_id": download_link
            })

            logger.info(f"[{idx}] {title[:70]}... | Date: {parsed_date}")

        except Exception as e:
            logger.error(f"Error processing link #{idx}: {e}")
            continue

    logger.info(f"Ireland scraping complete – {len(items)} valid PDFs.")
    return items













# # countries/ie.py working good 
# import requests
# from bs4 import BeautifulSoup
# import re
# import logging
# from urllib.parse import urljoin, urlparse

# try:
#     from utils.file_helper import normalize_date
# except ImportError:
#     def normalize_date(date_str):
#         return date_str

# BASE_URL = "https://www.hpra.ie"
# PAGE_URL = "https://www.hpra.ie/regulation/medical-devices/documents-and-guidance/guidance-documents"


# def scrape_data(config, logger):
#     logger.info(f"Scraping Ireland – HPRA ({PAGE_URL})")
#     items = []
#     session = requests.Session()
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
#     })

#     try:
#         resp = session.get(PAGE_URL, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch page: {e}")
#         return items

#     soup = BeautifulSoup(resp.text, "lxml")
#     pdf_links = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
#     logger.info(f"Found {len(pdf_links)} <a> tags with .pdf in href")

#     for idx, a in enumerate(pdf_links, 1):
#         try:
#             href = a["href"]
#             parsed = urlparse(href)
#             path = parsed.path

#             # Skip if path does NOT end with .pdf
#             if not path.lower().endswith(".pdf"):
#                 continue

#             # Skip "tracked changes" PDFs
#             if "changes-tracked" in href.lower() or "tracked" in href.lower():
#                 continue

#             title_tag = a.find("h2", class_="title")
#             if not title_tag:
#                 title_tag = a.find_previous("h2")
#             title = title_tag.get_text(strip=True) if title_tag else "Untitled PDF"

#             download_link = urljoin(BASE_URL, href)

#             # Extract date from <span class="info">
#             info_span = a.find_next("span", class_="info")
#             raw_date = None
#             if info_span:
#                 date_match = re.search(r"(\d{1,2}[/_]\d{1,2}[/_]\d{4})", info_span.get_text())
#                 if date_match:
#                     raw_date = date_match.group(1).replace("_", "/")

#             parsed_date = normalize_date(raw_date) if raw_date else None

#             items.append({
#                 "title": title[:config.get("max_title_length", 200)],
#                 "url": PAGE_URL,
#                 "download_link": download_link,
#                 "doc_format": "PDF",
#                 "file_extension": "pdf",
#                 "publish_date": parsed_date,
#                 "modify_date": parsed_date,
#                 "abstract": f"Ireland HPRA: {title}",
#                 "atom_id": download_link
#             })

#             logger.info(f"[{idx}] {title[:70]}... | Date: {parsed_date}")

#         except Exception as e:
#             logger.error(f"Error processing link #{idx}: {e}")
#             continue

#     logger.info(f"Ireland scraping complete – {len(items)} valid PDFs.")
#     return items