# countries/canada.py
"""
Health Canada – Medical Devices Guidance Documents Scraper
UPDATED: Stores all temp PDFs in OS temp directory (no project folder files)
"""

import os
import re
import time
import logging
import tempfile
from datetime import datetime
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from fpdf import FPDF  # pip install fpdf2


BASE_URL = "https://www.canada.ca"
START_URL = "https://www.canada.ca/en/health-canada/services/drugs-health-products/medical-devices/application-information/guidance-documents.html"

# SYSTEM TEMP DIRECTORY
TEMP_DIR = tempfile.gettempdir()


def _clean_text(text: str) -> str:
    """Clean text for PDF generation (FPDF uses latin-1)"""
    if not text:
        return ""
    replacements = {
        "\u2013": "-", "\u2014": "-", "\u2019": "'", "\u201c": '"', "\u201d": '"',
        "\u2022": "- ", "\u00a0": " ", "\u2018": "'"
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    return text.encode("latin-1", "replace").decode("latin-1")


def _html_to_text(soup) -> str:
    """Extract clean readable text from Health Canada page"""
    main = soup.find("main") or soup.body
    if not main:
        return soup.get_text()

    # Remove junk
    for tag in main(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()

    lines = []
    for elem in main.descendants:
        if not hasattr(elem, "name"):
            continue
        text = elem.get_text(" ", strip=True)
        if not text:
            continue

        if elem.name in ["h1", "h2", "h3", "h4", "h5"]:
            lines.append("\n" + text.upper() + "\n")
        elif elem.name == "p":
            lines.append(text + "\n")
        elif elem.name == "li":
            lines.append("• " + text)
        elif elem.name == "td":
            lines.append(text)

    content = "\n".join(lines)
    content = re.sub(r"\n{3,}", "\n\n", content)
    return content.strip()


def _generate_pdf(title: str, text: str, filepath: str):
    """Generate a clean PDF using fpdf2"""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    # Title
    pdf.set_font("Helvetica", "B", 16)
    pdf.multi_cell(0, 10, _clean_text(title[:100]))
    pdf.ln(10)

    # Date & Source
    pdf.set_font("Helvetica", "I", 10)
    pdf.multi_cell(0, 8, _clean_text(
        f"Source: Health Canada | Generated on {datetime.now():%Y-%m-%d}"
    ))
    pdf.ln(10)

    # Body
    pdf.set_font("Helvetica", "", 11)
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            pdf.ln(4)
            continue
        pdf.multi_cell(0, 6, _clean_text(line))

    pdf.output(filepath)


def scrape_data(config, logger: logging.Logger):
    logger.info(f"Scraping Health Canada ({START_URL})")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })

    try:
        resp = session.get(START_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to load main page: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    section = soup.find(lambda tag: tag.name in ["h2", "h3"] and "Medical Devices Guidance Documents" in tag.text)
    if not section:
        logger.error("Could not find guidance section")
        return []

    items = []
    ul = section.find_next("ul")
    for li in ul.find_all("li", recursive=False):
        a = li.find("a")
        if not a or not a.get("href"):
            continue

        title = a.get_text(strip=True)
        page_url = urljoin(BASE_URL, a["href"])

        date_match = re.search(r"\[(\d{4}-\d{2}-\d{2})\]", li.get_text())
        pub_date = date_match.group(1) if date_match else datetime.now().strftime("%Y-%m-%d")

        items.append({
            "title": title,
            "page_url": page_url,
            "publish_date": pub_date
        })

    logger.info(f"Found {len(items)} documents")

    results = []
    temp_files = []

    for idx, item in enumerate(items, 1):
        title = item["title"]
        page_url = item["page_url"]
        pub_date = item["publish_date"]

        logger.info(f"[{idx}/{len(items)}] {title[:80]}")

        try:
            r = session.get(page_url, timeout=30)
            r.raise_for_status()
            page_soup = BeautifulSoup(r.text, "html.parser")

            pdf_url = None
            alt_link = page_soup.find("a", string=re.compile("Download the alternative format", re.I))
            if alt_link and alt_link.get("href"):
                pdf_url = urljoin(BASE_URL, alt_link["href"])

            if not pdf_url:
                for a in page_soup.find_all("a", href=True):
                    if a["href"].lower().endswith(".pdf"):
                        candidate = urljoin(BASE_URL, a["href"])
                        try:
                            head = session.head(candidate, allow_redirects=True, timeout=10)
                            if head.status_code < 400:
                                pdf_url = candidate
                                break
                        except:
                            continue

            # CASE 1 — Real PDF exists
            if pdf_url:
                try:
                    dl = session.get(pdf_url, stream=True, timeout=60)
                    dl.raise_for_status()

                    safe_title = re.sub(r'[<>:"/\\|?*]', '_', title)[:100]
                    safe_title = re.sub(r"\s+", " ", safe_title).strip()

                    filename = f"{pub_date}_{safe_title}.pdf"
                    local_path = os.path.join(TEMP_DIR, filename)

                    with open(local_path, "wb") as f:
                        for chunk in dl.iter_content(8192):
                            f.write(chunk)

                    temp_files.append(local_path)

                    results.append({
                        "title": title,
                        "url": page_url,
                        "download_link": local_path,
                        "doc_format": "PDF",
                        "file_extension": "pdf",
                        "publish_date": pub_date,
                        "modify_date": pub_date,
                        "abstract": f"Health Canada Guidance: {title}",
                        "atom_id": pdf_url,
                    })
                    logger.info("   Downloaded PDF")
                    continue

                except Exception as e:
                    logger.warning(f"   PDF download failed: {e}")

            # CASE 2 — Generate PDF from HTML
            logger.info("   No PDF found → generating from HTML")
            text = _html_to_text(page_soup)
            if not text.strip():
                text = "No readable content found."

            safe_title = re.sub(r'[<>:"/\\|?*]', '_', title)[:100]
            safe_title = re.sub(r"\s+", " ", safe_title).strip()

            filename = f"{pub_date}_{safe_title}_GENERATED.pdf"
            local_path = os.path.join(TEMP_DIR, filename)

            _generate_pdf(title, text, local_path)
            temp_files.append(local_path)

            results.append({
                "title": title + " (HTML converted to PDF)",
                "url": page_url,
                "download_link": local_path,
                "doc_format": "PDF",
                "file_extension": "pdf",
                "publish_date": pub_date,
                "modify_date": pub_date,
                "abstract": f"HTML-only page converted to PDF: {title}",
                "atom_id": page_url + "#generated",
            })
            logger.info("   Generated PDF from HTML")

        except Exception as e:
            logger.error(f"   Failed: {e}")

        time.sleep(0.5)

    logger.info(f"Scraping complete → {len(results)} documents ready")
    return results






# # countries/canada.py
# """
# Health Canada – Medical Devices Guidance Documents Scraper
# Fully compatible with your existing pipeline (2025 working version)
# """

# import os
# import re
# import time
# import logging
# from datetime import datetime
# from urllib.parse import urljoin
# import requests
# from bs4 import BeautifulSoup
# from fpdf import FPDF  # pip install fpdf2


# BASE_URL = "https://www.canada.ca"
# START_URL = "https://www.canada.ca/en/health-canada/services/drugs-health-products/medical-devices/application-information/guidance-documents.html"


# def _clean_text(text: str) -> str:
#     """Clean text for PDF generation (FPDF uses latin-1)"""
#     if not text:
#         return ""
#     replacements = {
#         "\u2013": "-", "\u2014": "-", "\u2019": "'", "\u201c": '"', "\u201d": '"',
#         "\u2022": "- ", "\u00a0": " ", "\u2018": "'"
#     }
#     for bad, good in replacements.items():
#         text = text.replace(bad, good)
#     return text.encode("latin-1", "replace").decode("latin-1")


# def _html_to_text(soup) -> str:
#     """Extract clean readable text from Health Canada page"""
#     main = soup.find("main") or soup.body
#     if not main:
#         return soup.get_text()

#     # Remove junk
#     for tag in main(["script", "style", "nav", "header", "footer", "aside"]):
#         tag.decompose()

#     lines = []
#     for elem in main.descendants:
#         if not hasattr(elem, "name"):
#             continue
#         text = elem.get_text(" ", strip=True)
#         if not text:
#             continue

#         if elem.name in ["h1", "h2", "h3", "h4", "h5"]:
#             lines.append("\n" + text.upper() + "\n")
#         elif elem.name == "p":
#             lines.append(text + "\n")
#         elif elem.name == "li":
#             lines.append("• " + text)
#         elif elem.name == "td":
#             lines.append(text)

#     content = "\n".join(lines)
#     content = re.sub(r"\n{3,}", "\n\n", content)
#     return content.strip()


# def _generate_pdf(title: str, text: str, filepath: str):
#     """Generate a clean PDF using fpdf2"""
#     pdf = FPDF()
#     pdf.add_page()
#     pdf.set_auto_page_break(auto=True, margin=15)

#     # Title
#     pdf.set_font("Helvetica", "B", 16)
#     pdf.multi_cell(0, 10, _clean_text(title[:100]))
#     pdf.ln(10)

#     # Date & Source
#     pdf.set_font("Helvetica", "I", 10)
#     pdf.multi_cell(0, 8, _clean_text(f"Source: Health Canada | Generated on {datetime.now():%Y-%m-%d}"))
#     pdf.ln(10)

#     # Body
#     pdf.set_font("Helvetica", "", 11)
#     for line in text.split("\n"):
#         line = line.strip()
#         if not line:
#             pdf.ln(4)
#             continue
#         pdf.multi_cell(0, 6, _clean_text(line))

#     pdf.output(filepath)


# def scrape_data(config, logger: logging.Logger):
#     """
#     Main function called by your pipeline.
#     Must return list of dicts with these keys:
#         - title
#         - url (page URL)
#         - download_link (PDF or generated PDF path)
#         - doc_format = "PDF"
#         - file_extension = "pdf"
#         - publish_date (YYYY-MM-DD)
#         - modify_date (YYYY-MM-DD)
#         - abstract
#         - atom_id (unique identifier)
#     """
#     logger.info(f"Scraping Health Canada Medical Devices Guidance ({START_URL})")

#     session = requests.Session()
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
#     })

#     try:
#         resp = session.get(START_URL, timeout=30)
#         resp.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to load main page: {e}")
#         return []

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # Find the guidance list
#     section = soup.find(lambda tag: tag.name in ["h2", "h3"] and "Medical Devices Guidance Documents" in tag.text)
#     if not section:
#         logger.error("Could not find guidance section")
#         return []

#     items = []
#     ul = section.find_next("ul")
#     for li in ul.find_all("li", recursive=False):
#         a = li.find("a")
#         if not a or not a.get("href"):
#             continue

#         title = a.get_text(strip=True)
#         page_url = urljoin(BASE_URL, a["href"])

#         # Extract date from [YYYY-MM-DD]
#         date_match = re.search(r"\[(\d{4}-\d{2}-\d{2})\]", li.get_text())
#         pub_date = date_match.group(1) if date_match else datetime.now().strftime("%Y-%m-%d")

#         items.append({
#             "title": title,
#             "page_url": page_url,
#             "publish_date": pub_date
#         })

#     logger.info(f"Found {len(items)} guidance documents")

#     results = []
#     temp_files = []  # To clean up later if needed

#     for idx, item in enumerate(items, 1):
#         title = item["title"]
#         page_url = item["page_url"]
#         pub_date = item["publish_date"]

#         logger.info(f"[{idx}/{len(items)}] {title[:80]}")

#         try:
#             r = session.get(page_url, timeout=30)
#             r.raise_for_status()
#             page_soup = BeautifulSoup(r.text, "html.parser")

#             # Try to find real PDF
#             pdf_url = None
#             alt_link = page_soup.find("a", string=re.compile("Download the alternative format", re.I))
#             if alt_link and alt_link.get("href"):
#                 pdf_url = urljoin(BASE_URL, alt_link["href"])

#             if not pdf_url:
#                 for a in page_soup.find_all("a", href=True):
#                     if a["href"].lower().endswith(".pdf"):
#                         candidate = urljoin(BASE_URL, a["href"])
#                         try:
#                             head = session.head(candidate, allow_redirects=True, timeout=10)
#                             if head.status_code < 400:
#                                 pdf_url = candidate
#                                 break
#                         except:
#                             continue

#             # CASE 1: Real PDF exists → download
#             if pdf_url:
#                 try:
#                     dl = session.get(pdf_url, stream=True, timeout=60)
#                     dl.raise_for_status()

#                     safe_title = re.sub(r'[<>:"/\\|?*]', '_', title)[:100]
#                     safe_title = re.sub(r"\s+", " ", safe_title).strip()
#                     filename = f"{pub_date}_{safe_title}.pdf"
#                     local_path = os.path.join("temp_canada", filename)

#                     os.makedirs("temp_canada", exist_ok=True)
#                     with open(local_path, "wb") as f:
#                         for chunk in dl.iter_content(8192):
#                             f.write(chunk)

#                     temp_files.append(local_path)

#                     results.append({
#                         "title": title,
#                         "url": page_url,
#                         "download_link": local_path,
#                         "doc_format": "PDF",
#                         "file_extension": "pdf",
#                         "publish_date": pub_date,
#                         "modify_date": pub_date,
#                         "abstract": f"Health Canada Guidance: {title}",
#                         "atom_id": pdf_url,
#                     })
#                     logger.info(f"   Downloaded PDF")
#                     continue

#                 except Exception as e:
#                     logger.warning(f"   PDF download failed: {e}")

#             # CASE 2: No PDF → generate from HTML
#             logger.info("   No PDF found → generating from HTML")
#             text = _html_to_text(page_soup)
#             if not text.strip():
#                 text = "No readable content found on page."

#             safe_title = re.sub(r'[<>:"/\\|?*]', '_', title)[:100]
#             safe_title = re.sub(r"\s+", " ", safe_title).strip()
#             filename = f"{pub_date}_{safe_title}_GENERATED.pdf"
#             local_path = os.path.join("temp_canada", filename)
#             os.makedirs("temp_canada", exist_ok=True)

#             _generate_pdf(title, text, local_path)
#             temp_files.append(local_path)

#             results.append({
#                 "title": title + " (HTML converted to PDF)",
#                 "url": page_url,
#                 "download_link": local_path,
#                 "doc_format": "PDF",
#                 "file_extension": "pdf",
#                 "publish_date": pub_date,
#                 "modify_date": pub_date,
#                 "abstract": f"Health Canada Guidance (HTML-only page converted to PDF): {title}",
#                 "atom_id": page_url + "#generated",
#             })
#             logger.info(f"   Generated PDF from HTML")

#         except Exception as e:
#             logger.error(f"   Failed: {e}")

#         time.sleep(0.5)

#     logger.info(f"Canada scraping complete → {len(results)} documents ready for DB/S3")

#     return results

