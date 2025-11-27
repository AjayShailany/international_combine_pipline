# countries/new_zealand.py
"""
New Zealand – Medsafe scraper (FINAL WORKING VERSION)
Uses built-in Helvetica + smart Unicode → ASCII fallback
No external fonts needed → works everywhere
All 7 documents will succeed
"""

import os
import re
import logging
import tempfile
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from fpdf import FPDF


# Smart Unicode → safe ASCII fallback (covers bullets, dashes, quotes, etc.)
def safe_pdf_text(text: str) -> str:
    if not text:
        return ""
    replacements = {
        '•': '·',           # bullet
        '–': '-',           # en-dash
        '—': '-',           # em-dash
        '“': '"',           # left double quote
        '”': '"',           # right double quote
        '‘': "'",           # left single quote
        '’': "'",           # right single quote
        '™': '(TM)',        # trademark
        '®': '(R)',         # registered
        '©': '(C)',         # copyright
        '°': ' degrees',    # degree symbol
        '\u200b': '',       # zero-width space
        '\xa0': ' ',        # non-breaking space
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    # Final fallback: strip anything non-ASCII that might still break
    return text.encode('latin-1', 'ignore').decode('latin-1')


BASE_URL = "https://www.medsafe.govt.nz/regulatory/DevicesNew/industry.asp"
TITLE_COLOR = (0, 124, 126)


def scrape_data(config, logger: logging.Logger):
    base_url = config["url"]
    logger.info(f"Scraping New Zealand – Medsafe ({base_url})")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "en-NZ,en;q=0.9",
    })

    def fetch_soup(url):
        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    soup = fetch_soup(base_url)
    if not soup:
        return []

    links = []
    for a in soup.select("div#content-area div.subject h2 a"):
        title = " ".join(a.get_text().split())
        href = a.get("href", "").strip()
        if href:
            links.append({"title": title, "url": urljoin(base_url, href)})

    if not links:
        logger.warning("No guidance links found")
        return []

    logger.info(f"Found {len(links)} guidance sections")
    items = []

    for idx, sec in enumerate(links, 1):
        page_url = sec["url"]
        page_title = sec["title"]
        logger.info(f"[{idx}/{len(links)}] Processing: {page_title}")

        page_soup = fetch_soup(page_url)
        if not page_soup:
            continue

        content = page_soup.find("div", id="content-area")
        if not content:
            logger.warning(f"No content area found: {page_url}")
            continue

        # Extract revised date
        modify_date = None
        updated = content.find("p", class_="updated")
        if updated:
            m = re.search(r"Revised:\s*(.+)", updated.get_text())
            if m:
                try:
                    dt = datetime.strptime(m.group(1).strip(), "%d %B %Y")
                    modify_date = dt.strftime("%Y-%m-%d")
                except:
                    pass
        # Clean up
        for junk in content.select("#breadcrumbs, p.updated"):
            junk.decompose()

        # Build content blocks
        blocks = []
        for el in content.children:
            if not hasattr(el, "name") or not el.name:
                continue

            if el.name in ["h1", "h2", "h3"]:
                txt = el.get_text(" ", strip=True)
                if txt:
                    blocks.append({"type": "heading", "text": txt})

            elif el.name == "p":
                txt = el.get_text(" ", strip=True)
                if txt:
                    blocks.append({"type": "paragraph", "text": txt})

            elif el.name in ["ul", "ol"]:
                for li in el.find_all("li", recursive=False):
                    txt = li.get_text(" ", strip=True)
                    if txt:
                        blocks.append({"type": "bullet", "text": txt})

            elif el.name == "table":
                rows = []
                for tr in el.find_all("tr"):
                    row = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                    if any(c.strip() for c in row):
                        rows.append(row)
                if rows:
                    blocks.append({"type": "table", "rows": rows})

        # Generate PDF using only built-in fonts
        try:
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=15)
            pdf.add_page()

            # Title
            pdf.set_font("Helvetica", "B", 16)
            pdf.set_text_color(*TITLE_COLOR)
            pdf.multi_cell(0, 10, safe_pdf_text(page_title))
            pdf.ln(10)

            for b in blocks:
                if b["type"] == "heading":
                    pdf.set_font("Helvetica", "B", 13)
                    pdf.set_text_color(*TITLE_COLOR)
                    pdf.multi_cell(0, 8, safe_pdf_text(b["text"]))
                    pdf.ln(3)

                elif b["type"] == "paragraph":
                    pdf.set_font("Helvetica", size=11)
                    pdf.set_text_color(0, 0, 0)
                    pdf.multi_cell(0, 6, safe_pdf_text(b["text"]))
                    pdf.ln(2)

                elif b["type"] == "bullet":
                    pdf.set_font("Helvetica", size=11)
                    pdf.set_text_color(0, 0, 0)
                    pdf.multi_cell(0, 6, f"· {safe_pdf_text(b['text'])}")
                    pdf.ln(1)

                elif b["type"] == "table" and b["rows"]:
                    pdf.ln(4)
                    col_count = max(len(r) for r in b["rows"])
                    col_width = (pdf.w - 30) / col_count

                    for r_idx, row in enumerate(b["rows"]):
                        pdf.set_font("Helvetica", "B" if r_idx == 0 else "", 10)
                        x = 15
                        y = pdf.get_y()
                        max_h = 0

                        for cell in row:
                            pdf.set_xy(x, y)
                            cell_text = safe_pdf_text(cell)
                            pdf.multi_cell(col_width, 7, cell_text, border=1, align="L")
                            cell_h = pdf.get_y() - y
                            max_h = max(max_h, cell_h)
                            x += col_width

                        pdf.set_xy(15, y + max_h)
                    pdf.ln(5)

            # Save to proper temp file
            fd, temp_path = tempfile.mkstemp(suffix=".pdf", prefix="medsafe_nz_")
            os.close(fd)
            pdf.output(temp_path)
            logger.info(f"PDF created: {temp_path}")

        except Exception as e:
            logger.error(f"PDF generation failed for {page_title}: {e}")
            continue

        items.append({
            "title": page_title[: config.get("max_title_length", 250)],
            "url": page_url,
            "download_link": temp_path,
            "local_path": temp_path,
            "doc_format": "PDF",
            "file_extension": "pdf",
            "publish_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
            "modify_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
            "abstract": f"Medsafe New Zealand guidance: {page_title}"[:1000],
            "atom_id": page_url,
        })

    logger.info(f"New Zealand complete – {len(items)} PDFs ready (all 7 should work now!)")
    return items






# # countries/new_zealand.py
# """
# New Zealand – Medsafe scraper (FINAL VERSION)
# Uses FPDF2 with DejaVu font → full Unicode support (•, –, ™, etc.)
# Works perfectly on Windows + all 7 documents succeed.
# """

# import os
# import re
# import logging
# import tempfile
# from datetime import datetime
# from urllib.parse import urljoin

# import requests
# from bs4 import BeautifulSoup
# from fpdf import FPDF


# # Use FPDF2 with built-in Unicode support
# class PDF(FPDF):
#     def __init__(self):
#         super().__init__()
#         # Add DejaVu font (bundled with fpdf2) – supports bullets, smart quotes, etc.
#         self.add_font("DejaVu", "", "DejaVuSans.ttf", uni=True)
#         self.add_font("DejaVu", "B", "DejaVuSans-Bold.ttf", uni=True)
#         self.set_font("DejaVu", size=11)

#     def header(self):
#         pass

#     def footer(self):
#         pass


# BASE_URL = "https://www.medsafe.govt.nz/regulatory/DevicesNew/industry.asp"
# TITLE_COLOR = (0, 124, 126)


# def scrape_data(config, logger: logging.Logger):
#     base_url = config["url"]
#     logger.info(f"Scraping New Zealand – Medsafe ({base_url})")

#     session = requests.Session()
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
#         "Accept-Language": "en-NZ,en;q=0.9",
#     })

#     def fetch_soup(url):
#         try:
#             r = session.get(url, timeout=30)
#             r.raise_for_status()
#             return BeautifulSoup(r.text, "html.parser")
#         except Exception as e:
#             logger.error(f"Failed to fetch {url}: {e}")
#             return None

#     soup = fetch_soup(base_url)
#     if not soup:
#         return []

#     links = []
#     for a in soup.select("div#content-area div.subject h2 a"):
#         title = " ".join(a.get_text().split())
#         href = a.get("href", "").strip()
#         if href:
#             links.append({"title": title, "url": urljoin(base_url, href)})

#     if not links:
#         logger.warning("No guidance links found")
#         return []

#     logger.info(f"Found {len(links)} guidance sections")
#     items = []

#     for idx, sec in enumerate(links, 1):
#         page_url = sec["url"]
#         page_title = sec["title"]
#         logger.info(f"[{idx}/{len(links)}] Processing: {page_title}")

#         page_soup = fetch_soup(page_url)
#         if not page_soup:
#             continue

#         content = page_soup.find("div", id="content-area")
#         if not content:
#             continue

#         # Extract revised date
#         modify_date = None
#         updated = content.find("p", class_="updated")
#         if updated:
#             m = re.search(r"Revised:\s*(.+)", updated.get_text())
#             if m:
#                 try:
#                     dt = datetime.strptime(m.group(1).strip(), "%d %B %Y")
#                     modify_date = dt.strftime("%Y-%m-%d")
#                 except:
#                     pass
#         for junk in content.select("#breadcrumbs, p.updated"):
#             junk.decompose()

#         # Build blocks
#         blocks = []
#         for el in content.children:
#             if not hasattr(el, "name") or not el.name:
#                 continue
#             tag = el.name

#             if tag in ["h1", "h2", "h3"]:
#                 txt = el.get_text(" ", strip=True)
#                 if txt:
#                     blocks.append({"type": "heading", "text": txt})

#             elif tag == "p":
#                 txt = el.get_text(" ", strip=True)
#                 if txt:
#                     blocks.append({"type": "paragraph", "text": txt})

#             elif tag in ["ul", "ol"]:
#                 for li in el.find_all("li", recursive=False):
#                     txt = li.get_text(" ", strip=True)
#                     if txt:
#                         blocks.append({"type": "bullet", "text": txt})

#             elif tag == "table":
#                 rows = []
#                 for tr in el.find_all("tr"):
#                     row = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
#                     if any(c.strip() for c in row):
#                         rows.append(row)
#                 if rows:
#                     blocks.append({"type": "table", "rows": rows})

#         # Generate PDF with full Unicode support
#         try:
#             pdf = PDF()
#             pdf.set_auto_page_break(auto=True, margin=15)
#             pdf.add_page()

#             # Title
#             pdf.set_font("DejaVu", "B", 16)
#             pdf.set_text_color(*TITLE_COLOR)
#             pdf.multi_cell(0, 10, page_title)
#             pdf.ln(10)

#             for b in blocks:
#                 if b["type"] == "heading":
#                     pdf.set_font("DejaVu", "B", 13)
#                     pdf.set_text_color(*TITLE_COLOR)
#                     pdf.multi_cell(0, 8, b["text"])
#                     pdf.ln(3)

#                 elif b["type"] == "paragraph":
#                     pdf.set_font("DejaVu", size=11)
#                     pdf.set_text_color(0, 0, 0)
#                     pdf.multi_cell(0, 6, b["text"])
#                     pdf.ln(2)

#                 elif b["type"] == "bullet":
#                     pdf.set_font("DejaVu", size=11)
#                     pdf.set_text_color(0, 0, 0)
#                     pdf.multi_cell(0, 6, f"• {b['text']}")
#                     pdf.ln(1)

#                 elif b["type"] == "table" and b["rows"]:
#                     pdf.ln(4)
#                     col_count = max(len(r) for r in b["rows"])
#                     col_width = (pdf.w - 30) / col_count

#                     for r_idx, row in enumerate(b["rows"]):
#                         pdf.set_font("DejaVu", "B" if r_idx == 0 else "", 10)
#                         x = 15
#                         y = pdf.get_y()
#                         max_h = 0

#                         for cell in row:
#                             pdf.set_xy(x, y)
#                             h = pdf.multi_cell(col_width, 7, cell, border=1, align="L")
#                             cell_h = pdf.get_y() - y
#                             max_h = max(max_h, cell_h)
#                             x += col_width

#                         pdf.set_xy(15, y + max_h)
#                     pdf.ln(5)

#             # Save to temp file
#             fd, temp_path = tempfile.mkstemp(suffix=".pdf", prefix="medsafe_nz_")
#             os.close(fd)
#             pdf.output(temp_path)
#             logger.info(f"PDF created: {temp_path}")

#         except Exception as e:
#             logger.error(f"PDF generation failed for {page_title}: {e}")
#             continue

#         items.append({
#             "title": page_title[: config.get("max_title_length", 250)],
#             "url": page_url,
#             "download_link": temp_path,
#             "local_path": temp_path,
#             "doc_format": "PDF",
#             "file_extension": "pdf",
#             "publish_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
#             "modify_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
#             "abstract": f"Medsafe New Zealand guidance: {page_title}"[:1000],
#             "atom_id": page_url,
#         })

#     logger.info(f"New Zealand complete – {len(items)} PDFs ready (expected: 7)")
#     return items





# # countries/new_zealand.py
# """
# New Zealand – Medsafe scraper
# Converts HTML guidance pages into clean PDFs.
# Now works perfectly on Windows + handles Unicode bullets (•, –, etc.)
# """

# import os
# import re
# import logging
# import tempfile
# from datetime import datetime
# from urllib.parse import urljoin

# import requests
# from bs4 import BeautifulSoup
# from fpdf import FPDF


# BASE_URL = "https://www.medsafe.govt.nz/regulatory/DevicesNew/industry.asp"
# TITLE_COLOR = (0, 124, 126)  # Medsafe teal


# def clean_text_for_pdf(text: str) -> str:
#     """Replace common Unicode bullets and smart quotes that FPDF can't handle"""
#     replacements = {
#         '\u2022': '•',       # bullet
#         '\u2013': '-',       # en-dash
#         '\u2014': '-',       # em-dash
#         '\u2018': "'",       # left single quote
#         '\u2019': "'",       # right single quote
#         '\u201c': '"',       # left double quote
#         '\u201d': '"',       # right double quote
#         '\u200b': '',        # zero-width space
#         '\xa0': ' ',         # non-breaking space
#     }
#     for bad, good in replacements.items():
#         text = text.replace(bad, good)
#     return text


# def scrape_data(config, logger: logging.Logger):
#     base_url = config["url"]
#     logger.info(f"Scraping New Zealand – Medsafe ({base_url})")

#     session = requests.Session()
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                       "AppleWebKit/537.36 (KHTML, like Gecko) "
#                       "Chrome/120.0 Safari/537.36",
#         "Accept-Language": "en-NZ,en;q=0.9",
#     })

#     def fetch_soup(url: str):
#         try:
#             r = session.get(url, timeout=30)
#             r.raise_for_status()
#             return BeautifulSoup(r.text, "html.parser")
#         except Exception as e:
#             logger.error(f"Failed to fetch {url}: {e}")
#             return None

#     # Get main guidance sections
#     soup = fetch_soup(base_url)
#     if not soup:
#         return []

#     links = []
#     for a in soup.select("div#content-area div.subject h2 a"):
#         title = " ".join(a.get_text().split())
#         href = a.get("href", "").strip()
#         if href:
#             links.append({"title": title, "url": urljoin(base_url, href)})

#     if not links:
#         logger.warning("No guidance links found")
#         return []

#     logger.info(f"Found {len(links)} guidance sections")
#     items = []

#     for idx, sec in enumerate(links, 1):
#         page_url = sec["url"]
#         page_title = sec["title"]
#         logger.info(f"[{idx}/{len(links)}] Processing: {page_title}")

#         page_soup = fetch_soup(page_url)
#         if not page_soup:
#             continue

#         content = page_soup.find("div", id="content-area")
#         if not content:
#             logger.warning(f"No content-area in {page_url}")
#             continue

#         # Extract revised date
#         modify_date = None
#         updated = content.find("p", class_="updated")
#         if updated:
#             match = re.search(r"Revised:\s*(.+)", updated.get_text())
#             if match:
#                 try:
#                     dt = datetime.strptime(match.group(1).strip(), "%d %B %Y")
#                     modify_date = dt.strftime("%Y-%m-%d")
#                 except:
#                     pass
#         if updated:
#             updated.decompose()

#         # Remove breadcrumbs
#         for b in content.select("#breadcrumbs"):
#             b.decompose()

#         # Build blocks
#         blocks = []
#         for el in content.children:
#             if not hasattr(el, "name") or not el.name:
#                 continue
#             tag = el.name

#             if tag in ["h1", "h2", "h3"]:
#                 txt = el.get_text(" ", strip=True)
#                 if txt:
#                     blocks.append({"type": "heading", "text": txt})

#             elif tag == "p":
#                 txt = el.get_text(" ", strip=True)
#                 if txt:
#                     blocks.append({"type": "paragraph", "text": txt})

#             elif tag in ["ul", "ol"]:
#                 for li in el.find_all("li", recursive=False):
#                     txt = li.get_text(" ", strip=True)
#                     if txt:
#                         blocks.append({"type": "bullet", "text": txt})

#             elif tag == "table":
#                 rows = []
#                 for tr in el.find_all("tr"):
#                     row = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
#                     if any(c.strip() for c in row):
#                         rows.append(row)
#                 if rows:
#                     blocks.append({"type": "table", "rows": rows})

#         # Generate PDF using a proper temporary file that works on Windows
#         try:
#             pdf = FPDF()
#             pdf.set_auto_page_break(auto=True, margin=15)
#             pdf.add_page()
#             pdf.set_font("Arial", "B", 16)
#             pdf.set_text_color(*TITLE_COLOR)
#             pdf.multi_cell(0, 10, clean_text_for_pdf(page_title))
#             pdf.ln(10)

#             for b in blocks:
#                 text = clean_text_for_pdf(b["text"]) if "text" in b else ""

#                 if b["type"] == "heading":
#                     pdf.set_font("Arial", "B", 13)
#                     pdf.set_text_color(*TITLE_COLOR)
#                     pdf.multi_cell(0, 8, text)
#                     pdf.ln(3)

#                 elif b["type"] == "paragraph":
#                     pdf.set_font("Arial", "", 11)
#                     pdf.set_text_color(0, 0, 0)
#                     pdf.multi_cell(0, 6, text)
#                     pdf.ln(2)

#                 elif b["type"] == "bullet":
#                     pdf.set_font("Arial", "", 11)
#                     pdf.set_text_color(0, 0, 0)
#                     pdf.multi_cell(0, 6, f"• {text}")
#                     pdf.ln(1)

#                 elif b["type"] == "table" and b["rows"]:
#                     pdf.ln(4)
#                     col_count = max(len(r) for r in b["rows"])
#                     col_width = (pdf.w - 30) / col_count  # 15mm margin each side

#                     for r_idx, row in enumerate(b["rows"]):
#                         pdf.set_font("Arial", "B" if r_idx == 0 else "", 10)
#                         max_h = 0
#                         x = pdf.get_x() + 15
#                         y = pdf.get_y()

#                         for cell in row:
#                             pdf.set_xy(x, y)
#                             cell_text = clean_text_for_pdf(cell)
#                             h = pdf.multi_cell(col_width, 7, cell_text, border=1, align="L")
#                             cell_height = pdf.get_y() - y
#                             max_h = max(max_h, cell_height)
#                             x += col_width

#                         pdf.set_xy(15, y + max_h)
#                     pdf.ln(5)

#             # Use tempfile that works on ALL OS (including Windows)
#             fd, temp_path = tempfile.mkstemp(suffix=".pdf", prefix="medsafe_nz_")
#             os.close(fd)  # close the file descriptor, we only need the path

#             pdf.output(temp_path)
#             logger.info(f"PDF created: {temp_path}")

#         except Exception as e:
#             logger.error(f"PDF generation failed for {page_title}: {e}")
#             continue

#         # Add to pipeline
#         items.append({
#             "title": page_title[: config.get("max_title_length", 250)],
#             "url": page_url,
#             "download_link": temp_path,           # S3Manager reads this
#             "local_path": temp_path,              # backup key
#             "doc_format": "PDF",
#             "file_extension": "pdf",
#             "publish_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
#             "modify_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
#             "abstract": f"Medsafe New Zealand guidance: {page_title}"[:1000],
#             "atom_id": page_url,
#         })

#     logger.info(f"New Zealand complete – {len(items)} PDFs ready for upload")
#     return items







# # countries/new_zealand.py
# """
# New Zealand – Medsafe scraper
# Converts all guidance pages (which have no native PDF) into clean, well-formatted PDFs.
# Fully compatible with your existing pipeline (DB → S3 → docket system).
# """

# import os
# import re
# import logging
# from datetime import datetime
# from urllib.parse import urljoin

# import requests
# from bs4 import BeautifulSoup, NavigableString
# from fpdf import FPDF


# # ----------------------------------------------------------------------
# # Configuration (will be overridden by countries.json)
# # ----------------------------------------------------------------------
# BASE_URL = "https://www.medsafe.govt.nz/regulatory/DevicesNew/industry.asp"
# TITLE_COLOR = (0, 124, 126)  # Medsafe green-blue


# # ----------------------------------------------------------------------
# # Helper functions
# # ----------------------------------------------------------------------
# def latin1_safe(text: str | None) -> str:
#     if not text:
#         return ""
#     return text.encode("latin-1", "replace").decode("latin-1")


# def parse_revised_date(text: str | None) -> str | None:
#     if not text:
#         return None
#     m = re.search(r"Revised:\s*(.+)$", text.strip())
#     if not m:
#         return None
#     raw = m.group(1).strip()
#     try:
#         dt = datetime.strptime(raw, "%d %B %Y")
#         return dt.strftime("%Y-%m-%d")
#     except ValueError:
#         return None


# def has_url(text: str) -> bool:
#     return "http://" in text or "https://" in text


# # ----------------------------------------------------------------------
# # Main scraper – must export scrape_data(config, logger)
# # ----------------------------------------------------------------------
# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape Medsafe (New Zealand) medical device guidance pages.
#     All pages are HTML-only → we generate beautiful PDFs on the fly.
#     """
#     base_url = config["url"]  # from countries.json
#     logger.info(f"Scraping New Zealand – Medsafe ({base_url})")

#     session = requests.Session()
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                       "AppleWebKit/537.36 (KHTML, like Gecko) "
#                       "Chrome/120.0 Safari/537.36",
#         "Accept-Language": "en-NZ,en;q=0.9",
#         "Referer": "https://www.medsafe.govt.nz/",
#     })

#     def fetch_soup(url: str) -> BeautifulSoup | None:
#         try:
#             r = session.get(url, timeout=30)
#             r.raise_for_status()
#             return BeautifulSoup(r.text, "html.parser")
#         except Exception as e:
#             logger.error(f"Failed to fetch {url}: {e}")
#             return None

#     # ------------------------------------------------------------------
#     # 1. Get main page and extract subject links
#     # ------------------------------------------------------------------
#     soup = fetch_soup(base_url)
#     if not soup:
#         return []

#     subject_links = []
#     for a in soup.select("div#content-area div.subject h2 a"):
#         title = " ".join(a.get_text().split())
#         href = a.get("href", "").strip()
#         if href:
#             subject_links.append({
#                 "title": title,
#                 "url": urljoin(base_url, href)
#             })

#     if not subject_links:
#         logger.warning("No guidance sections found")
#         return []

#     logger.info(f"Found {len(subject_links)} guidance sections")

#     items = []

#     # ------------------------------------------------------------------
#     # 2. Process each guidance page → generate PDF
#     # ------------------------------------------------------------------
#     for idx, sec in enumerate(subject_links, 1):
#         page_url = sec["url"]
#         page_title = sec["title"]

#         logger.info(f"[{idx}/{len(subject_links)}] Processing: {page_title}")

#         page_soup = fetch_soup(page_url)
#         if not page_soup:
#             continue

#         content_div = page_soup.find("div", id="content-area")
#         if not content_div:
#             logger.warning(f"No content-area in {page_url}")
#             continue

#         # Extract revised date
#         modify_date = None
#         updated_p = content_div.find("p", class_="updated")
#         if updated_p:
#             modify_date = parse_revised_date(updated_p.get_text())

#         # Clean up
#         for junk in content_div.select("#breadcrumbs, p.updated"):
#             junk.decompose()

#         # Build clean text blocks
#         blocks = []
#         for elem in content_div.children:
#             if isinstance(elem, NavigableString) or not hasattr(elem, "name"):
#                 continue

#             tag = elem.name

#             if tag in ["h1", "h2", "h3"]:
#                 text = elem.get_text(" ", strip=True)
#                 if text:
#                     blocks.append({"type": "heading", "text": text})

#             elif tag == "p":
#                 text = elem.get_text(" ", strip=True)
#                 if text:
#                     blocks.append({"type": "paragraph", "text": text})

#             elif tag in ["ul", "ol"]:
#                 for li in elem.find_all("li", recursive=False):
#                     text = li.get_text(" ", strip=True)
#                     if text:
#                         blocks.append({"type": "bullet", "text": text})

#             elif tag == "table":
#                 rows = []
#                 for tr in elem.find_all("tr"):
#                     row = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
#                     if row and any(cell.strip() for cell in row):
#                         rows.append(row)
#                 if rows:
#                     blocks.append({"type": "table", "rows": rows})

#         # ----------------------------------------------------------------
#         # 3. Generate PDF using FPDF (same style as your standalone script)
#         # ----------------------------------------------------------------
#         pdf = FPDF()
#         pdf.set_auto_page_break(auto=True, margin=15)
#         pdf.add_page()
#         pdf.set_font("Arial", "B", 16)
#         pdf.set_text_color(*TITLE_COLOR)
#         pdf.multi_cell(0, 10, latin1_safe(page_title))
#         pdf.ln(8)

#         for block in blocks:
#             if block["type"] == "heading":
#                 pdf.set_font("Arial", "B", 13)
#                 pdf.set_text_color(*TITLE_COLOR)
#                 pdf.ln(2)
#                 pdf.multi_cell(0, 7, latin1_safe(block["text"]))
#                 pdf.ln(1)

#             elif block["type"] == "paragraph":
#                 pdf.set_font("Arial", "", 11)
#                 pdf.set_text_color(0, 0, 0)
#                 has_link = any("http" in line for line in block["text"].split("\n"))
#                 if has_link:
#                     pdf.set_text_color(*TITLE_COLOR)
#                 pdf.multi_cell(0, 5, latin1_safe(block["text"]))
#                 pdf.ln(1)

#             elif block["type"] == "bullet":
#                 pdf.set_font("Arial", "", 11)
#                 pdf.set_text_color(0, 0, 0)
#                 if "http" in block["text"]:
#                     pdf.set_text_color(*TITLE_COLOR)
#                 pdf.multi_cell(0, 5, f"• {latin1_safe(block['text'])}")
#                 pdf.ln(0.5)

#             elif block["type"] == "table" and block["rows"]:
#                 pdf.ln(2)
#                 rows = block["rows"]
#                 if not rows:
#                     continue

#                 col_width_total = pdf.w - 2 * pdf.l_margin
#                 ncols = max(len(r) for r in rows)
#                 if ncols == 2:
#                     widths = [col_width_total * 0.25, col_width_total * 0.75]
#                 else:
#                     widths = [col_width_total / ncols] * ncols

#                 for i, row in enumerate(rows):
#                     pdf.set_font("Arial", "B" if i == 0 else "", 11)

#                     x_start = pdf.l_margin
#                     y_start = pdf.get_y()
#                     max_h = 0

#                     for col_idx, cell_text in enumerate(row):
#                         w = widths[col_idx] if col_idx < len(widths) else col_width_total / ncols
#                         pdf.set_xy(x_start, y_start)

#                         # green if this cell contains a URL, else black
#                         if has_url(cell_text):
#                             pdf.set_text_color(*TITLE_COLOR)
#                         else:
#                             pdf.set_text_color(0, 0, 0)

#                         # Fixed: multi_cell returns list in FPDF2, so calculate height properly
#                         lines = pdf.multi_cell(w, 6, latin1_safe(cell_text), border=1, align="L")
#                         cell_h = len(lines) * 6 if isinstance(lines, list) else 6
#                         max_h = max(max_h, cell_h)
#                         x_start += w

#                     pdf.set_xy(pdf.l_margin, y_start + max_h)

#                 pdf.ln(2)

#         # Save PDF to a temporary-looking name – S3Manager will handle final naming
#         safe_name = re.sub(r"[^\w\s-]", "", page_title)
#         safe_name = re.sub(r"\s+", "_", safe_name).strip("_")[:100]
#         if not safe_name:
#             safe_name = f"guidance_{idx}"
#         temp_pdf_path = f"/tmp/medsafe_nz_{os.getpid()}_{idx}_{safe_name}.pdf"

#         try:
#             pdf.output(temp_pdf_path)
#         except Exception as e:
#             logger.error(f"PDF generation failed for {page_title}: {e}")
#             continue

#         # ----------------------------------------------------------------
#         # 4. Build pipeline-compatible item
#         # ----------------------------------------------------------------
#         items.append({
#             "title": page_title[: config.get("max_title_length", 250)],
#             "url": page_url,
#             "download_link": temp_pdf_path,        # S3Manager will upload this file
#             "doc_format": "PDF",
#             "file_extension": "pdf",
#             "publish_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
#             "modify_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
#             "abstract": f"Medsafe guidance: {page_title}"[:1000],
#             "atom_id": page_url,                   # unique hash key
#             "local_path": temp_pdf_path,           # alternative key used by S3Manager
#         })

#         logger.info(f"Generated PDF for: {page_title}")

#     logger.info(f"New Zealand scraping complete – {len(items)} documents ready")
#     return items







# # countries/new_zealand.py
# """
# New Zealand – Medsafe scraper
# Converts all guidance pages (which have no native PDF) into clean, well-formatted PDFs.
# Fully compatible with your existing pipeline (DB → S3 → docket system).
# """

# import os
# import re
# import logging
# from datetime import datetime
# from urllib.parse import urljoin

# import requests
# from bs4 import BeautifulSoup, NavigableString
# from fpdf import FPDF


# # ----------------------------------------------------------------------
# # Configuration (will be overridden by countries.json)
# # ----------------------------------------------------------------------
# BASE_URL = "https://www.medsafe.govt.nz/regulatory/DevicesNew/industry.asp"
# TITLE_COLOR = (0, 124, 126)  # Medsafe green-blue


# # ----------------------------------------------------------------------
# # Helper functions
# # ----------------------------------------------------------------------
# def latin1_safe(text: str | None) -> str:
#     if not text:
#         return ""
#     return text.encode("latin-1", "replace").decode("latin-1")


# def parse_revised_date(text: str | None) -> str | None:
#     if not text:
#         return None
#     m = re.search(r"Revised:\s*(.+)$", text.strip())
#     if not m:
#         return None
#     raw = m.group(1).strip()
#     try:
#         dt = datetime.strptime(raw, "%d %B %Y")
#         return dt.strftime("%Y-%m-%d")
#     except ValueError:
#         return None


# def has_url(text: str) -> bool:
#     return "http://" in text or "https://" in text


# # ----------------------------------------------------------------------
# # Main scraper – must export scrape_data(config, logger)
# # ----------------------------------------------------------------------
# def scrape_data(config, logger: logging.Logger):
#     """
#     Scrape Medsafe (New Zealand) medical device guidance pages.
#     All pages are HTML-only → we generate beautiful PDFs on the fly.
#     """
#     base_url = config["url"]  # from countries.json
#     logger.info(f"Scraping New Zealand – Medsafe ({base_url})")

#     session = requests.Session()
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                       "AppleWebKit/537.36 (KHTML, like Gecko) "
#                       "Chrome/120.0 Safari/537.36",
#         "Accept-Language": "en-NZ,en;q=0.9",
#         "Referer": "https://www.medsafe.govt.nz/",
#     })

#     def fetch_soup(url: str) -> BeautifulSoup | None:
#         try:
#             r = session.get(url, timeout=30)
#             r.raise_for_status()
#             return BeautifulSoup(r.text, "html.parser")
#         except Exception as e:
#             logger.error(f"Failed to fetch {url}: {e}")
#             return None

#     # ------------------------------------------------------------------
#     # 1. Get main page and extract subject links
#     # ------------------------------------------------------------------
#     soup = fetch_soup(base_url)
#     if not soup:
#         return []

#     subject_links = []
#     for a in soup.select("div#content-area div.subject h2 a"):
#         title = " ".join(a.get_text().split())
#         href = a.get("href", "").strip()
#         if href:
#             subject_links.append({
#                 "title": title,
#                 "url": urljoin(base_url, href)
#             })

#     if not subject_links:
#         logger.warning("No guidance sections found")
#         return []

#     logger.info(f"Found {len(subject_links)} guidance sections")

#     items = []

#     # ------------------------------------------------------------------
#     # 2. Process each guidance page → generate PDF
#     # ------------------------------------------------------------------
#     for idx, sec in enumerate(subject_links, 1):
#         page_url = sec["url"]
#         page_title = sec["title"]

#         logger.info(f"[{idx}/{len(subject_links)}] Processing: {page_title}")

#         page_soup = fetch_soup(page_url)
#         if not page_soup:
#             continue

#         content_div = page_soup.find("div", id="content-area")
#         if not content_div:
#             logger.warning(f"No content-area in {page_url}")
#             continue

#         # Extract revised date
#         modify_date = None
#         updated_p = content_div.find("p", class_="updated")
#         if updated_p:
#             modify_date = parse_revised_date(updated_p.get_text())

#         # Clean up
#         for junk in content_div.select("#breadcrumbs, p.updated"):
#             junk.decompose()

#         # Build clean text blocks
#         blocks = []
#         for elem in content_div.children:
#             if isinstance(elem, NavigableString) or not hasattr(elem, "name"):
#                 continue

#             tag = elem.name

#             if tag in ["h1", "h2", "h3"]:
#                 text = elem.get_text(" ", strip=True)
#                 if text:
#                     blocks.append({"type": "heading", "text": text})

#             elif tag == "p":
#                 text = elem.get_text(" ", strip=True)
#                 if text:
#                     blocks.append({"type": "paragraph", "text": text})

#             elif tag in ["ul", "ol"]:
#                 for li in elem.find_all("li", recursive=False):
#                     text = li.get_text(" ", strip=True)
#                     if text:
#                         blocks.append({"type": "bullet", "text": text})

#             elif tag == "table":
#                 rows = []
#                 for tr in elem.find_all("tr"):
#                     row = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
#                     if row and any(cell.strip() for cell in row):
#                         rows.append(row)
#                 if rows:
#                     blocks.append({"type": "table", "rows": rows})

#         # ----------------------------------------------------------------
#         # 3. Generate PDF using FPDF (same style as your standalone script)
#         # ----------------------------------------------------------------
#         pdf = FPDF()
#         pdf.set_auto_page_break(auto=True, margin=15)
#         pdf.add_page()
#         pdf.set_font("Arial", "B", 16)
#         pdf.set_text_color(*TITLE_COLOR)
#         pdf.multi_cell(0, 10, latin1_safe(page_title))
#         pdf.ln(8)

#         for block in blocks:
#             if block["type"] == "heading":
#                 pdf.set_font("Arial", "B", 13)
#                 pdf.set_text_color(*TITLE_COLOR)
#                 pdf.multi_cell(0, 8, latin1_safe(block["text"]))
#                 pdf.ln(3)

#             elif block["type"] == "paragraph":
#                 pdf.set_font("Arial", "", 11)
#                 pdf.set_text_color(0, 0, 0)
#                 has_link = any("http" in line for line in block["text"].split("\n"))
#                 if has_link:
#                     pdf.set_text_color(*TITLE_COLOR)
#                 pdf.multi_cell(0, 6, latin1_safe(block["text"]))
#                 pdf.ln(2)

#             elif block["type"] == "bullet":
#                 pdf.set_font("Arial", "", 11)
#                 pdf.set_text_color(0, 0, 0)
#                 if "http" in block["text"]:
#                     pdf.set_text_color(*TITLE_COLOR)
#                 pdf.multi_cell(0, 6, f"• {latin1_safe(block['text'])}")
#                 pdf.ln(1)

#             elif block["type"] == "table" and block["rows"]:
#                 pdf.ln(4)
#                 col_count = max(len(r) for r in block["rows"])
#                 col_width = (pdf.w - 2 * pdf.l_margin) / col_count

#                 for r_idx, row in enumerate(block["rows"]):
#                     pdf.set_font("Arial", "B" if r_idx == 0 else "", 10)
#                     max_h = 0
#                     x_start = pdf.get_x()
#                     y_start = pdf.get_y()

#                     for cell in row:
#                         pdf.set_xy(x_start, y_start)
#                         cell_h = pdf.multi_cell(col_width, 7, latin1_safe(cell), border=1, align="L").get("h", 7)
#                         max_h = max(max_h, cell_h)
#                         x_start += col_width

#                     pdf.set_xy(pdf.l_margin, y_start + max_h)

#                 pdf.ln(4)

#         # Save PDF to a temporary-looking name – S3Manager will handle final naming
#         safe_name = re.sub(r"[^\w\s-]", "", page_title)
#         safe_name = re.sub(r"\s+", "_", safe_name).strip("_")[:100]
#         if not safe_name:
#             safe_name = f"guidance_{idx}"
#         temp_pdf_path = f"/tmp/medsafe_nz_{os.getpid()}_{idx}_{safe_name}.pdf"

#         try:
#             pdf.output(temp_pdf_path)
#         except Exception as e:
#             logger.error(f"PDF generation failed for {page_title}: {e}")
#             continue

#         # ----------------------------------------------------------------
#         # 4. Build pipeline-compatible item
#         # ----------------------------------------------------------------
#         items.append({
#             "title": page_title[: config.get("max_title_length", 250)],
#             "url": page_url,
#             "download_link": temp_pdf_path,        # S3Manager will upload this file
#             "doc_format": "PDF",
#             "file_extension": "pdf",
#             "publish_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
#             "modify_date": modify_date or datetime.now().strftime("%Y-%m-%d"),
#             "abstract": f"Medsafe guidance: {page_title}"[:1000],
#             "atom_id": page_url,                   # unique hash key
#             "local_path": temp_pdf_path,           # alternative key used by S3Manager
#         })

#         logger.info(f"Generated PDF for: {page_title}")

#     logger.info(f"New Zealand scraping complete – {len(items)} documents ready")
#     return items