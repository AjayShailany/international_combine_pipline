"""
Chile – ISPCH (Instituto de Salud Pública de Chile) scraper.
Pipeline-compatible. Uses Selenium in headless mode for diprece.minsal.cl (403 bypass).
Maps:
  - descripción_norma → title
  - fecha_publicación_en_d.o._o_fecha_dictación → publish_date
  - modificaciones → modify_date
  - enlace → download_link (resolved via Selenium if needed)
No local files. No JSON. No folder creation.
"""

import os
import re
import logging
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Selenium (headless, Lightsail-safe)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
BASE_URL = "https://www.ispch.gob.cl/normativa-andid/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    "Referer": "https://www.ispch.gob.cl/",
}


# ----------------------------------------------------------------------
# Lightsail-safe Selenium setup (headless, no sandbox, no GPU)
# ----------------------------------------------------------------------
_driver = None
def get_driver():
    global _driver
    if _driver is None:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")

        service = Service(ChromeDriverManager().install())
        _driver = webdriver.Chrome(service=service, options=chrome_options)
        _driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false});")
        _driver.set_page_load_timeout(60)
    return _driver


# ----------------------------------------------------------------------
# Date parser
# ----------------------------------------------------------------------
def _parse_date(date_str: str) -> str:
    if not date_str or not date_str.strip():
        return datetime.now().strftime("%Y-%m-%d")
    date_str = date_str.strip()
    patterns = ["%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d.%m.%Y"]
    for fmt in patterns:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    match = re.search(r"\b(20\d{2})\b", date_str)
    return f"{match.group(1)}-01-01" if match else datetime.now().strftime("%Y-%m-%d")


# ----------------------------------------------------------------------
# Resolve PDF URL (Selenium for diprece.minsal.cl, requests for LeyChile)
# ----------------------------------------------------------------------
def resolve_pdf_url(original_url: str, session: requests.Session, logger: logging.Logger) -> str:
    if not original_url:
        return ""

    # Case 1: Direct PDF
    if original_url.lower().endswith(".pdf"):
        return original_url

    # Case 2: LeyChile Exportar button
    if "leychile.cl" in original_url:
        try:
            resp = session.get(original_url, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            btn = soup.find("a", href=lambda h: h and "Exportar" in h and "pdf" in h.lower())
            if btn and btn.get("href"):
                pdf_url = urljoin(original_url, btn["href"])
                logger.debug(f"LeyChile PDF: {pdf_url}")
                return pdf_url
        except Exception as e:
            logger.debug(f"LeyChile parse failed: {e}")

    # Case 3: diprece.minsal.cl → use Selenium (bypass 403)
    if "diprece.minsal.cl" in original_url:
        driver = get_driver()
        try:
            logger.debug(f"Using Selenium for diprece: {original_url}")
            driver.get(original_url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            current = driver.current_url
            if current.lower().endswith(".pdf"):
                return current
            # Try to find PDF embed or link
            pdf_link = driver.find_element(By.CSS_SELECTOR, "a[href*='.pdf'], embed[src*='.pdf'], iframe[src*='.pdf']")
            src = pdf_link.get_attribute("href") or pdf_link.get_attribute("src")
            if src:
                return urljoin(original_url, src)
        except Exception as e:
            logger.debug(f"Selenium failed for diprece: {e}")
        return original_url  # fallback

    # Case 4: Follow redirect
    try:
        resp = session.head(original_url, allow_redirects=True, timeout=15)
        if resp.url.lower().endswith(".pdf"):
            return resp.url
    except:
        pass

    return ""


# ----------------------------------------------------------------------
# Main scraper – pipeline compatible
# ----------------------------------------------------------------------
def scrape_data(config, logger: logging.Logger):
    base_url = config.get("url") or BASE_URL
    logger.info(f"Scraping Chile – ISPCH ({base_url})")

    items = []

    # Session with retry
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(HEADERS)

    # Fetch main page
    try:
        resp = session.get(base_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch ISPCH page: {e}")
        return items

    soup = BeautifulSoup(resp.text, "lxml")

    # Parse all tables: MD, IVD, and others
    tbody_ids = ["page2", "page3", "page4"]
    all_rows = []

    for tbody_id in tbody_ids:
        tbody = soup.find("tbody", id=tbody_id)
        if not tbody:
            continue
        rows = tbody.find_all("tr")
        if len(rows) < 2:
            continue

        header_cells = rows[0].find_all("th")
        headers = [th.get_text(strip=True).lower() for th in header_cells]
        data_rows = rows[1:]

        for r in data_rows:
            cells = r.find_all("td")
            if len(cells) != len(headers):
                continue
            row = {}
            for i, cell in enumerate(cells):
                raw_key = headers[i]
                key = re.sub(r"[^a-z0-9_]", "", raw_key.replace(" ", "_").replace("/", "_"))
                a = cell.find("a")
                text = cell.get_text(strip=True)
                if a and a.get("href"):
                    href = a["href"]
                    if href.startswith("//"):
                        href = "https:" + href
                    elif href.startswith("/"):
                        href = urljoin(base_url, href)
                    row[key] = {"text": text, "url": href}
                else:
                    row[key] = text
            all_rows.append(row)

    if not all_rows:
        logger.warning("No rows found in any table")
        return items

    logger.info(f"Found {len(all_rows)} normative documents")

    # Process each row
    for idx, row in enumerate(all_rows, start=1):
        try:
            # Title from descripción_norma
            desc_obj = row.get("descripcion_norma") or row.get("descripcin_norma")
            title = "Sin título"
            if desc_obj:
                title = desc_obj.get("text") if isinstance(desc_obj, dict) else desc_obj
            title = re.sub(r"\s+", " ", title).strip()
            if not title or title.lower() in ["sin título", ""]:
                title = f"Normativa {row.get('nmero_normativa', 'unknown')}"

            # Publish date
            pub_key = "fecha_publicacin_en_do_o_fecha_dictacin"
            pub_raw = row.get(pub_key, "")
            pub_text = pub_raw.get("text") if isinstance(pub_raw, dict) else pub_raw
            publish_date = _parse_date(pub_text)

            # Modify date
            mod_raw = row.get("modificaciones", "")
            mod_text = mod_raw.get("text") if isinstance(mod_raw, dict) else mod_raw
            modify_date = _parse_date(mod_text)

            # Link
            link_obj = row.get("enlace") or row.get("link")
            if not link_obj or not isinstance(link_obj, dict):
                logger.debug(f"Row {idx}: no valid link")
                continue

            original_url = link_obj["url"]
            pdf_url = resolve_pdf_url(original_url, session, logger)
            if not pdf_url:
                logger.debug(f"Row {idx}: failed to resolve PDF: {original_url}")
                continue

            # Clean title
            clean_title = re.sub(r'[<>:"/\\|?*]', '_', title)
            clean_title = re.sub(r"\s+", " ", clean_title).strip()
            max_len = config.get("max_title_length", 250)
            if len(clean_title) > max_len:
                clean_title = clean_title[: max_len - 3] + "..."

            item = {
                "title": clean_title,
                "url": base_url,
                "download_link": pdf_url,
                "doc_format": "PDF",
                "file_extension": "pdf",
                "publish_date": publish_date,
                "modify_date": modify_date,
                "abstract": f"ISPCH Chile: {clean_title}",
                "atom_id": pdf_url,
            }

            items.append(item)
            logger.info(f"[{idx}] {clean_title[:70]}...")

        except Exception as e:
            logger.warning(f"Error processing row {idx}: {e}")
            continue

    # Cleanup
    if _driver:
        try:
            _driver.quit()
        except:
            pass

    logger.info(f"Chile scraping complete – {len(items)} valid documents")
    return items