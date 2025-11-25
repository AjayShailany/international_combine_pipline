import os
import re
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ----------------------------------------------------------------------
#  CONSTANTS
# ----------------------------------------------------------------------
BASE_URL = "https://www.imdrf.org"
GHTF_BASE = "https://www.imdrf.org/documents/ghtf-final-documents/"
IMDRF_LIST_URL = "https://www.imdrf.org/documents/library?f%5B0%5D=type%3Atechnical_document"

GHTF_GROUPS = [
    {"slug": "ghtf-study-group-1-pre-market-evaluation", "title": "Study Group 1 - Pre-market Evaluation"},
    {"slug": "ghtf-study-group-2-post-market-surveillancevigilance", "title": "Study Group 2 - Post-market Surveillance/Vigilance"},
    {"slug": "ghtf-study-group-3-quality-systems", "title": "Study Group 3 - Quality Systems"},
    {"slug": "ghtf-study-group-4-auditing", "title": "Study Group 4 - Auditing"},
    {"slug": "ghtf-study-group-5-clinical-safetyperformance", "title": "Study Group 5 - Clinical Safety/Performance"},
]


# ----------------------------------------------------------------------
#  HELPERS
# ----------------------------------------------------------------------
def normalize_date(date_str: Optional[str]) -> Optional[str]:
    """Convert various date formats to YYYY-MM-DD."""
    if not date_str:
        return None
    date_str = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", date_str.strip())
    date_formats = [
        "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y",
        "%d %B %Y", "%d %b %Y", "%B %d %Y", "%b %d %Y"
    ]
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def _get_doc_format(ext: str) -> str:
    """Return readable format name based on file extension."""
    return {
        "pdf": "PDF",
        "doc": "Word",
        "docx": "Word",
        "xls": "Excel",
        "xlsx": "Excel",
    }.get(ext.lower(), "Unknown")


def _init_selenium_driver() -> webdriver.Chrome:
    """Initialize Selenium Chrome WebDriver with optimized options."""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")   # ✅ ADD THIS LINE
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-notifications")
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver


# ----------------------------------------------------------------------
#  GHTF Scraper
# ----------------------------------------------------------------------
def _scrape_ghtf(driver: webdriver.Chrome, logger: logging.Logger) -> List[Dict[str, Any]]:
    """Scrape GHTF documents using Selenium."""
    items: List[Dict[str, Any]] = []

    for group in GHTF_GROUPS:
        page_url = urljoin(GHTF_BASE, group["slug"])
        logger.info(f"GHTF Group: {group['title']} ({page_url})")

        try:
            driver.get(page_url)
            time.sleep(2)
            
            # Wait for content to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "file-collection"))
            )
            
            html = driver.page_source
        except TimeoutException as e:
            logger.error(f"Timeout loading {page_url}: {e}")
            continue
        except Exception as e:
            logger.error(f"Failed to fetch {page_url}: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")

        containers = soup.find_all("div", class_="file-collections file-collections--container")
        tech_container = next(
            (c for c in containers if c.find("h2") and "technical documents" in c.find("h2").get_text(strip=True).lower()),
            None
        )
        if not tech_container:
            logger.info(f"No technical documents found in {group['title']}")
            continue

        for block in tech_container.select("div.file-collection-type-main.file-collection"):
            title_div = block.find("div", class_="file-collection__header")
            doc_title = title_div.get_text(strip=True) if title_div else "Untitled"

            info_div = block.find("div", class_="file-collection__info")
            posted_raw = None
            if info_div:
                m = re.search(r"Date posted:\s*(.+)", info_div.get_text(strip=True), re.I)
                posted_raw = m.group(1).strip() if m else None

            all_links = [a["href"] for a in block.select("ul.file-collection__files a[href]")]
            pdf_links = [urljoin(page_url, h) for h in all_links if h.lower().endswith(".pdf")]
            doc_links = [urljoin(page_url, h) for h in all_links if h.lower().endswith((".doc", ".docx"))]
            file_links = pdf_links if pdf_links else doc_links
            if not file_links:
                continue

            publish_date = normalize_date(posted_raw)
            for href in file_links:
                ext = os.path.splitext(href)[1][1:].lower()
                items.append({
                    "title": doc_title,
                    "url": page_url,
                    "download_link": href,
                    "file_extension": ext,
                    "doc_format": _get_doc_format(ext),
                    "publish_date": publish_date,
                    "modify_date": publish_date,
                    "source": group["title"]
                })
        time.sleep(1)

    logger.info(f"GHTF scraping complete. Total: {len(items)}")
    return items


# ----------------------------------------------------------------------
#  IMDRF Technical Documents Scraper
# ----------------------------------------------------------------------
def _scrape_imdrf(driver: webdriver.Chrome, logger: logging.Logger) -> List[Dict[str, Any]]:
    """Scrape IMDRF technical documents with pagination using Selenium."""
    items: List[Dict[str, Any]] = []
    page = 0

    while True:
        list_page = f"{IMDRF_LIST_URL}&page={page}"
        logger.info(f"IMDRF page {page + 1}: {list_page}")
        
        try:
            driver.get(list_page)
            time.sleep(2)
            
            # Wait for articles to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.node--type-technical_document"))
            )
            
            html = driver.page_source
        except TimeoutException:
            logger.warning(f"Timeout on page {page + 1}, no more articles found")
            break
        except Exception as e:
            logger.error(f"Failed to load page {page + 1}: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")
        articles = soup.select("article.node--type-technical_document")
        
        if not articles:
            logger.info(f"No articles found on page {page + 1}")
            break

        for art in articles:
            try:
                title_a = art.select_one("h3.teaser__title a")
                if not title_a:
                    continue
                title = title_a.get_text(strip=True)
                doc_url = urljoin(BASE_URL, title_a["href"])

                code = art.select_one("div.field--name-field-doc-code")
                document_code = code.get_text(strip=True) if code else None

                date_tag = art.select_one("div.field--name-field-published-date time")
                publish_date = None
                if date_tag and date_tag.has_attr("datetime"):
                    publish_date = date_tag["datetime"][:10]
                else:
                    publish_date = normalize_date(date_tag.get_text(strip=True) if date_tag else None)

                # Visit document page with Selenium
                try:
                    driver.get(doc_url)
                    time.sleep(1.5)
                    doc_html = driver.page_source
                except Exception as e:
                    logger.warning(f"Failed to load document page {doc_url}: {e}")
                    continue

                doc_soup = BeautifulSoup(doc_html, "html.parser")

                all_links = [a["href"] for a in doc_soup.find_all("a", href=True)]
                pdf_links = [urljoin(BASE_URL, h) for h in all_links if h.lower().endswith(".pdf")]
                doc_links = [urljoin(BASE_URL, h) for h in all_links if h.lower().endswith((".doc", ".docx"))]
                file_url = pdf_links[0] if pdf_links else (doc_links[0] if doc_links else None)
                if not file_url:
                    continue

                ext = os.path.splitext(file_url)[1][1:].lower()
                modify_date = None
                header = doc_soup.select_one("div.file-collections__header")
                if header:
                    date_el = header.select_one("time")
                    if date_el and date_el.has_attr("datetime"):
                        modify_date = date_el["datetime"][:10]
                if not modify_date:
                    modify_date = publish_date

                items.append({
                    "title": title,
                    "document_code": document_code,
                    "url": doc_url,
                    "download_link": file_url,
                    "file_extension": ext,
                    "doc_format": _get_doc_format(ext),
                    "publish_date": publish_date,
                    "modify_date": modify_date,
                    "source": "IMDRF Technical Documents"
                })
                logger.info(f"Extracted: {title} (Code: {document_code})")
                
            except Exception as e:
                logger.error(f"Error processing article: {e}")
                continue

            time.sleep(0.5)

        # Check for next page
        try:
            driver.get(list_page)
            time.sleep(1)
            next_button = driver.find_element(By.CSS_SELECTOR, "li.pager__item--next a")
            if not next_button:
                break
        except NoSuchElementException:
            logger.info("No next page found, stopping pagination")
            break
        except Exception as e:
            logger.warning(f"Error checking for next page: {e}")
            break

        page += 1
        time.sleep(1)

    logger.info(f"IMDRF scraping complete. Total: {len(items)}")
    return items


# ----------------------------------------------------------------------
#  Pipeline Entry Point
# ----------------------------------------------------------------------
def scrape_data(config, logger: logging.Logger) -> List[Dict[str, Any]]:
    """Pipeline entry point: returns all required IMDRF and GHTF documents using Selenium."""
    logger.info("Starting combined IMDRF + GHTF scraping with Selenium")
    
    driver = _init_selenium_driver()
    
    try:
        ghtf_docs = _scrape_ghtf(driver, logger)
        logger.info(f"GHTF Documents collected: {len(ghtf_docs)}")
        
        imdrf_docs = _scrape_imdrf(driver, logger)
        logger.info(f"IMDRF Documents collected: {len(imdrf_docs)}")

        all_docs = ghtf_docs + imdrf_docs
        logger.info(f"Combined scraping complete. Total: {len(all_docs)} documents")

        return all_docs
    finally:
        driver.quit()
        logger.info("Selenium driver closed")


# Test runner
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    docs = scrape_data({}, logger)
    print(f"\n\n{'='*60}")
    print(f"Total Documents Scraped: {len(docs)}")
    print(f"{'='*60}")
    for doc in docs:
        print(f"✓ {doc['title'][:50]} | Source: {doc['source']}")
