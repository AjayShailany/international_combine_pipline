# countries/sg.py 
import requests  
from bs4 import BeautifulSoup
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import random
from utils.file_helper import normalize_date, get_doc_format


def scrape_data(config, logger):
    """
    Scrape Singapore HSA Medical Devices Guidance Documents.
    Returns list of dicts compatible with the rest of the pipeline.
    """
    base_url = config["url"]
    logger.info(f"Scraping Singapore HSA: {base_url}")

    items = []
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })

    # Fetch page
    try:
        res = session.get(base_url, timeout=30)
        res.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch Singapore page: {e}")
        return items

    soup = BeautifulSoup(res.text, "lxml")
    sections = soup.select("h2")

    if not sections:
        logger.warning("No <h2> sections found on Singapore page.")
        return items

    total_pdfs = 0

    for sec in sections:
        outer_topic = sec.get_text(strip=True)
        logger.info(f"MAIN TOPIC: {outer_topic}")

        next_block = sec.find_next_sibling()

        while next_block and next_block.name not in ["h2", "h1"]:
            sub_head = next_block.select_one("h3, strong")
            sub_topic = sub_head.get_text(strip=True) if sub_head else outer_topic

            # Find PDF links
            links = next_block.select("a[href*='.pdf'], a[data-file*='.pdf'], a[data-href*='.pdf']")

            for link in links:
                href = (
                    link.get("href")
                    or link.get("data-file")
                    or link.get("data-href")
                )

                if not href:
                    continue

                # Resolve full URL
                if href.startswith('/'):
                    download_link = "https://www.hsa.gov.sg" + href
                elif href.startswith('http'):
                    download_link = href
                else:
                    continue

                # Title
                title = link.get_text(strip=True)
                if not title:
                    title = href.split('/')[-1].replace('.pdf', '').replace('_', ' ')

                topic_full = (
                    f"{outer_topic} - {sub_topic}"
                    if sub_topic != outer_topic
                    else outer_topic
                )

                if len(title) > 50:
                    title = topic_full

                # Extract file format & extension
                ext, fmt_upper = get_doc_format(download_link)
                if ext not in ['pdf']:
                    logger.info(f"Skipping non-PDF: {ext} - {download_link}")
                    continue

                total_pdfs += 1
                logger.info(f"[{total_pdfs}] Found: {title[:80]}...")

                # Append final document
                items.append({
                    'title': title[:config.get('max_title_length', 200)],
                    'url': download_link,             # <<<<<< UPDATED
                    'download_link': download_link,
                    'doc_format': fmt_upper,
                    'file_extension': ext,
                    'publish_date': None,
                    'modify_date': None,
                    'abstract': topic_full,
                    'atom_id': download_link
                })

                # Delay to avoid blocking
                time.sleep(random.uniform(0.5, 1.5))

            next_block = next_block.find_next_sibling()

    logger.info(f"Singapore scraping complete. Found {len(items)} documents.")
    return items










# # countries/sg.py woking good 
# import requests
# from bs4 import BeautifulSoup
# import logging
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# import time
# import random
# from utils.file_helper import normalize_date, get_doc_format


# def scrape_data(config, logger):
#     """
#     Scrape Singapore HSA Medical Devices Guidance Documents.
#     Returns list of dicts compatible with the rest of the pipeline.
#     """
#     base_url = config["url"]
#     logger.info(f"Scraping Singapore HSA: {base_url}")

#     items = []
#     session = requests.Session()
#     retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
#     session.mount('https://', HTTPAdapter(max_retries=retries))
#     session.headers.update({
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
#     })

#     try:
#         res = session.get(base_url, timeout=30)
#         res.raise_for_status()
#     except Exception as e:
#         logger.error(f"Failed to fetch Singapore page: {e}")
#         return items

#     soup = BeautifulSoup(res.text, "lxml")
#     sections = soup.select("h2")
#     if not sections:
#         logger.warning("No <h2> sections found on Singapore page.")
#         return items

#     total_pdfs = 0
#     for sec in sections:
#         outer_topic = sec.get_text(strip=True)
#         logger.info(f"MAIN TOPIC: {outer_topic}")

#         next_block = sec.find_next_sibling()
#         while next_block and next_block.name not in ["h2", "h1"]:
#             sub_head = next_block.select_one("h3, strong")
#             sub_topic = sub_head.get_text(strip=True) if sub_head else outer_topic

#             links = next_block.select("a[href*='.pdf'], a[data-file*='.pdf'], a[data-href*='.pdf']")
#             for link in links:
#                 href = link.get("href") or link.get("data-file") or link.get("data-href")
#                 if not href:
#                     continue

#                 # Resolve relative URLs
#                 if href.startswith('/'):
#                     download_link = "https://www.hsa.gov.sg" + href
#                 elif href.startswith('http'):
#                     download_link = href
#                 else:
#                     continue  # skip malformed

#                 title = link.get_text(strip=True)
#                 if not title:
#                     title = href.split('/')[-1].replace('.pdf', '').replace('_', ' ')

#                 topic_full = f"{outer_topic} - {sub_topic}" if sub_topic != outer_topic else outer_topic
#                 if len(title) > 50:
#                     title = topic_full  # fallback

#                 # Get file extension and format
#                 ext, fmt_upper = get_doc_format(download_link)
#                 if ext not in ['pdf']:
#                     logger.info(f"Skipping non-PDF: {ext} - {download_link}")
#                     continue

#                 total_pdfs += 1
#                 logger.info(f"[{total_pdfs}] Found: {title[:80]}...")

#                 items.append({
#                     'title': title[:config.get('max_title_length', 200)],
#                     'url': base_url,
#                     'download_link': download_link,
#                     'doc_format': fmt_upper,
#                     'file_extension': ext,
#                     'publish_date': None,  # fallback
#                     'modify_date': None,
#                     'abstract': topic_full,
#                     'atom_id': download_link  # unique identifier
#                 })

#                 # Be respectful
#                 time.sleep(random.uniform(0.5, 1.5))

#             next_block = next_block.find_next_sibling()

#     logger.info(f"Singapore scraping complete. Found {len(items)} documents.")
#     return items