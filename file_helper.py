# utils/file_helper.py
import re
from datetime import datetime
import logging

def clean_title(title):
    return re.sub(r'[<>:"/\\|?*]', '', title).strip()

def normalize_date(date_str):
    import re
    from datetime import datetime

    if not date_str:
        return None

    # Try common RSS/Atom/HTTP datetime formats first
    rss_formats = [
        "%a, %d %b %Y %H:%M:%S %z",   # e.g. Wed, 12 Nov 2025 09:29:23 +0100
        "%a, %d %b %Y %H:%M:%S GMT",  # e.g. Wed, 12 Nov 2025 09:29:23 GMT
    ]
    for fmt in rss_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    # --- your existing logic below (unchanged) ---
    # Remove any words after the date
    # Keep only numbers, letters and delimiters
    match = re.match(r"[A-Za-z0-9 ./,\-]+", date_str)
    if not match:
        return None
    date_str = match.group(0)

    # Remove ordinal suffixes (st, nd, rd, th)
    date_str = re.sub(r"(st|nd|rd|th)", "", date_str, flags=re.IGNORECASE)
    date_str = date_str.strip().replace(",", " ")

    date_formats = [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d %B %Y",
        "%d %b %Y",
        "%B %d %Y",
        "%b %d %Y",
        "%d %B %y",
        "%d %b %y",
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    return None

def get_doc_format(url: str):
    import os
    from urllib.parse import urlparse, parse_qs

    # First try from query param ?filename=abc.pdf
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    filename = qs.get('filename', [None])[0]
    if filename:
        ext = os.path.splitext(filename)[1].lower().replace('.', '')
    else:
        # Fallback to URL path
        path = os.path.basename(parsed.path)
        ext = os.path.splitext(path)[1].lower().replace('.', '')

    if ext in ['pdf', 'doc', 'docx', 'rtf', 'txt', 'html']:
        fmt_upper = ext.upper()
    else:
        fmt_upper = "UNKNOWN"

    return ext, fmt_upper