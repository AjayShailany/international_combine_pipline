# run.py
import json
import logging
import sys
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import Config
from utils.logger import setup_logging
from utils.db_manager import DatabaseManager
from utils.s3_manager import S3Manager


def cleanup_temp_dirs(logger):
    """Delete all temporary directories created during this pipeline run."""
    temp_root = tempfile.gettempdir()
    logger.info("Cleaning up temporary directories...")
    try:
        for name in os.listdir(temp_root):
            path = os.path.join(temp_root, name)
            if os.path.isdir(path) and name.startswith("tmp"):
                try:
                    shutil.rmtree(path, ignore_errors=True)
                    logger.debug(f"Deleted: {path}")
                except Exception as e:
                    logger.warning(f"Could not delete {path}: {e}")
    except Exception as e:
        logger.warning(f"Failed to clean temp dirs: {e}")


# --------------------------------------------------------------------- #
# 1. Parallel per-country processing
# --------------------------------------------------------------------- #
def process_country(code, cfg, main_logger):
    """Run scraper ->DB ->S3 for ONE country. Returns (code, processed_count)"""
    cfg['country'] = code
    logger = setup_logging(code)

    try:
        # ---- 1. Scrape -------------------------------------------------
        mod = __import__(f"countries.{code.lower()}", fromlist=['scrape_data'])
        raw_items = mod.scrape_data(cfg, logger)
        if not raw_items:
            main_logger.warning(f"[{code}] No items scraped")
            return code, 0

        # ---- 2. DB + S3 ------------------------------------------------
        db = DatabaseManager(cfg)
        s3 = S3Manager(cfg)

        items = db.assign_document_ids(raw_items, cfg)
        processed = s3.process_documents(items)
        db.save_documents(processed)

        main_logger.info(f"[{code}] finished – {len(processed)} docs")
        return code, len(processed)

    except Exception as e:
        logger.error(f"[{code}] pipeline failed", exc_info=True)
        main_logger.error(f"[{code}] failed: {e}")
        return code, 0


# --------------------------------------------------------------------- #
def main():
    sys.stdout.reconfigure(encoding='utf-8')
    Config.validate()
    main_logger = setup_logging()          # main.log + console

    with open('countries.json') as f:
        countries = json.load(f)

    # ---- Parallel execution -----------------------------------------
    max_workers = min(10, len(countries))   # tune as you like
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {
            executor.submit(process_country, code, cfg.copy(), main_logger): code
            for code, cfg in countries.items()
        }
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                _, count = future.result()
                results[code] = count
            except Exception as exc:
                main_logger.error(f"[{code}] generated an exception: {exc}")
                results[code] = 0

    # ---- Summary ----------------------------------------------------
    total = sum(results.values())
    main_logger.info(f"Pipeline completed – processed {total} documents across {len(results)} countries")
    main_logger.info("Country summary: " + ", ".join(f"{c}:{n}" for c, n in results.items()))
        # ---- Final global cleanup --------------------------------------
    main_logger.info("Cleaning up all temporary directories after pipeline...")
    cleanup_temp_dirs(main_logger)
    main_logger.info("All temporary directories cleaned successfully.")


if __name__ == "__main__":
    main()
