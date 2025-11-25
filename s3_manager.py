# utils/s3_manager.py
import os
import boto3
import requests
import tempfile
import shutil
import hashlib
from urllib.parse import quote
from config import Config
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class S3Manager:
    def __init__(self, country_config):
        self.config = country_config
        self.s3 = boto3.client(
            's3',
            region_name=Config.AWS_REGION,
            aws_access_key_id=Config.AWS_ACCESS_KEY,
            aws_secret_access_key=Config.AWS_SECRET_KEY
        )
        self.bucket = Config.AWS_BUCKET
        self.logger = logging.getLogger(country_config.get('country', 'S3'))

    # --------------------------------------------------------------------- #
    # 1. Duplicate-aware upload
    # --------------------------------------------------------------------- #
    def _file_md5(self, path, chunk_size=8192):
        md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def upload_if_changed(self, local_path, s3_key):
        """
        Always upload the file to S3 (overwrite if exists).
        Returns True if uploaded.
        """
        try:
            # Try deleting existing object (optional, ensures clean overwrite)
            self.s3.delete_object(Bucket=self.bucket, Key=s3_key)
            self.logger.info(f"Deleted existing S3 object before upload: {s3_key}")
        except self.s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "404":
                self.logger.warning(f"Error deleting existing object: {e}")

        # ---- always upload ------------------------------------------------
        self.s3.upload_file(local_path, self.bucket, s3_key)
        s3_url = f"https://{self.bucket}.s3.{Config.AWS_REGION}.amazonaws.com/{quote(s3_key)}"
        self.logger.info(f"S3 uploaded (overwrite or new): {s3_key}")
        return True

    # --------------------------------------------------------------------- #
    # 2. Existing helpers (unchanged except using new upload)
    # --------------------------------------------------------------------- #
    def _prepare_local_file(self, source, dest_path):
        # … (exactly the same as you posted) …
            # (copy-paste your original implementation here – omitted for brevity)
        if not source:
            self.logger.error("No source provided")
            return False

        if isinstance(source, str) and source.lower().startswith(('http://', 'https://')):
            try:
                self.logger.info(f"Downloading from URL: {source}")

                session = requests.Session()
                retry = Retry(total=5, backoff_factor=1,
                              status_forcelist=[403, 429, 500, 502, 503],
                              allowed_methods=["GET"])
                session.mount("https://", HTTPAdapter(max_retries=retry))
                session.mount("http://", HTTPAdapter(max_retries=retry))

                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    )
                }
                if "diprece.minsal.cl" in source.lower():
                    self.logger.info("Applying Chile MoH headers")
                    headers.update({
                        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
                        "Referer": "https://diprece.minsal.cl/"
                    })

                resp = session.get(source, headers=headers, timeout=60, stream=True)
                resp.raise_for_status()

                with open(dest_path, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                size = os.path.getsize(dest_path)
                self.logger.info(f"Downloaded: {dest_path} ({size} bytes)")
                return True

            except Exception as e:
                self.logger.error(f"Download failed: {e}")
                return False

        else:
            # local file – just copy
            try:
                # Ensure destination directory exists
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                # Ensure source file actually exists
                if not os.path.exists(source):
                    self.logger.error(f"Source file not found: {source}")
                    return False

                shutil.copy2(source, dest_path)
                self.logger.info(f"Copied local file: {source} -> {dest_path}")
                return True

            except Exception as e:
                self.logger.error(f"Copy failed: {e}")
                return False

    def process_documents(self, items):
        processed = []
        temp_dir = tempfile.mkdtemp()
        self.logger.info(f"Temp directory: {temp_dir}")

        try:
            for item in items:
                try:
                    doc_id = item.get('doc_id')

                    # skip non-updated items
                    if item.get('is_new') is False and item.get('needs_update') is False:
                        self.logger.info(f"Skipping S3 upload (no update required): {doc_id}")
                        continue

                    ext = item.get('file_extension', 'pdf')
                    local_path = os.path.join(temp_dir, f"{doc_id}.{ext}")

                    source = (
                        item.get('download_link') or 
                        item.get('source_url') or
                        item.get('local_path') or
                        item.get('url')
                    )

                    if not source:
                        self.logger.warning(f"No source found for {doc_id}, skipping")
                        continue

                    if not self._prepare_local_file(source, local_path):
                        self.logger.warning(f"Failed to download/prepare file for {doc_id}")
                        continue

                    s3_key = self.config['folder_structure'].format(
                        base=self.config['base_s3_folder'],
                        country=self.config['s3_country_folder'].upper(),
                        agency_sub=self.config['agency_sub'],
                        docket_id=item.get('docket_id'),
                        doc_id=item.get('doc_id'),
                        ext=ext
                    )

                    uploaded = self.upload_if_changed(local_path, s3_key)

                    # always update s3_url mapping
                    s3_url = f"https://{self.bucket}.s3.{Config.AWS_REGION}.amazonaws.com/{quote(s3_key)}"
                    item.update({
                        "aws_key": s3_key,
                        "s3_link_url": s3_url,
                        "download_link": s3_url
                    })

                    processed.append(item)
                    os.remove(local_path)

                except Exception as e:
                    self.logger.error(f"Error processing document {item.get('doc_id')}: {e}")
                    continue

            self.logger.info(f"Total documents processed: {len(processed)}")
            return processed

        finally:
            # no cleanup
            pass

