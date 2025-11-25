import hashlib
import logging
import pandas as pd
from datetime import datetime
from db_connect import run_query_to_list_of_dicts, insert_append_table_with_df, run_query_insert_update
from config import Config
from utils.file_helper import clean_title


class DatabaseManager:
    def __init__(self, country_config):
        self.config = country_config
        self.logger = logging.getLogger(country_config.get('country', 'DB'))
        self.table = Config.DB_TABLE

    def get_max_docket_number(self):
        """Get next docket number based on docket_prefix and agency_id."""
        prefix = self.config['docket_prefix']
        prefix_len = len(prefix)
        
        query = f"""
            SELECT MAX(CAST(SUBSTRING(docket_id, :prefix_len) AS UNSIGNED)) AS max_num
            FROM {self.table}
            WHERE docket_id LIKE :prefix_pattern AND agency_id = :agency_id
        """
        success, result = run_query_to_list_of_dicts(query, {
            'prefix_len': prefix_len + 2,
            'prefix_pattern': f"{prefix}-%",
            'agency_id': self.config['agency_id']
        })
        
        return result[0]['max_num'] if success and result and result[0]['max_num'] else 0

    def assign_document_ids(self, items, cfg):
        if not items:
            return items

        valid_items = []
        for item in items:
            pub = item.get('publish_date')

            if isinstance(pub, str):
                try:
                    pub_obj = datetime.strptime(pub, "%Y-%m-%d").date()
                except:
                    pub_obj = None
            else:
                pub_obj = pub
            item['publish_date_obj'] = pub_obj

            # modify_date normalization
            mod = item.get('modify_date')
            if not mod:
                mod = pub
            if isinstance(mod, str):
                try:
                    mod_obj = datetime.strptime(mod, "%Y-%m-%d").date()
                except:
                    mod_obj = pub_obj
            else:
                mod_obj = mod
            item['modify_date'] = mod_obj.strftime('%Y-%m-%d') if mod_obj else None

            # doc_hash calculation
            hash_input = item.get('atom_id') or f"{clean_title(item['title'])}{item['url']}"
            item['doc_hash'] = hashlib.md5(hash_input.encode()).hexdigest()

            valid_items.append(item)

        items = valid_items
        if not items:
            return []

        # get next new docket number
        max_num = self.get_max_docket_number()
        next_docket_num = max_num + 1

        existing_items = []
        new_items = []

        for item in items:
            query = f"""
                SELECT docket_id, doc_id, publish_date, modifyDate, doc_hash
                FROM {self.table}
                WHERE doc_hash = :doc_hash
            """
            success, result = run_query_to_list_of_dicts(query, {'doc_hash': item['doc_hash']})
            if not success or not result:
                new_items.append(item)
                continue

            old = result[0]
            old_pub = old.get("publish_date")
            new_pub = item.get("publish_date")

            old_mod = old.get("modifyDate")
            new_mod = item.get("modify_date")

            def _dt(x):
                try:
                    return datetime.strptime(x, "%Y-%m-%d").date() if x else None
                except:
                    return None

            old_pub = _dt(old_pub)
            new_pub = _dt(new_pub)
            old_mod = _dt(old_mod)
            new_mod = _dt(new_mod)

            needs_update = False

            # CASE 1: document has dates
            if new_pub or new_mod:
                if old_pub != new_pub or old_mod != new_mod:
                    needs_update = True
            else:
                # CASE 2: no dates -> only compare file content by S3 later
                needs_update = True

            item.update({
                'docket_id': old['docket_id'],
                'doc_id': old['doc_id'],
                'is_new': False,
                'needs_update': needs_update
            })
            existing_items.append(item)

        # sort new items
        new_items.sort(key=lambda x: x.get('publish_date_obj') or datetime.min)

        # assign docket ids
        for item in new_items:
            docket_id = f"{self.config['docket_prefix']}-{next_docket_num}"
            doc_id = f"{docket_id}-1"
            item.update({
                'docket_id': docket_id,
                'doc_id': doc_id,
                'is_new': True,
                'needs_update': False
            })
            next_docket_num += 1

        return existing_items + new_items


    def prepare_records(self, items):
        """Prepare database records from items, filtering out unchanged ones."""
        records = []
        now = datetime.now()

        for item in items:
            if not item.get('is_new') and not item.get('needs_update'):
                self.logger.info(f"Skipped (no change): {item['doc_id']}")
                continue

            record = {
                'docket_id': item.get('docket_id'),
                'doc_id': item['doc_id'],
                'doc_hash': item['doc_hash'],
                'document_type': self.config['document_type'],
                'agency_id': self.config['agency_id'],
                'reference': '',
                'title': clean_title(item['title']),
                'url': item['url'],
                'abstract': item.get('abstract', '')[:1000],
                'program_id': self.config['program_id'],
                'publish_date': item['publish_date'],
                'modifyDate': item['modify_date'],  # From scraper or publish_date
                'effective_date': None,
                'doc_format': item['doc_format'],
                's3_country_folder': self.config['s3_country_folder'],
                'aws_bucket': Config.AWS_BUCKET,
                'aws_key': item['aws_key'],
                's3_link_url': item['s3_link_url'],
                'in_elastic': None,
                'create_date': now if item.get('is_new') else item.get('create_date'),
                'modified_date': now
            }
            records.append(record)

        return records


    def save_documents(self, items):
        """Insert new documents and update existing ones."""
        if not items:
            self.logger.info("No items to save")
            return

        records = self.prepare_records(items)
        if not records:
            self.logger.info("No new or updated records to save")
            return

        df = pd.DataFrame(records)

        existing_query = f"SELECT doc_id FROM {self.table} WHERE doc_id IN ({','.join([f':id_{i}' for i in range(len(df))])})" if len(df) > 0 else f"SELECT doc_id FROM {self.table} WHERE 1=0"
        
        if len(df) > 0:
            params = {f'id_{i}': doc_id for i, doc_id in enumerate(df['doc_id'].tolist())}
            success, existing = run_query_to_list_of_dicts(existing_query, params)
        else:
            existing = []

        existing_ids = [row['doc_id'] for row in existing] if existing else []

        new_df = df[~df['doc_id'].isin(existing_ids)]
        update_df = df[df['doc_id'].isin(existing_ids)]

        # INSERT
        if not new_df.empty:
            success, msg = insert_append_table_with_df(new_df, self.table)
            if success:
                self.logger.info(f"Inserted {len(new_df)} new documents")
            else:
                self.logger.error(f"Insert failed: {msg}")

        updated_count = 0
        unchanged_count = 0

        # UPDATE
        for _, row in update_df.iterrows():
            row_dict = row.to_dict()

            # Fetch existing data from DB for comparison
            check_sql = f"SELECT aws_key, s3_link_url, modifyDate FROM {self.table} WHERE doc_id=:doc_id"
            success, old_record_list = run_query_to_list_of_dicts(check_sql, {"doc_id": row_dict["doc_id"]})
            old = old_record_list[0] if old_record_list else None

            # Prepare row values
            row_dict = {k: (str(v) if pd.notna(v) else None) for k, v in row_dict.items()}

            # Compare values ONLY for log accuracy
            if old and (
                old["aws_key"] == row_dict["aws_key"] and
                old["s3_link_url"] == row_dict["s3_link_url"] and
                old["modifyDate"] == row_dict["modifyDate"]
            ):
                unchanged_count += 1
                continue  # do the update logic but no log

            # run update (logic unchanged)
            update_sql = f"""
                UPDATE {self.table} SET
                    aws_key = :aws_key,
                    s3_link_url = :s3_link_url,
                    modifyDate = :modifyDate,
                    modified_date = NOW()
                WHERE doc_id = :doc_id
            """

            run_query_insert_update(update_sql, row_dict)
            updated_count += 1

        # Correct logs
        if updated_count > 0:
            self.logger.info(f"Updated {updated_count} documents")
        if unchanged_count > 0:
            self.logger.info(f"Unchanged {unchanged_count} documents (no DB update needed)")

        self.logger.info("DB save completed")

