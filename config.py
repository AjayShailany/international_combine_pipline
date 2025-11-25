# config.py
import os
from dotenv import load_dotenv

# Load .env (override existing vars)
load_dotenv(override=True)

class Config:
    # Database
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")
    DB_TABLE = os.getenv("DB_TABLE", "in_documents_migration")

    # AWS
    AWS_ACCESS_KEY = os.getenv("AWS_CUSTOM_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_CUSTOM_SECRET_ACCESS_KEY")
    AWS_BUCKET = os.getenv("AWS_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION_USED")

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @staticmethod
    def get_db_connection_string():
        return f"mysql+pymysql://{Config.DB_USER}:{Config.DB_PASSWORD}@{Config.DB_HOST}:3306/{Config.DB_NAME}"

    @staticmethod
    def validate():
        required = [
            Config.DB_HOST, Config.DB_USER, Config.DB_PASSWORD,
            Config.DB_NAME, Config.AWS_ACCESS_KEY, Config.AWS_SECRET_KEY,
            Config.AWS_BUCKET, Config.AWS_REGION
        ]
        missing = [k for k, v in locals().items() if k in required and not v]
        if missing:
            raise EnvironmentError(f"Missing env vars: {missing}")