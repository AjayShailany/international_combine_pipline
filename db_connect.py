from config import Config
from sqlalchemy import text, create_engine
import pandas as pd
import logging

def get_engine():
    """Create SQLAlchemy engine using Config"""
    conn_str = Config.get_db_connection_string()
    return create_engine(conn_str)

def run_query(query_str, params=None):
    """Execute a SQL query with optional parameters using SQLAlchemy."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query_str), params or {})
            return True, result.fetchall()
    except Exception as e:
        logging.error(e)
        return False, None
    finally:
        engine.dispose()


def run_query_insert_update(query_str, params=None):
    """Execute INSERT/UPDATE/DELETE with proper parameter binding."""
    engine = get_engine()
    try:
        with engine.begin() as conn:
            if params:
                result = conn.execute(text(query_str), params)
            else:
                result = conn.execute(text(query_str))
            
            query_type = query_str.strip().upper().split()[0]
            if query_type in ("INSERT", "UPDATE", "DELETE"):
                conn.commit()
                return True, None
            elif query_type == "SELECT":
                return True, result.fetchall()
            return True, None
    except Exception as e:
        logging.error(f"Database error: {e}")
        return False, None
    finally:
        engine.dispose()


def run_query_to_df(query_str):
    """Execute a query and return the result as a Pandas DataFrame."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query_str), conn)
            return True, result
    except Exception as e:
        logging.error(e)
        return False, None
    finally:
        engine.dispose()


def insert_append_table_with_df(df, table_name):
    """Insert DataFrame into MySQL table with validation."""
    if df is None or df.empty:
        logging.warning(f"Empty DataFrame for table {table_name}")
        return True, "No data to insert"
    
    engine = get_engine()
    try:
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
            return True, f"Inserted {len(df)} rows"
    except Exception as e:
        logging.error(f"Insert failed for {table_name}: {e}")
        return False, f"Unable to insert: {str(e)}"
    finally:
        engine.dispose()


def run_query_to_json(query_str, params=None):
    """Execute a SQL query with optional parameters and return results as JSON."""
    engine = get_engine()  
    try:
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query_str), conn, params=params or {})
            result = result.to_json(orient="records")
            return True, result
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return False, f"Unable to execute query: {str(e)}"
    finally:
        engine.dispose()


def run_query_to_list_of_dicts(query_str, params=None):
    """Execute query and return results as list of dictionaries."""
    engine = get_engine()  
    try:
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query_str), conn, params=params or {})
            result_list = result.to_dict(orient="records")
            return True, result_list
    except Exception as e:
        logging.error(f"Query failed: {e}")
        return False, None
    finally:
        engine.dispose()


def run_query_insert_with_id(query_str, params=None):
    """Insert a row and return the last inserted ID (MySQL version)."""
    engine = get_engine()
    try:
        with engine.begin() as conn:
            result = conn.execute(text(query_str), params or {})
            return True, result.lastrowid
    except Exception as e:
        logging.error(f"Insert failed: {e}")
        return False, None
    finally:
        engine.dispose()


def replace_table_with_df(df, table_name):
    """Replace entire table with DataFrame."""
    if df is None or df.empty:
        logging.warning(f"Empty DataFrame for table {table_name}")
        return True, "No data to replace"
    
    engine = get_engine()  
    try:
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            return True, "Data replaced"
    except Exception as e:
        logging.error(f"Replace failed: {e}")
        return False, f"Unable to replace: {str(e)}"
    finally:
        engine.dispose()
