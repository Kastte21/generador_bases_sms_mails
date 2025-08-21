# app/database.py
import psycopg2
import polars as pl
from contextlib import contextmanager
from typing import List
from . import settings

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(**settings.DB_CONFIG)
        yield conn.cursor()
    finally:
        if conn:
            conn.close()

def get_campaign_discounts(cursor, dnis: List[str]) -> pl.DataFrame:
    if not dnis:
        return pl.DataFrame()
    
    query = """
        SELECT 
            idccliente, 
            dcto_reg, 
            dcto_sub, 
            dcto_ger 
        FROM campanias 
        WHERE idccliente = ANY(%s)
    """
    cursor.execute(query, (dnis,))
    
    if cursor.rowcount == 0:
        return pl.DataFrame()
        
    column_names = [desc[0] for desc in cursor.description]
    return pl.DataFrame(cursor.fetchall(), schema=column_names, orient="row")

def get_all_mails(cursor) -> pl.DataFrame:
    cursor.execute("SELECT idccliente, email, ranking FROM mails")
    if cursor.rowcount == 0:
        return pl.DataFrame()
    column_names = [desc[0] for desc in cursor.description]
    df = pl.DataFrame(cursor.fetchall(), schema=column_names, orient="row")
    return df

def get_campaign_details_for_mails(cursor, dnis: List[str]) -> pl.DataFrame:
    if not dnis:
        return pl.DataFrame()
    
    dnis_text = [str(dni).strip() for dni in dnis]
    
    query = """
        SELECT 
            idccliente, 
            dcto_reg, 
            dcto_sub, 
            dcto_ger,
            plazo_reg
        FROM campanias 
        WHERE idccliente = ANY(%s)
    """
    cursor.execute(query, (dnis_text,))
    
    if cursor.rowcount == 0:
        return pl.DataFrame()
        
    column_names = [desc[0] for desc in cursor.description]
    return pl.DataFrame(cursor.fetchall(), schema=column_names, orient="row")

# --- MAILS PERSONALES RANKING 1 ---
def get_rank_1_mails(cursor) -> pl.DataFrame:
    query = "SELECT idccliente, email FROM mails WHERE ranking = 1"
    cursor.execute(query)
    if cursor.rowcount == 0:
        return pl.DataFrame()
        
    column_names = [desc[0] for desc in cursor.description]
    return pl.DataFrame(cursor.fetchall(), schema=column_names)