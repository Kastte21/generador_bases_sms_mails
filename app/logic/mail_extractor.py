# app/logic/mail_extractor.py
import polars as pl
import logging

from app import database as db
from app.utils import file_utils
from app import settings

logger = logging.getLogger(__name__)

def _prepare_base_df(df: pl.DataFrame) -> pl.DataFrame:
    first_col_name = df.columns[0]
    if first_col_name.lower() != 'dni':
        df = df.rename({first_col_name: 'dni'})
        logger.info(f"Se renombró la columna '{first_col_name}' a 'dni'.")

    if 'CORREO' not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias('CORREO'))
        logger.info("Se creó la columna 'CORREO' en el DataFrame.")

    return df.with_columns(pl.col('dni').cast(pl.Utf8).str.strip_chars())

def _filter_personal_emails(df_mails: pl.DataFrame) -> pl.DataFrame:
    allowed_domains = settings.PERSONAL_EMAIL_DOMAINS
    domain_pattern = "|".join([d.replace(".", r"\.") for d in allowed_domains])
    
    pattern = rf"^[\w\.-]+@({domain_pattern})$"

    df_filtered = df_mails.filter(
        pl.col("email").str.contains(pattern, literal=False)
    )

    removed = len(df_mails) - len(df_filtered)
    logger.info(f"Se filtraron {len(df_filtered)} correos válidos con dominios personales exactos (se descartaron {removed}).")

    return df_filtered

def run_rank_1_mail_extraction():
    logger.info("="*60)
    logger.info("INICIANDO EXTRACCIÓN DE MAILS CON RANKING 1")
    logger.info("="*60)

    try:
        base_files = file_utils.load_export_mail_base_files()
        if not base_files:
            logger.warning("No se encontraron archivos en la carpeta de entrada. Proceso terminado.")
            return

        with db.get_db_connection() as cursor:
            df_rank1_all = db.get_rank_1_mails(cursor)

        if df_rank1_all.is_empty():
            logger.error("No se encontraron correos con ranking 1 en la base de datos. No se puede continuar.")
            return

        df_rank1_all = df_rank1_all.with_columns([
            pl.col("email").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
            pl.col("idccliente").cast(pl.Utf8).str.strip_chars()
        ])

        df_rank1_personal = _filter_personal_emails(df_rank1_all)
        df_rank1_personal = df_rank1_personal.rename({"email": "email_rank1"})

        for original_path, df_base in base_files:
            logger.info("-" * 60)
            logger.info(f"\U0001F4C4 Procesando archivo: {original_path.name}")

            df_base = _prepare_base_df(df_base)

            df_base = df_base.with_columns(pl.col("dni").cast(pl.Utf8).str.strip_chars())

            df_result = df_base.join(
                df_rank1_personal,
                left_on="dni",
                right_on="idccliente",
                how="left"
            )

            df_result = df_result.with_columns(
                pl.col("email_rank1").fill_null(pl.col("CORREO")).alias("CORREO")
            )

            df_final = df_result.drop("email_rank1")

            df_final_filtered = df_final.filter(pl.col("CORREO").is_not_null())

            found_count = df_final_filtered.height
            logger.info(f"Se encontraron correos válidos para {found_count} registros. Se eliminaron los demás.")

            file_utils.save_exported_mail_file(df_final_filtered, original_path)

    except Exception as e:
        logger.critical(f"El proceso de extracción de mails falló: {e}", exc_info=True)
        raise