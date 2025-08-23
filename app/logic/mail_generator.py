# app/logic/mail_generator.py
import polars as pl
import logging

from app import database as db
from app.utils import file_utils
from app import settings

logger = logging.getLogger(__name__)

def _add_supervisor_column(df: pl.DataFrame) -> pl.DataFrame:
    if "modelo" not in df.columns:
        return df
    expr = pl.lit(None, dtype=pl.Utf8)
    for key, value in settings.SUPERVISOR_MAP.items():
        expr = pl.when(pl.col("modelo").str.contains(key)).then(pl.lit(value)).otherwise(expr)

    return df.with_columns(expr.alias("sup"))

def _enrich_with_emails(df: pl.DataFrame, df_mails: pl.DataFrame) -> pl.DataFrame:
    logger.info("=" * 60)
    logger.info("Uniendo base de entrada con la tabla de correos...")
    df = df.with_columns(pl.col("idccliente").cast(pl.Utf8))
    df_mails = df_mails.with_columns(pl.col("idccliente").cast(pl.Utf8))
    
    df_merged = df.join(df_mails, on="idccliente", how="inner")
    logger.info(f"Se encontraron correos para {len(df_merged)} de {len(df)} clientes.")
    return df_merged

def _process_deuda_total(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Procesando como base de 'Deuda Total' (sin filtros adicionales).")

    column_order = [
        "idccliente", "cliente", "email", "ranking", "dtotalac",
        "modelo", "sup"
    ]
    column_order = [col for col in column_order if col in df.columns]
    df = df.select(column_order)

    df = df.filter(pl.col("email").is_not_null())

    # Eliminar duplicados
    df = df.unique()

    logger.info(f"Registros finales en base 'Deuda Total': {df.height}")
    return df

def _process_descuento_regular(df: pl.DataFrame, df_campaigns: pl.DataFrame) -> pl.DataFrame:
    logger.info("Procesando y filtrando base de 'Descuento Regular'...")

    # Validar columnas mínimas
    required_input = {"idccliente", "dreg", "cuota"}
    required_campaign = {"dcto_reg", "plazo_reg"}
    missing_input = required_input - set(df.columns)
    missing_campaign = required_campaign - set(df_campaigns.columns)
    if missing_input:
        raise ValueError(f"Faltan columnas en base de entrada: {missing_input}")
    if missing_campaign:
        raise ValueError(f"Faltan columnas en campañas: {missing_campaign}")

    # Normalizar idccliente
    df = df.with_columns(pl.col("idccliente").cast(pl.Utf8).str.strip_chars())
    df_campaigns = df_campaigns.with_columns(pl.col("idccliente").cast(pl.Utf8).str.strip_chars())

    # Unir con campañas
    df = df.join(df_campaigns, on="idccliente", how="left")

    # Verificar unión
    logger.info(f"Clientes después del join: {df.height}")
    df = df.filter(pl.col("dcto_reg").is_not_null())
    logger.info(f"Clientes con descuento en campañas: {df.height}")

    # Reemplazar columna dreg por dcto_reg
    df = df.drop("dreg")
    df = df.with_columns([
        pl.concat_str([
            (pl.col("dcto_reg") * 100).round(0).cast(pl.Int64).cast(pl.Utf8),
            pl.lit("%")
        ]).alias("dreg")
    ])

    # Validar cuota vs plazo
    df = df.with_columns([
        pl.col("cuota").cast(pl.Int64),
        pl.col("plazo_reg").cast(pl.Int64)
    ])
    cuota_excedida = df.filter(pl.col("cuota") > pl.col("plazo_reg"))
    logger.info(f"\u26A0\uFE0F  Registros con cuota mayor al plazo: {cuota_excedida.height}")

    if cuota_excedida.height > 0:
        ids_invalidos = set(cuota_excedida.select("idccliente").to_series().to_list())
        logger.info(f"Clientes eliminados por cuota > plazo_reg: {len(ids_invalidos)} únicos")

    # Eliminar registros inválidos
    df = df.filter(pl.col("cuota") <= pl.col("plazo_reg"))

    # Verificar correos válidos
    correos_validos = df.filter(pl.col("email").is_not_null()).height
    logger.info(f"Clientes con correo válido: {correos_validos}")

    column_order = [
        "idccliente", "cliente", "email", "ranking", "dtotalac", "cuota",
        "dreg", "modelo", "sup", "dcto_reg", "dcto_sub", "dcto_ger", "plazo_reg"
    ]
    column_order = [col for col in column_order if col in df.columns]
    df = df.select(column_order)

    logger.info(f"Registros antes de eliminar duplicados: {df.height}")
    df = df.unique()
    logger.info(f"Registros después de eliminar duplicados: {df.height}")

    return df

def _process_comparativo(df: pl.DataFrame, df_campaigns: pl.DataFrame) -> pl.DataFrame:
    logger.info("Procesando base de 'Descuento Regular + Comparativo'...")

    # Validar columnas mínimas
    required_input = {"idccliente", "dreg", "dcomp", "cuota"}
    required_campaign = {"dcto_reg", "dcto_sub", "dcto_ger", "plazo_reg"}
    missing_input = required_input - set(df.columns)
    missing_campaign = required_campaign - set(df_campaigns.columns)
    if missing_input:
        raise ValueError(f"Faltan columnas en base de entrada: {missing_input}")
    if missing_campaign:
        raise ValueError(f"Faltan columnas en campañas: {missing_campaign}")

    # Normalizar idccliente
    df = df.with_columns(pl.col("idccliente").cast(pl.Utf8).str.strip_chars())
    df_campaigns = df_campaigns.with_columns(pl.col("idccliente").cast(pl.Utf8).str.strip_chars())

    # Unir con campañas
    df = df.join(df_campaigns, on="idccliente", how="left")

    # Filtrar registros con descuento regular válido
    df = df.filter(pl.col("dcto_reg").is_not_null())
    logger.info(f"Clientes con descuento regular: {df.height}")

    # Reemplazar columna dreg por dcto_reg en formato porcentaje
    df = df.drop("dreg")
    df = df.with_columns([
        pl.concat_str([
            (pl.col("dcto_reg") * 100).round(0).cast(pl.Int64).cast(pl.Utf8),
            pl.lit("%")
        ]).alias("dreg")
    ])

    # Calcular descuento comparativo según tipo, o usar EMAX si no hay dcomp_header
    if "dcomp_header" in df.columns:
        df = df.with_columns([
            pl.when(pl.col("dcomp_header").is_in(["ECLAU", "DCLAU"])).then(pl.col("dcto_sub"))
            .when(pl.col("dcomp_header") == "EJUL").then(pl.col("dcto_ger"))
            .when(pl.col("dcomp_header") == "EMAX").then(pl.max_horizontal(["dcto_sub", "dcto_ger"]))
            .otherwise(None)
            .alias("dcomp_val")
        ])
    else:
        logger.warning("No se encontró 'dcomp_header'. Se calculará EMAX por defecto.")
        df = df.with_columns([
            pl.max_horizontal(["dcto_sub", "dcto_ger"]).alias("dcomp_val")
        ])

    # Reemplazar columna dcomp por dcomp_val en formato porcentaje
    df = df.drop("dcomp")
    df = df.with_columns([
        pl.concat_str([
            (pl.col("dcomp_val") * 100).round(0).cast(pl.Int64).cast(pl.Utf8),
            pl.lit("%")
        ]).alias("dcomp")
    ])

    # Validar cuota vs plazo
    df = df.with_columns([
        pl.col("cuota").cast(pl.Int64),
        pl.col("plazo_reg").cast(pl.Int64)
    ])
    cuota_excedida = df.filter(pl.col("cuota") > pl.col("plazo_reg"))
    logger.info(f"\u26A0\uFE0F  Registros con cuota mayor al plazo: {cuota_excedida.height}")

    if cuota_excedida.height > 0:
        ids_invalidos = set(cuota_excedida.select("idccliente").to_series().to_list())
        logger.info(f"Clientes eliminados por cuota > plazo_reg: {len(ids_invalidos)} únicos")

    # Eliminar registros inválidos
    df = df.filter(pl.col("cuota") <= pl.col("plazo_reg"))

    # Verificar correos válidos
    correos_validos = df.filter(pl.col("email").is_not_null()).height
    logger.info(f"Clientes con correo válido: {correos_validos}")

    # Reordenar columnas
    column_order = [
        "idccliente", "cliente", "email", "ranking", "dtotalac", "cuota",
        "dreg", "dcomp", "modelo", "sup",
        "dcto_reg", "dcto_sub", "dcto_ger", "plazo_reg"
    ]
    column_order = [col for col in column_order if col in df.columns]
    df = df.select(column_order)

    # Eliminar duplicados
    logger.info(f"Registros antes de eliminar duplicados: {df.height}")
    df = df.unique()
    logger.info(f"Registros después de eliminar duplicados: {df.height}")

    return df

def run_mail_generation(mail_type: str, source_table: str = "mails"):
    logger.info("=" * 60)
    logger.info(f"INICIANDO GENERACIÓN DE BASE MAILS - TIPO: {mail_type.upper()} - TABLA: {source_table}")

    try:
        mapping_key = f"mails_{mail_type}_map"
        file_data_list = file_utils.load_mail_files(mapping_key)
        if not file_data_list:
            logger.warning("No se encontraron archivos o datos válidos. Proceso terminado.")
            return

        with db.get_db_connection() as cursor:
            if source_table == "mails":
                df_mails_db = db.get_all_mails(cursor)
            elif source_table == "mailssearch":
                df_mails_db = db.get_all_mailssearch(cursor)
            else:
                raise ValueError(f"Tabla de origen desconocida: {source_table}")

            for file_path, df_input in file_data_list:
                dnis_to_check = df_input.get_column("idccliente").to_list()
                df_campaigns_db = db.get_campaign_details_for_mails(cursor, dnis_to_check)

                df_processed = _enrich_with_emails(df_input, df_mails_db)

                if mail_type == "deuda_total":
                    ids_enriched = set(df_processed.select("idccliente").to_series().to_list())
                    ids_validos_campaign = set(df_campaigns_db.select("idccliente").to_series().to_list())
                    df_filtered = df_processed.filter(pl.col("idccliente").is_in(ids_validos_campaign))
                    ids_final = set(df_filtered.select("idccliente").to_series().to_list())
                    ids_retirados = ids_enriched - ids_final
                    validos = len(ids_final)
                    retirados = len(ids_retirados)

                    logger.info(f"Clientes válidos en campañas (deuda_total): {validos} | Clientes retirados: {retirados}")

                    if ids_retirados:
                        logger.info(f"ID de clientes retirados: {', '.join(sorted(map(str, ids_retirados)))}")
                    else:
                        logger.info("No se retiraron clientes adicionales.")

                    df_final = _process_deuda_total(df_filtered)

                elif mail_type == "descuento_regular":
                    df_final = _process_descuento_regular(df_processed, df_campaigns_db)
                elif mail_type == "comparativo":
                    df_final = _process_comparativo(df_processed, df_campaigns_db)
                else:
                    raise ValueError("Tipo de mail no válido.")

                df_final = _add_supervisor_column(df_final)

                file_utils.save_split_mail_output(df_final, original_file_path=file_path)

    except Exception as e:
        logger.critical(f"El proceso de generación de mails falló: {e}", exc_info=True)
        raise