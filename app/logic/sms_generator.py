# app/logic/sms_generator.py
import polars as pl
import logging

from app import settings
from app.utils import file_utils, text_utils
from app import database as db

logger = logging.getLogger(__name__)


def apply_template(template: str, replacements: dict[str, pl.Expr]) -> pl.Expr:
    named_exprs = [expr.alias(k.strip("{}")) for k, expr in replacements.items()]
    return pl.struct(named_exprs).map_elements(
        lambda row: template.format(**{key: row.get(key, "") for key in row}),
        return_dtype=pl.Utf8
    )

def _validate_and_format_phones(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Validando y formateando números de teléfono...")
    
    df = df.with_columns(
        pl.col("telefono").cast(pl.Utf8).str.replace_all(r"\D", "").alias("telefono_limpio")
    )
    
    initial_rows = len(df)
    
    df_valid = df.filter(
        (pl.col("telefono_limpio").str.starts_with("9")) &
        (pl.col("telefono_limpio").str.len_chars() == settings.VALID_PHONE_LENGTH)
    )
    
    removed_count = initial_rows - len(df_valid)
    logger.info(f"Se eliminaron {removed_count} filas por números de teléfono no válidos.")
    
    return df_valid.with_columns(
        pl.concat_str([pl.lit(settings.COUNTRY_CODE), pl.col("telefono_limpio")]).alias("telefono")
    ).drop("telefono_limpio")

def _enrich_with_db_discounts(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Enriqueciendo datos con descuentos de la base de datos...")
    dnis = df.get_column("documento").to_list()

    with db.get_db_connection() as cursor:
        df_discounts = db.get_campaign_discounts(cursor, dnis)

    if df_discounts.is_empty():
        logger.warning("No se encontraron descuentos en la BD para los documentos proporcionados.")
        return df.with_columns(pl.lit(False).alias("keep_row"))

    # Deduplicar por cliente
    df_discounts = df_discounts.unique(subset=["idccliente"])

    return df.join(df_discounts, left_on="documento", right_on="idccliente", how="left")

def _replace_or_filter_by_discount(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Reemplazando descuentos con los de la base de datos si no coinciden...")

    discount_col_name = next((col for col in ["DREG", "ECLAU", "EJUL", "EMAX"] if col in df.columns), None)
    if not discount_col_name:
        logger.warning("No se encontró ninguna columna de descuento (DREG, ECLAU, etc.). No se puede procesar.")
        return df

    df = df.with_columns(
        (pl.col(discount_col_name).str.replace("%", "").cast(pl.Float64) / 100).alias("excel_discount_numeric")
    )

    db_discount_to_compare = (
        pl.when(pl.lit(discount_col_name) == "DREG").then(pl.col("dcto_reg"))
        .when(pl.lit(discount_col_name) == "ECLAU").then(pl.col("dcto_sub"))
        .when(pl.lit(discount_col_name) == "EJUL").then(pl.col("dcto_ger"))
        .when(pl.lit(discount_col_name) == "EMAX").then(pl.max_horizontal("dcto_sub", "dcto_ger"))
        .otherwise(None)
    )
    df = df.with_columns(db_discount_to_compare.alias("db_discount_to_compare"))

    epsilon = 0.001
    mismatch_count = df.filter(
        (pl.col("excel_discount_numeric") - pl.col("db_discount_to_compare")).abs() >= epsilon
    ).shape[0]
    logger.info(f"Hay {mismatch_count} filas que no coincidían con el descuento de la base de datos.")

    # Reemplazar el descuento si no coincide
    df = df.with_columns(
        pl.when((pl.col("excel_discount_numeric") - pl.col("db_discount_to_compare")).abs() >= epsilon)
        .then((pl.col("db_discount_to_compare") * 100).cast(pl.Int64).cast(pl.Utf8) + pl.lit("%"))
        .otherwise(pl.col(discount_col_name))
        .alias(discount_col_name)
    )

    # Eliminar filas sin descuento en la BD
    df_filtered = df.filter(pl.col("db_discount_to_compare").is_not_null())
    removed_count = len(df) - len(df_filtered)
    logger.info(f"Se eliminaron {removed_count} filas sin descuento en la base de datos.")

    return df_filtered

def _build_tenor_and_check_length(df: pl.DataFrame, tipo_template: str = "TIPO_1") -> pl.DataFrame:
    logger.info("Construyendo mensajes (TENOR) y aplicando restricciones de longitud...")

    template = settings.TENOR_TEMPLATES.get(tipo_template)
    if not template:
        logger.critical(f"No se encontró la plantilla '{tipo_template}' en TENOR_TEMPLATES.")
        raise ValueError(f"Plantilla '{tipo_template}' no definida.")

    # Detectar campos requeridos
    requiere_descuento = "{DESCUENTO}" in template
    requiere_cuota = "{CUOTA}" in template

    # Detectar columna de descuento si es necesario
    discount_col = None
    if requiere_descuento:
        discount_col = next((col for col in ["DREG", "ECLAU", "EJUL", "EMAX"] if col in df.columns), None)
        if not discount_col:
            logger.critical("La plantilla requiere descuento pero no se encontró ninguna columna válida.")
            raise ValueError("No se puede construir el mensaje TENOR sin columna de descuento.")

    # Limpiar número del asesor
    df = df.with_columns(
        pl.col("TELEFONO ASESOR").cast(pl.Utf8).str.replace_all(r"\D", "").alias("TELEFONO_ASESOR_LIMPIO")
    )

    wa_link_expr = pl.concat_str([
        pl.lit("wa.me/+51"),
        pl.col("TELEFONO_ASESOR_LIMPIO")
    ])

    # Reemplazos dinámicos
    replacements_with_link = {
        "{CLIENTE}": pl.col("CLIENTE"),
        "{TELEFONO_ASESOR}": wa_link_expr
    }
    replacements_without_link = {
        "{CLIENTE}": pl.col("CLIENTE"),
        "{TELEFONO_ASESOR}": pl.col("TELEFONO_ASESOR_LIMPIO")
    }

    if requiere_descuento:
        replacements_with_link["{DESCUENTO}"] = pl.col(discount_col)
        replacements_without_link["{DESCUENTO}"] = pl.col(discount_col)

    if requiere_cuota:
        cuota_expr = pl.col("CUOTA").cast(pl.Utf8)
        replacements_with_link["{CUOTA}"] = cuota_expr
        replacements_without_link["{CUOTA}"] = cuota_expr

    tenor_with_link = apply_template(template, replacements_with_link)
    tenor_without_link = apply_template(template, replacements_without_link)

    final_tenor = pl.when(tenor_with_link.str.len_chars() <= settings.SMS_MAX_LENGTH) \
                    .then(tenor_with_link) \
                    .otherwise(tenor_without_link)

    df = df.with_columns(final_tenor.alias("TENOR_FINAL"))
    df_filtered = df.filter(pl.col("TENOR_FINAL").str.len_chars() <= settings.SMS_MAX_LENGTH)

    removed_count = len(df) - len(df_filtered)
    logger.info(f"Se eliminaron {removed_count} filas por superar la longitud máxima de {settings.SMS_MAX_LENGTH} caracteres.")

    return df_filtered.with_columns(pl.col("TENOR_FINAL").str.len_chars().alias("LARGO"))

def _add_final_rows(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
        
    logger.info(f"Añadiendo {len(settings.EXTRA_PHONES_TO_ADD)} números de teléfono extra al final de la base.")
    last_row = df.tail(1)
    
    extra_rows = []
    for phone in settings.EXTRA_PHONES_TO_ADD:
        new_row = last_row.with_columns(
            pl.lit(settings.COUNTRY_CODE + phone).alias("telefono")
        )
        extra_rows.append(new_row)
        
    return pl.concat([df] + extra_rows, how="vertical")

def run_sms_generation(tipo_template: str = "TIPO_1"):
    logger.info("=" * 60)
    logger.info("INICIANDO PROCESO DE GENERACIÓN DE BASE SMS")

    try:
        files = file_utils.load_individual_sms_files()
        if not files:
            logger.warning("No se encontraron archivos en la carpeta de entrada. Proceso terminado.")
            return

        for nombre_archivo, df in files:
            logger.info("=" * 60)
            logger.info(f"📄 Procesando archivo: {nombre_archivo}.xlsx")

            try:
                # 1. Transformaciones
                df = _validate_and_format_phones(df)
                df = df.with_columns(text_utils.to_proper_case(pl.col("CLIENTE")))
                df = _enrich_with_db_discounts(df)
                df = _replace_or_filter_by_discount(df)
                df = _build_tenor_and_check_length(df, tipo_template=tipo_template)
                df = _add_final_rows(df)

                # 2. Selección de columnas
                final_cols = ["documento", "telefono", "CLIENTE", "TENOR_FINAL", "TELEFONO ASESOR", "LARGO"]
                discount_col = next((col for col in ["DREG", "ECLAU", "EJUL", "EMAX"] if col in df.columns), None)
                if discount_col:
                    final_cols.insert(3, discount_col)

                df_final = df.select([col for col in final_cols if col in df.columns])

                if "TELEFONO ASESOR" in df_final.columns:
                    df_final = df_final.rename({"TELEFONO ASESOR": "TELFASESOR"})

                df_final = df_final.unique(subset=["telefono"])

                if "TELFASESOR" in df_final.columns:
                    df_final = df_final.with_columns(
                        pl.col("TELFASESOR").cast(pl.Utf8)
                    )

                # 3. Guardar archivo con nombre personalizado
                output_file = file_utils.save_sms_output(df_final, nombre_base=nombre_archivo)
                logger.info(f"✅ Archivo generado: {output_file}")

            except Exception as e:
                logger.error(f"❌ Error al procesar {nombre_archivo}: {e}", exc_info=True)

    except Exception as e:
        logger.critical(f"El proceso general falló: {e}", exc_info=True)
        raise