# app/utils/file_utils.py
import polars as pl
from pathlib import Path
from datetime import datetime

from app import settings
import logging

logger = logging.getLogger(__name__)

def load_individual_sms_files() -> list[tuple[str, pl.DataFrame]]:
    input_dir = settings.SMS_INPUT_DIR
    if not input_dir.exists():
        raise FileNotFoundError(f"El directorio de entrada no existe: {input_dir}")
        
    all_files = list(input_dir.glob('*.xlsx'))
    if not all_files:
        return []

    files = []
    for f in all_files:
        df = pl.read_excel(f, engine='openpyxl', read_options={"dtype": {
            "documento": pl.Utf8, 
            "telefono": pl.Utf8, 
            "TELEFONO ASESOR": pl.Utf8
        }})
        files.append((f.stem, df))
    return files


def save_sms_output(df: pl.DataFrame, nombre_base: str) -> Path:
    output_dir = settings.SMS_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"{nombre_base}_SMS_{timestamp}.xlsx"
    
    df.write_excel(output_file)
    return output_file

#---- MAILS ----
def load_and_map_excel(path: Path, mapping_key: str) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"El archivo {path} no existe.")
        
    df = pl.read_excel(path, engine='openpyxl')
    df = df.rename({col: col.strip() for col in df.columns})

    column_map = settings.MAPPINGS.get(mapping_key)
    if not column_map:
        raise ValueError(f"No se encontró la clave de mapeo '{mapping_key}' en mappings.yml")

    # Detectar columna comparativa y asignar dcomp_header
    if mapping_key == "mails_comparativo_map":
        comp_col = next((col for col in ["ECLAU", "EJUL", "EMAX"] if col in df.columns), None)
        if not comp_col:
            raise ValueError("No se encontró ninguna columna de comparativo válida (ECLAU, EJUL, EMAX)")
        column_map = column_map.copy()
        column_map[comp_col] = "dcomp"
        df = df.with_columns(pl.lit(comp_col).alias("dcomp_header"))
        logger.info(f"Comparativo detectado: {comp_col} → dcomp_header asignado")

    # Aplicar mapeo solo a columnas presentes
    column_map = {k: v for k, v in column_map.items() if k in df.columns}
    df = df.rename(column_map)

    # Forzar tipos esperados
    type_map = {
        "cuota": pl.Int64,
        "plazo_reg": pl.Int64,
        "idccliente": pl.Utf8,
        "email": pl.Utf8
    }
    for col, dtype in type_map.items():
        if col in df.columns:
            try:
                df = df.with_columns(pl.col(col).cast(dtype))
            except Exception as e:
                logger.warning(f"No se pudo convertir '{col}' a {dtype}: {e}")

    # Seleccionar columnas finales
    final_columns = list(column_map.values())
    if mapping_key == "mails_comparativo_map" and "dcomp_header" in df.columns:
        final_columns.append("dcomp_header")

    df = df.select(final_columns)

    return df

def load_mail_files(mapping_key: str) -> list[tuple[Path, pl.DataFrame]]:
    mails_dir = settings.MAILS_INPUT_DIR
    if not mails_dir.exists():
        raise FileNotFoundError(f"No se encontró el directorio de Mails: '{mails_dir}'")

    all_files = list(mails_dir.glob('*.xlsx'))
    if not all_files:
        return []

    file_data = []
    for f in all_files:
        df = load_and_map_excel(f, mapping_key)
        file_data.append((f, df))

    return file_data

from pathlib import Path

def save_split_mail_output(df: pl.DataFrame, original_file_path: Path):
    if df.is_empty():
        logging.warning("No hay datos para guardar.")
        return

    output_dir = settings.MAILS_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    base_name = original_file_path.stem

    model_names = df.select("modelo").unique().to_series().to_list()
    logging.info(f"Guardando {len(model_names)} archivos de salida divididos por 'modelo'...")

    for model_name in model_names:
        data = df.filter(pl.col("modelo") == model_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{base_name}_{model_name}_{timestamp}.xlsx"
        output_path = output_dir / file_name
        data.write_excel(output_path)
        logging.info(f" -> Archivo guardado: {output_path}")