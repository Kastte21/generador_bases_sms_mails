# app/settings.py
import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

# --- Project Paths ---
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
CONFIG_PATH = BASE_DIR / "config/mappings.yml"

# --- Directorios de Datos ---
SMS_INPUT_DIR = BASE_DIR / "data/input/SMS"
SMS_OUTPUT_DIR = BASE_DIR / "data/output/SMS"
MAILS_INPUT_DIR = BASE_DIR / "data/input/MAILS"
MAILS_OUTPUT_DIR = BASE_DIR / "data/output/MAILS"
EXPORT_MAILS_INPUT_DIR = BASE_DIR / "data/input/EXPORT_MAILS"
EXPORT_MAILS_OUTPUT_DIR = BASE_DIR / "data/output/EXPORT_MAILS"

# --- Configuración de Base de Datos ---
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# --- Mappings Configuration ---
try:
    with open(CONFIG_PATH, 'r', encoding="utf-8") as f:
        MAPPINGS = yaml.safe_load(f)
except FileNotFoundError:
    raise FileNotFoundError(f"El archivo de mapeos no se encontró en: {CONFIG_PATH}")

# --- Dominios Personales ---
PERSONAL_EMAIL_DOMAINS = [
    'gmail.com', 'hotmail.com', 'outlook.com', 
    'live.com', 'icloud.com', 'yahoo.com'
]

# --- Constantes del Proceso SMS ---
COUNTRY_CODE = "51"
VALID_PHONE_LENGTH = 9
SMS_MAX_LENGTH = 160
EXTRA_PHONES_TO_ADD = ["963119313", "954665484"]

# --- Plantillas de Mensajes (TENOR) ---
TENOR_TEMPLATES = {
    "TIPO_1": "SOLO POR HOY! {CLIENTE} solicita tu descuento del {DESCUENTO} contáctanos al {TELEFONO_ASESOR} y activa tu beneficio. Valido en {CUOTA} cuotas. BCP",
    "TIPO_2": "BCP, {CLIENTE} solicita tu desct exclusivo del {DESCUENTO} sujeto a evaluación. Contáctanos al {TELEFONO_ASESOR} y activa tu beneficio",
    "TIPO_3": "HOLA {CLIENTE} en el BCP te ofrecemos acceder a un DCSTO aprobado para cancelar tu deuda y así mejores tu calificación financiera. Contáctanos al {TELEFONO_ASESOR} BCP",
    "TIPO_4": "HOLA {CLIENTE} te ayudamos a solucionar tu deuda y evitar el riesgo de entrar en Cobranza Judicial. Contáctanos hoy al {TELEFONO_ASESOR} BCP",
    "TIPO_5": "HOLA {CLIENTE}. Lamentamos informarte que has sido reportado a las centrales de riesgos. Realiza el pago de tu deuda con el BCP contáctanos al {TELEFONO_ASESOR}"
}

# --- Mapeo de Modelos a Supervisores ---
SUPERVISOR_MAP = {
    "EYMIE": "EYMIE BEGAZO",
    "MAJO": "MARIA JOSE MANSILLA",
    "BARBARA": "BARBARA MEJIAS",
    "JOSUE": "JOSUE RIOS",
    "OSWALDO": "OSWALDO APAZA",
    "EDWARD": "EDWARD VILLANUEVA",
    "KARLA": "KARLA MONTESINOS",
    "SOFIA": "SOFIA PEÑA",
    "ARTURO": "ARTURO ARCE"
}