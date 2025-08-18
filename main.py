import sys
import logging
from datetime import datetime
from app.logic import sms_generator, mail_generator

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Menú principal
def show_menu():
    print("\n" + "=" * 60)
    print("      SISTEMA DE GENERACIÓN DE BASES PARA SMS Y MAILS")
    print("=" * 60)
    print(" 1. Envío SMS")
    print(" 2. Envío MAILS")
    print(" 3. Extraer MAILS ranking 1 (No implementado)")
    print(" 4. Extraer MAILS por cliente (No implementado)")
    print(" 5. Salir")
    print("=" * 60)

# Submenú de mails
def show_mail_submenu():
    print("\n" + "=" * 60)
    print("               SUB-MENÚ: ENVÍO MAILS")
    print("=" * 60)
    print(" 1. Deuda Total")
    print(" 2. Descuento Regular")
    print(" 3. Comparativo")
    print(" 4. Volver al menú principal")
    print("=" * 60)

# Selección de tenor SMS
def select_tenor_type() -> str | None:
    print("\n" + "=" * 60)
    print("           SELECCIÓN DE TIPO DE TENOR PARA SMS")
    print("=" * 60)

    tenores = {
        "1": "TIPO_1",
        "2": "TIPO_2",
        "3": "TIPO_3",
        "4": "TIPO_4",
        "5": "TIPO_5"
    }

    for key, value in tenores.items():
        print(f" {key}. {value}")
    print(" 6. Volver al menú principal")
    print("=" * 60)

    while True:
        seleccion = input("Seleccione el tipo de TENOR (1-6): ").strip()
        if seleccion == "6":
            return None
        elif seleccion in tenores:
            return tenores[seleccion]
        else:
            print("❌ Opción inválida. Intente nuevamente.")

def execute_mail_send() -> bool:
    while True:
        show_mail_submenu()
        sub_option = input("Seleccione un tipo de base de Mail (1-4): ").strip()
        mail_type = {
            "1": "deuda_total",
            "2": "descuento_regular",
            "3": "comparativo"
        }
        if sub_option in mail_type:
            mail_generator.run_mail_generation(mail_type[sub_option])
            return True
        elif sub_option == "4":
            return False
        else:
            logging.warning("Opción de sub-menú no válida.")

def main():
    while True:
        show_menu()
        option = input("Seleccione una opción (1-5): ").strip()
        start_time = datetime.now()
        process_executed = False

        try:
            if option == "1":
                tenor = select_tenor_type()
                if tenor is not None:
                    sms_generator.run_sms_generation(tipo_template=tenor)
                    process_executed = True
                else:
                    continue

            elif option == "2":
                execute = execute_mail_send()
                if execute:
                    process_executed = True
                else:
                    continue

            elif option in ["3", "4"]:
                logging.warning("⚠️ Esta opción aún no ha sido implementada.")

            elif option == "5":
                print("\n👋 ¡Hasta luego!")
                sys.exit(0)

            else:
                logging.warning("❌ Opción no válida. Por favor, intente de nuevo.")

        except KeyboardInterrupt:
            logging.warning("\n Operación cancelada por el usuario.")
            sys.exit(0)
        except Exception as e:
            logging.error(f" La operación falló: {e}")

        if process_executed:
            duration = datetime.now() - start_time
            logging.info(f"✅ Duración total de la operación: {duration}")

        input("\nPresione Enter para volver al menú...")

if __name__ == "__main__":
    main()