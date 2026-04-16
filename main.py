"""
main.py
Punto de entrada del procesador de datos CSV/XLSX.
Soporta modo interactivo guiado y modo CLI con argumentos.

Uso CLI:
    python main.py --archivo data/clientes.xlsx --filtro saldo --operador ">" --valor 100 --hilos 4 --exportar

Uso interactivo:
    python main.py
"""

import argparse
import sys

from processor.filters import FILTROS_VALIDOS, OPERADORES_VALIDOS
from processor.orchestrator import procesar
from processor.reader import leer_archivo
from utils.exporter import exportar_csv
from utils.logger import logger


# ── Banner ────────────────────────────────────────────────────────────────────

BANNER = """
╔══════════════════════════════════════════════════════╗
║          Data Processor — CSV / XLSX v1.0            ║
║     Procesamiento concurrente con múltiples hilos    ║
╚══════════════════════════════════════════════════════╝
"""


# ── Punto de entrada ──────────────────────────────────────────────────────────

def main() -> None:
    """Función principal. Decide entre modo interactivo y modo CLI."""
    print(BANNER)
    logger.info("🟢 Iniciando Data Processor ...")

    args = _parsear_argumentos()

    if _es_modo_interactivo(args):
        config = _modo_interactivo()
    else:
        config = _modo_cli(args)

    _ejecutar(config)

    logger.info("🏁 Procesamiento finalizado.")


# ── Modos de entrada ──────────────────────────────────────────────────────────

def _es_modo_interactivo(args: argparse.Namespace) -> bool:
    """Retorna True si no se pasaron argumentos obligatorios por CLI."""
    return not any([args.archivo, args.filtro, args.operador, args.valor])


def _modo_interactivo() -> dict:
    """
    Solicita los parámetros al usuario de forma guiada e interactiva.

    Returns:
        Diccionario con la configuración completa.
    """
    logger.info("📋 Modo interactivo activado")
    print()

    # Archivo
    while True:
        archivo = input("📂 Ingresa la ruta del archivo (CSV o XLSX): ").strip()
        if archivo:
            break
        print("   ⚠️  La ruta no puede estar vacía.")

    # Campo de filtro
    campos_str = " / ".join(sorted(FILTROS_VALIDOS))
    while True:
        campo = input(f"🔍 Filtrar por ({campos_str}): ").strip().lower()
        if campo in FILTROS_VALIDOS:
            break
        print(f"   ⚠️  Opción inválida. Elige entre: {campos_str}")

    # Operador
    ops_str = "  ".join(sorted(OPERADORES_VALIDOS))

    # Cédula y teléfono solo aceptan '='
    if campo in {"cedula", "telefono"}:
        operador = "="
        print(f"⚙️  Operador: = (único permitido para '{campo}')")
    else:
        while True:
            operador = input(f"⚙️  Operador ({ops_str}): ").strip()
            if operador in OPERADORES_VALIDOS:
                break
            print(f"   ⚠️  Operador inválido. Elige entre: {ops_str}")

    # Valor
    while True:
        valor = input(f"💲 Valor a buscar: ").strip()
        if valor:
            break
        print("   ⚠️  El valor no puede estar vacío.")

    # Modo de ejecución (va ANTES que los hilos)
    print("   1. hilos    → ThreadPoolExecutor  (recomendado para la mayoría de casos)")
    print("   2. procesos → ProcessPoolExecutor (mejor para cálculos CPU-intensivos)")
    while True:
        modo = input("⚙️  Modo de ejecución (hilos / procesos) [default: hilos]: ").strip().lower()
        if modo == "":
            modo = "hilos"
            break
        if modo in {"hilos", "procesos"}:
            break
        print("   ⚠️  Opción inválida. Escribe 'hilos' o 'procesos'.")

    # Hilos o procesos (la etiqueta cambia según el modo)
    etiqueta_workers = "hilos" if modo == "hilos" else "procesos"
    while True:
        entrada_hilos = input(f"🧵 Número de {etiqueta_workers} [default: 4]: ").strip()
        if entrada_hilos == "":
            n_hilos = 4
            break
        try:
            n_hilos = int(entrada_hilos)
            if n_hilos < 1:
                raise ValueError
            break
        except ValueError:
            print("   ⚠️  Ingresa un número entero mayor a 0.")

    # Exportar
    exportar_entrada = input("💾 ¿Exportar resultados a CSV? (s/n) [default: n]: ").strip().lower()
    exportar = exportar_entrada in {"s", "si", "sí", "y", "yes"}

    print()

    return {
        "archivo":  archivo,
        "campo":    campo,
        "operador": operador,
        "valor":    valor,
        "n_hilos":  n_hilos,
        "modo":     modo,
        "exportar": exportar,
    }


def _modo_cli(args: argparse.Namespace) -> dict:
    """
    Construye la configuración desde los argumentos CLI.

    Returns:
        Diccionario con la configuración completa.
    """
    errores = []

    if not args.archivo:
        errores.append("--archivo es requerido")
    if not args.filtro:
        errores.append("--filtro es requerido")
    if not args.operador:
        errores.append("--operador es requerido")
    if not args.valor:
        errores.append("--valor es requerido")

    if errores:
        logger.error("❌ Argumentos faltantes:\n   " + "\n   ".join(errores))
        sys.exit(1)

    if args.filtro not in FILTROS_VALIDOS:
        logger.error(
            f"❌ Filtro inválido: '{args.filtro}'. "
            f"Permitidos: {', '.join(sorted(FILTROS_VALIDOS))}"
        )
        sys.exit(1)

    if args.operador not in OPERADORES_VALIDOS:
        logger.error(
            f"❌ Operador inválido: '{args.operador}'. "
            f"Permitidos: {', '.join(sorted(OPERADORES_VALIDOS))}"
        )
        sys.exit(1)

    logger.info("⌨️  Modo CLI activado")

    return {
        "archivo":  args.archivo,
        "campo":    args.filtro,
        "operador": args.operador,
        "valor":    args.valor,
        "n_hilos":  args.hilos,
        "modo":     args.modo,
        "exportar": args.exportar,
    }


# ── Ejecución principal ───────────────────────────────────────────────────────

def _ejecutar(config: dict) -> None:
    """
    Ejecuta el flujo completo: lectura → procesamiento → resultados → exportación.

    Args:
        config: Diccionario con todos los parámetros de ejecución.
    """
    logger.info("─" * 54)

    # 1. Lectura
    try:
        df = leer_archivo(config["archivo"])
    except (FileNotFoundError, ValueError, Exception) as e:
        logger.error(str(e))
        sys.exit(1)

    # 2. Procesamiento concurrente
    try:
        resultado, tiempo, workers_reales = procesar(
            df,
            campo    = config["campo"],
            operador = config["operador"],
            valor    = config["valor"],
            n_hilos  = config["n_hilos"],
            modo     = config["modo"],
        )
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # 3. Mostrar resumen
    tipo_worker = "Hilos utilizados  " if config["modo"] == "hilos" else "Procesos utilizados"
    logger.info("─" * 54)
    logger.info("📋 RESUMEN DE EJECUCIÓN")
    logger.info(f"   Archivo procesado : {config['archivo']}")
    logger.info(f"   Filtro aplicado   : {config['campo']} {config['operador']} {config['valor']}")
    logger.info(f"   {tipo_worker}: {workers_reales}")
    logger.info(f"   Total procesados  : {len(df):,} registros")
    logger.info(f"   Total encontrados : {len(resultado):,} registros")
    logger.info(f"   Modo de ejecución : {config['modo']}")
    logger.info(f"   Tiempo total      : {tiempo:.2f} segundos")
    logger.info("─" * 54)

    # 4. Muestra de resultados
    if not resultado.empty:
        print()
        print("📄 Muestra de resultados (primeros 5):")
        print(resultado.head(5).to_string(index=False))
        print()

    # 5. Exportación opcional
    if config["exportar"]:
        exportar_csv(resultado, config["campo"], config["operador"], config["valor"])


# ── Argumentos CLI ────────────────────────────────────────────────────────────

def _parsear_argumentos() -> argparse.Namespace:
    """Define y parsea los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Procesador concurrente de archivos CSV/XLSX",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument("--archivo",  type=str,                             help="Ruta al archivo CSV o XLSX")
    parser.add_argument("--filtro",   type=str,                             help="Campo a filtrar: cedula, telefono, saldo")
    parser.add_argument("--operador", type=str,                             help="Operador: =, >, <, >=, <=")
    parser.add_argument("--valor",    type=str,                             help="Valor de referencia para el filtro")
    parser.add_argument("--modo",     type=str, default="hilos",
                        choices=["hilos", "procesos"],                      help="Modo de ejecución: hilos o procesos (default: hilos)")
    parser.add_argument("--hilos",    type=int, default=4,                  help="Número de hilos/procesos (default: 4)")
    parser.add_argument("--exportar", action="store_true",                  help="Exportar resultados a CSV")

    return parser.parse_args()


if __name__ == "__main__":
    main()