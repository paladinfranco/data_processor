"""
processor/reader.py
Módulo responsable de la lectura y validación de archivos CSV y XLSX.
Soporta cualquier ruta del sistema de archivos.
"""

import os
import pandas as pd
from utils.logger import logger


# ── Constantes ────────────────────────────────────────────────────────────────
EXTENSIONES_SOPORTADAS = {".csv", ".xlsx"}

COLUMNAS_REQUERIDAS = {
    "cedula",
    "login",
    "telefono1",
    "telefono2",
    "direccion",
    "ciudad",
    "estado",
    "saldo",
}


# ── Funciones públicas ────────────────────────────────────────────────────────

def leer_archivo(ruta: str) -> pd.DataFrame:
    """
    Lee un archivo CSV o XLSX desde cualquier ruta del sistema y retorna
    un DataFrame validado.

    Args:
        ruta: Ruta absoluta o relativa al archivo.

    Returns:
        pd.DataFrame con los datos cargados.

    Raises:
        FileNotFoundError: Si el archivo no existe en la ruta indicada.
        ValueError: Si la extensión no es soportada o faltan columnas.
        Exception: Si el archivo está corrupto o tiene formato inválido.
    """
    logger.info(f"📂 Leyendo archivo: {ruta}")

    extension = _validar_extension(ruta)
    _validar_existencia(ruta)

    df = _cargar_dataframe(ruta, extension)

    _validar_columnas(df, ruta)
    _limpiar_datos(df)

    logger.info(
        f"✅ Archivo cargado exitosamente: "
        f"{len(df):,} registros | {len(df.columns)} columnas"
    )

    return df


# ── Funciones privadas ────────────────────────────────────────────────────────

def _validar_existencia(ruta: str) -> None:
    """Verifica que el archivo exista en el sistema de archivos."""
    if not os.path.exists(ruta):
        raise FileNotFoundError(
            f"❌ Archivo no encontrado: '{ruta}'\n"
            f"   Verifica que la ruta sea correcta."
        )


def _validar_extension(ruta: str) -> str:
    """
    Verifica que la extensión del archivo sea soportada.

    Returns:
        Extensión del archivo en minúsculas (ej: '.csv').
    """
    _, extension = os.path.splitext(ruta)
    extension = extension.lower()

    if extension not in EXTENSIONES_SOPORTADAS:
        raise ValueError(
            f"❌ Formato no soportado: '{extension}'\n"
            f"   Formatos aceptados: {', '.join(EXTENSIONES_SOPORTADAS)}"
        )

    return extension


def _cargar_dataframe(ruta: str, extension: str) -> pd.DataFrame:
    """
    Carga el archivo en un DataFrame según su extensión.

    Args:
        ruta:      Ruta al archivo.
        extension: Extensión validada del archivo.

    Returns:
        pd.DataFrame con los datos crudos.
    """
    try:
        if extension == ".csv":
            logger.debug("Cargando como CSV ...")
            df = pd.read_csv(ruta, dtype={"cedula": str, "telefono1": str, "telefono2": str})

        elif extension == ".xlsx":
            logger.debug("Cargando como XLSX ...")
            df = pd.read_excel(
                ruta,
                engine="openpyxl",
                dtype={"cedula": str, "telefono1": str, "telefono2": str},
            )

        return df

    except Exception as e:
        raise Exception(
            f"❌ Error al leer el archivo '{ruta}': {e}\n"
            f"   Verifica que el archivo no esté corrupto o abierto en otro programa."
        )


def _validar_columnas(df: pd.DataFrame, ruta: str) -> None:
    """Verifica que el DataFrame contenga todas las columnas requeridas."""
    columnas_presentes  = set(df.columns.str.lower())
    columnas_faltantes  = COLUMNAS_REQUERIDAS - columnas_presentes

    if columnas_faltantes:
        raise ValueError(
            f"❌ El archivo '{ruta}' no tiene el formato esperado.\n"
            f"   Columnas faltantes: {', '.join(sorted(columnas_faltantes))}\n"
            f"   Columnas esperadas: {', '.join(sorted(COLUMNAS_REQUERIDAS))}"
        )

    # Normaliza nombres de columnas a minúsculas
    df.columns = df.columns.str.lower()


def _limpiar_datos(df: pd.DataFrame) -> None:
    """
    Limpieza básica del DataFrame:
    - Elimina filas completamente vacías.
    - Convierte la columna saldo a numérico.
    - Elimina espacios en columnas de texto.
    """
    filas_antes = len(df)

    df.dropna(how="all", inplace=True)
    df["saldo"] = pd.to_numeric(df["saldo"], errors="coerce").fillna(0.0)

    # Elimina espacios en columnas de texto
    for col in ["cedula", "login", "telefono1", "telefono2", "direccion", "ciudad", "estado"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    filas_despues = len(df)
    eliminadas    = filas_antes - filas_despues

    if eliminadas > 0:
        logger.warning(f"⚠️  Se eliminaron {eliminadas} filas vacías durante la limpieza.")
