"""
utils/exporter.py
Módulo responsable de exportar los resultados del procesamiento a CSV.
"""

import os
from datetime import datetime

import pandas as pd

from utils.logger import logger


def exportar_csv(df: pd.DataFrame, campo: str, operador: str, valor: str) -> str:
    """
    Exporta el DataFrame de resultados a un archivo CSV en la carpeta output/.

    El nombre del archivo incluye el filtro aplicado y un timestamp
    para evitar sobreescrituras.

    Args:
        df:       DataFrame con los resultados a exportar.
        campo:    Campo de filtro aplicado (para el nombre del archivo).
        operador: Operador utilizado (para el nombre del archivo).
        valor:    Valor del filtro (para el nombre del archivo).

    Returns:
        Ruta del archivo generado.
    """
    if df.empty:
        logger.warning("⚠️  No hay resultados para exportar.")
        return ""

    os.makedirs("output", exist_ok=True)

    timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    operador_seg = operador.replace(">", "gt").replace("<", "lt").replace("=", "eq")
    nombre       = f"resultado_{campo}_{operador_seg}_{valor}_{timestamp}.csv"
    ruta         = os.path.join("output", nombre)

    df.to_csv(ruta, index=False, encoding="utf-8")

    logger.info(f"💾 Resultados exportados: {ruta} ({len(df):,} registros)")

    return ruta
