"""
utils/logger.py
Configuración centralizada del sistema de logging.
Provee un logger global con salida a consola y archivo.
"""

import logging
import os
from datetime import datetime


# ── Colores ANSI para la consola ──────────────────────────────────────────────
COLORES = {
    "DEBUG":    "\033[36m",   # Cyan
    "INFO":     "\033[32m",   # Verde
    "WARNING":  "\033[33m",   # Amarillo
    "ERROR":    "\033[31m",   # Rojo
    "CRITICAL": "\033[41m",   # Fondo rojo
    "RESET":    "\033[0m",
}


class FormateadorColor(logging.Formatter):
    """Formateador personalizado que agrega colores ANSI a la salida de consola."""

    FORMATO = "%(asctime)s [%(levelname)-8s] %(message)s"
    FECHA   = "%H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        color  = COLORES.get(record.levelname, COLORES["RESET"])
        reset  = COLORES["RESET"]
        formatter = logging.Formatter(
            f"{color}{self.FORMATO}{reset}",
            datefmt=self.FECHA,
        )
        return formatter.format(record)


def construir_logger(nombre: str = "data_processor") -> logging.Logger:
    """
    Construye y retorna el logger global de la aplicación.

    Args:
        nombre: Nombre del logger (aparece en registros de archivo).

    Returns:
        logging.Logger configurado con handlers de consola y archivo.
    """
    logger = logging.getLogger(nombre)

    # Evita agregar handlers duplicados si se llama más de una vez
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # ── Handler de consola (con colores) ─────────────────────────────────────
    handler_consola = logging.StreamHandler()
    handler_consola.setLevel(logging.INFO)
    handler_consola.setFormatter(FormateadorColor())

    # ── Handler de archivo (sin colores, con fecha completa) ─────────────────
    os.makedirs("logs", exist_ok=True)
    nombre_archivo = f"logs/ejecucion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    handler_archivo = logging.FileHandler(nombre_archivo, encoding="utf-8")
    handler_archivo.setLevel(logging.DEBUG)
    handler_archivo.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    logger.addHandler(handler_consola)
    logger.addHandler(handler_archivo)

    return logger


# Logger global listo para importar desde cualquier módulo
logger = construir_logger()
