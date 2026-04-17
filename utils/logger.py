"""
utils/logger.py
Configuración centralizada del sistema de logging.
Provee un logger global con salida a consola coordinada con tqdm
y salida a archivo con timestamp.
"""

import logging
import os
from datetime import datetime

from tqdm import tqdm


# ── Colores ANSI para la consola ──────────────────────────────────────────────
COLORES = {
    "DEBUG":    "\033[36m",   # Cyan
    "INFO":     "\033[32m",   # Verde
    "WARNING":  "\033[33m",   # Amarillo
    "ERROR":    "\033[31m",   # Rojo
    "CRITICAL": "\033[41m",   # Fondo rojo
    "RESET":    "\033[0m",
}


class HandlerTqdm(logging.Handler):
    """
    Handler personalizado que escribe los mensajes del logger
    a través de tqdm.write(), evitando que los logs interrumpan
    o redibuje la barra de progreso.
    """

    FORMATO = "%(asctime)s [%(levelname)-8s] %(message)s"
    FECHA   = "%H:%M:%S"

    def emit(self, record: logging.LogRecord) -> None:
        try:
            color     = COLORES.get(record.levelname, COLORES["RESET"])
            reset     = COLORES["RESET"]
            formatter = logging.Formatter(
                f"{color}{self.FORMATO}{reset}",
                datefmt=self.FECHA,
            )
            mensaje = formatter.format(record)
            tqdm.write(mensaje)
        except Exception:
            self.handleError(record)


def construir_logger(nombre: str = "data_processor") -> logging.Logger:
    """
    Construye y retorna el logger global de la aplicación.

    Args:
        nombre: Nombre del logger.

    Returns:
        logging.Logger configurado con handler tqdm y handler de archivo.
    """
    logger = logging.getLogger(nombre)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # ── Handler de consola coordinado con tqdm ────────────────────────────────
    handler_consola = HandlerTqdm()
    handler_consola.setLevel(logging.INFO)

    # ── Handler de archivo ────────────────────────────────────────────────────
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