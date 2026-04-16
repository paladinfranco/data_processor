"""
utils/timer.py
Utilidad para medir el tiempo de ejecución de bloques de código.
Implementado como context manager para uso limpio y reutilizable.
"""

import time
from utils.logger import logger


class Timer:
    """
    Context manager para medir tiempo de ejecución.

    Uso:
        with Timer("Procesamiento principal"):
            ... código a medir ...

    Salida en consola:
        ⏱  Procesamiento principal → 2.35 segundos
    """

    def __init__(self, descripcion: str = "Operación") -> None:
        self.descripcion = descripcion
        self._inicio: float = 0.0
        self.transcurrido: float = 0.0

    def __enter__(self) -> "Timer":
        self._inicio = time.perf_counter()
        logger.info(f"⏳ Iniciando: {self.descripcion} ...")
        return self

    def __exit__(self, *args) -> None:
        self.transcurrido = time.perf_counter() - self._inicio
        logger.info(
            f"⏱  {self.descripcion} → "
            f"{self.transcurrido:.2f} segundos"
        )
