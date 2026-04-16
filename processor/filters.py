"""
processor/filters.py
Módulo responsable de aplicar filtros dinámicos sobre un DataFrame.
Soporta filtrado por: cedula, telefono, saldo.
Soporta operadores: =, >, <, >=, <=
"""

import pandas as pd
from utils.logger import logger


# ── Constantes ────────────────────────────────────────────────────────────────
FILTROS_VALIDOS = {"cedula", "telefono", "saldo"}

OPERADORES_VALIDOS = {"=", ">", "<", ">=", "<="}


# ── Función pública ───────────────────────────────────────────────────────────

def aplicar_filtro(
    df: pd.DataFrame,
    campo: str,
    operador: str,
    valor: str,
) -> pd.DataFrame:
    """
    Aplica un filtro dinámico sobre el DataFrame recibido.

    Args:
        df:       DataFrame o chunk a filtrar.
        campo:    Campo por el cual filtrar (cedula, telefono, saldo).
        operador: Operador de comparación (=, >, <, >=, <=).
        valor:    Valor de referencia para el filtro.

    Returns:
        pd.DataFrame con los registros que cumplen el filtro.

    Raises:
        ValueError: Si el campo u operador no son válidos.
    """
    _validar_campo(campo)
    _validar_operador(operador)

    if campo == "cedula":
        return _filtrar_cedula(df, operador, valor)

    if campo == "telefono":
        return _filtrar_telefono(df, operador, valor)

    if campo == "saldo":
        return _filtrar_saldo(df, operador, valor)


# ── Validaciones ──────────────────────────────────────────────────────────────

def _validar_campo(campo: str) -> None:
    """Verifica que el campo de filtro sea válido."""
    if campo not in FILTROS_VALIDOS:
        raise ValueError(
            f"❌ Campo de filtro no válido: '{campo}'\n"
            f"   Campos permitidos: {', '.join(sorted(FILTROS_VALIDOS))}"
        )


def _validar_operador(operador: str) -> None:
    """Verifica que el operador sea válido."""
    if operador not in OPERADORES_VALIDOS:
        raise ValueError(
            f"❌ Operador no válido: '{operador}'\n"
            f"   Operadores permitidos: {', '.join(sorted(OPERADORES_VALIDOS))}"
        )


# ── Filtros por campo ─────────────────────────────────────────────────────────

def _filtrar_cedula(df: pd.DataFrame, operador: str, valor: str) -> pd.DataFrame:
    """
    Filtra por cédula.
    Solo soporta operador '=' ya que la cédula es un identificador de texto.
    """
    if operador != "=":
        raise ValueError(
            f"❌ El campo 'cedula' solo soporta el operador '='\n"
            f"   Operador recibido: '{operador}'"
        )

    logger.debug(f"Aplicando filtro: cedula = '{valor}'")
    return df[df["cedula"] == valor]


def _filtrar_telefono(df: pd.DataFrame, operador: str, valor: str) -> pd.DataFrame:
    """
    Filtra por teléfono buscando en telefono1 y telefono2.
    Solo soporta operador '=' ya que el teléfono es texto.
    """
    if operador != "=":
        raise ValueError(
            f"❌ El campo 'telefono' solo soporta el operador '='\n"
            f"   Operador recibido: '{operador}'"
        )

    logger.debug(f"Aplicando filtro: telefono1 o telefono2 = '{valor}'")
    return df[
        (df["telefono1"] == valor) |
        (df["telefono2"] == valor)
    ]


def _filtrar_saldo(df: pd.DataFrame, operador: str, valor: str) -> pd.DataFrame:
    """
    Filtra por saldo numérico con soporte completo de operadores.
    """
    try:
        valor_numerico = float(valor)
    except ValueError:
        raise ValueError(
            f"❌ El valor para filtrar 'saldo' debe ser numérico.\n"
            f"   Valor recibido: '{valor}'"
        )

    logger.debug(f"Aplicando filtro: saldo {operador} {valor_numerico}")

    operaciones = {
        "=":  df["saldo"] == valor_numerico,
        ">":  df["saldo"] >  valor_numerico,
        "<":  df["saldo"] <  valor_numerico,
        ">=": df["saldo"] >= valor_numerico,
        "<=": df["saldo"] <= valor_numerico,
    }

    return df[operaciones[operador]]
