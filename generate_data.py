"""
generate_data.py
Generador de archivos CSV con datos de prueba realistas.
Permite generar datasets de cualquier tamaño para probar
el procesamiento concurrente con volúmenes reales.

Uso:
    python generate_data.py                      # genera 100,000 registros
    python generate_data.py --registros 50000    # genera 50,000 registros
    python generate_data.py --registros 1000000  # genera 1,000,000 registros
"""

import argparse
import csv
import os
import random
import time
from tqdm import tqdm


# ── Datos base para generación realista ──────────────────────────────────────

CIUDADES_CALLES = {
    "Quito": [
        "Avenida Amazonas",
        "Avenida de los Shyris",
        "Avenida Eloy Alfaro",
        "Avenida Naciones Unidas",
        "Avenida De La República",
        "Avenida América",
        "Avenida 10 de Agosto",
        "Calle García Moreno (Calle de las 7 Cruces)",
        "Calle La Ronda",
        "Calle Guayaquil",
        "Calle Junín",
        "Boulevard 24 de Mayo",
        "Avenida Isabel La Católica",
        "Avenida Universitaria",
        "Avenida Mariscal Sucre",
    ],
    "Guayaquil": [
        "Avenida 9 de Octubre",
        "Avenida Francisco de Orellana",
        "Avenida de las Américas",
        "Avenida Carlos Julio Arosemena",
        "Avenida Simón Bolívar",
        "Calle Junín",
        "Calle Numa Pompilio Llona",
        "Malecón 2000 (Malecón Simón Bolívar)",
        "Avenida Juan Tanca Marengo",
        "Calle Pedro Carbo",
        "Avenida Quito",
        "Calle Chimborazo",
        "Avenida del Ejército",
    ],
    "Cuenca": [
        "Avenida Fray Vicente Solano",
        "Avenida Remigio Crespo",
        "Avenida de las Américas",
        "Calle Gran Colombia",
        "Calle Larga",
        "Avenida 12 de Abril",
        "Calle Benigno Malo",
        "Avenida Remigio Tamariz",
        "Calle Simón Bolívar",
        "Avenida España",
    ],
    "Ambato": [
        "Avenida Cevallos",
        "Calle Bolívar",
        "Avenida 12 de Noviembre",
        "Avenida Atahualpa",
        "Calle Sucre",
        "Avenida El Rey",
        "Calle Rocafuerte",
        "Avenida Indoamérica",
        "Calle Martínez",
    ],
    "Loja": [
        "Avenida Universitaria",
        "Calle Bernardo Valdivieso",
        "Avenida Manuel Agustín Aguirre",
        "Calle Sucre",
        "Avenida Emiliano Ortega",
        "Calle Bolívar",
        "Avenida Isidro Ayora",
        "Calle 24 de Mayo",
        "Avenida Gran Colombia",
    ],
    "Manta": [
        "Avenida Malecón (Malecón de Manta)",
        "Avenida 4 de Noviembre",
        "Avenida de las Américas",
        "Calle 12",
        "Avenida 23 de Octubre",
        "Calle 13",
        "Avenida Flavio Reyes",
        "Avenida Francisco de Orellana",
        "Calle 105",
    ],
    "Machala": [
        "Avenida 25 de Junio",
        "Calle Guayaquil",
        "Calle Pichincha",
        "Avenida Bolívar Madero Vargas",
        "Calle Rocafuerte",
        "Avenida Circunvalación Norte",
        "Calle Sucre",
        "Avenida de las Palmeras",
        "Calle Olmedo",
        "Avenida Colón Tinoco",
    ],
    "Esmeraldas": [
        "Avenida Eloy Alfaro",
        "Calle 6 de Diciembre",
        "Calle Cristóbal Colón",
        "Avenida Libertad",
        "Calle Bolívar",
        "Avenida Kennedy",
        "Malecón de Esmeraldas",
        "Calle Sucre",
    ],
    "Riobamba": [
        "Avenida Daniel León Borja",
        "Calle España",
        "Avenida La Prensa",
        "Calle Veloz",
        "Avenida Unidad Nacional",
        "Calle Primera Constituyente",
        "Avenida Eloy Alfaro",
        "Calle Guayaquil",
        "Calle Olmedo",
    ],
    "Ibarra": [
        "Avenida Mariano Acosta",
        "Calle Simón Bolívar",
        "Avenida Atahualpa",
        "Calle Flores",
        "Calle Sucre",
        "Avenida El Retorno",
        "Calle Pedro Moncayo",
        "Avenida Cristóbal de Troya",
        "Calle Olmedo",
    ],
    "Santo Domingo": [
        "Avenida Quito",
        "Avenida Abraham Calazacón",
        "Calle Ibarra",
        "Calle Portoviejo",
        "Avenida Tsáchilas",
        "Avenida Río Toachi",
        "Avenida 3 de Julio",
        "Calle Cuenca",
        "Avenida del Ejército",
    ],
    "Portoviejo": [
        "Avenida América",
        "Calle Olmedo",
        "Avenida Universitaria",
        "Calle Chile",
        "Avenida Alajuela",
        "Calle Pedro Gual",
        "Avenida 5 de Junio",
        "Calle Sucre",
        "Avenida Metropolitana",
    ],
    "Durán": [
        "Avenida León Febres Cordero",
        "Avenida El Oro",
        "Calle Principal",
        "Avenida Juan Montalvo",
        "Calle 3 de Noviembre",
        "Avenida Casuarina",
        "Calle Pichincha",
        "Avenida Abdón Calderón",
    ],
    "Milagro": [
        "Avenida 17 de Septiembre",
        "Avenida Carlos Julio Arosemena Monroy",
        "Calle 9 de Octubre",
        "Avenida Mariscal Sucre",
        "Avenida Los Chirijos",
        "Avenida Chile",
        "Avenida Las Américas",
        "Calle García Moreno",
    ],
    "Salinas": [
        "Avenida General Enríquez Gallo (Malecón de Salinas)",
        "Avenida 2 de Mayo",
        "Calle 17",
        "Avenida Eloy Alfaro",
        "Calle Rumichaca",
        "Avenida Olímpica",
        "Calle Los Almendros",
    ],
    "Playas": [
        "Malecón de Playas (Avenida Ponciano Arriaga)",
        "Avenida Guayaquil",
        "Calle Principal",
        "Calle Eloy Alfaro",
        "Avenida Los Álamos",
        "Calle 10 de Agosto",
    ],
    "Jipijapa": [
        "Avenida Santistevan",
        "Calle 10 de Agosto",
        "Calle Sucre",
        "Avenida Bolívar",
        "Calle Olmedo",
        "Calle García Moreno",
        "Avenida 24 de Mayo",
    ],
    "Atacames": [
        "Malecón de Atacames",
        "Avenida Central",
        "Calle Los Cocos",
        "Avenida Luis Tello",
        "Calle Principal",
        "Avenida Eloy Alfaro",
    ],
    "Daule": [
        "Avenida 25 de Julio",
        "Calle García Moreno",
        "Avenida Jaime Roldós Aguilera",
        "Calle Eloy Alfaro",
        "Avenida 10 de Agosto",
        "Calle Bolívar",
    ],
    "Babahoyo": [
        "Avenida Monseñor Cándido Rada",
        "Calle General Barona",
        "Avenida 10 de Agosto",
        "Malecón El Salto",
        "Calle García Moreno",
        "Avenida Clemente Baquerizo",
        "Calle 5 de Junio",
    ],
    "El Coca": [
        "Avenida Napo",
        "Calle Quito",
        "Avenida 12 de Febrero",
        "Malecón Río Napo",
        "Calle Rocafuerte",
        "Avenida Eloy Alfaro",
        "Calle 9 de Octubre",
    ],
}

CIUDADES = list(CIUDADES_CALLES.keys())

ESTADOS      = ["activo", "inactivo"]
ESTADO_PESO  = [0.75, 0.25]   # 75% activos, 25% inactivos

# Distribución de saldos por segmento:
# 30% saldo = 0 | 40% saldo bajo (1-100) | 30% saldo alto (101-9999)
SEGMENTOS_SALDO = [
    (0.30,  0.00,    0.00),
    (0.40,  1.00,  100.00),
    (0.30, 101.00, 9999.99),
]


# ── Funciones de generación ───────────────────────────────────────────────────

def _generar_cedula(indice: int) -> str:
    """Genera una cédula ecuatoriana sintética única basada en el índice."""
    return str(910000000 + indice)


def _generar_login(indice: int) -> str:
    """Genera un login único basado en el índice."""
    return f"user{indice:07d}"


def _generar_telefono() -> str:
    """Genera un número de teléfono ecuatoriano sintético."""
    prefijos_celular = ["099", "098", "097", "096", "095", "094", "093"]
    prefijos_fijo    = ["02", "03", "04", "05", "06", "07"]

    if random.random() < 0.7:
        prefijo = random.choice(prefijos_celular)
    else:
        prefijo = random.choice(prefijos_fijo)

    return prefijo + str(random.randint(1000000, 9999999))


def _generar_direccion(ciudad: str) -> str:
    """
    Genera una dirección realista usando calles reales de la ciudad indicada.

    Args:
        ciudad: Ciudad del registro para seleccionar calles correspondientes.

    Returns:
        Dirección con formato: 'Nombre de Calle N-XXX'
    """
    calles = CIUDADES_CALLES.get(ciudad, ["Calle Principal"])
    calle  = random.choice(calles)
    numero = random.randint(1, 9999)
    return f"{calle} N-{numero}"


def _generar_saldo() -> float:
    """
    Genera un saldo con distribución controlada por segmentos.
    30% sin saldo | 40% saldo bajo | 30% saldo alto
    """
    rand      = random.random()
    acumulado = 0.0

    for peso, minimo, maximo in SEGMENTOS_SALDO:
        acumulado += peso
        if rand <= acumulado:
            if minimo == maximo == 0.0:
                return 0.0
            return round(random.uniform(minimo, maximo), 2)

    return 0.0


def _generar_registro(indice: int) -> dict:
    """Genera un registro completo con todos los campos."""
    ciudad = random.choice(CIUDADES)
    return {
        "cedula":    _generar_cedula(indice),
        "login":     _generar_login(indice),
        "telefono1": _generar_telefono(),
        "telefono2": _generar_telefono(),
        "direccion": _generar_direccion(ciudad),
        "ciudad":    ciudad,
        "estado":    random.choices(ESTADOS, weights=ESTADO_PESO, k=1)[0],
        "saldo":     _generar_saldo(),
    }


# ── Función principal ─────────────────────────────────────────────────────────

def generar_csv(n_registros: int) -> str:
    """
    Genera un archivo CSV con N registros de datos sintéticos.

    Args:
        n_registros: Número de registros a generar.

    Returns:
        Ruta del archivo generado.
    """
    os.makedirs("data", exist_ok=True)

    nombre_archivo = f"data/clientes_{n_registros:,}.csv".replace(",", "_")
    columnas       = [
        "cedula", "login", "telefono1", "telefono2",
        "direccion", "ciudad", "estado", "saldo",
    ]

    print(f"\n📋 Generando {n_registros:,} registros ...")
    print(f"📂 Archivo destino: {nombre_archivo}\n")

    inicio = time.perf_counter()

    with open(nombre_archivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columnas)
        writer.writeheader()

        for i in tqdm(
            range(1, n_registros + 1),
            desc="  ⚙️  Generando registros",
            unit="reg",
            ncols=70,
            colour="cyan",
        ):
            writer.writerow(_generar_registro(i))

    tiempo     = time.perf_counter() - inicio
    tamanio_mb = os.path.getsize(nombre_archivo) / (1024 * 1024)

    print(f"\n✅ Archivo generado exitosamente:")
    print(f"   Registros  : {n_registros:,}")
    print(f"   Tamaño     : {tamanio_mb:.2f} MB")
    print(f"   Tiempo     : {tiempo:.2f} segundos")
    print(f"   Ubicación  : {nombre_archivo}\n")

    return nombre_archivo


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parsear_argumentos() -> argparse.Namespace:
    """Define y parsea los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Generador de archivos CSV con datos de prueba realistas"
    )
    parser.add_argument(
        "--registros",
        type=int,
        default=100_000,
        help="Número de registros a generar (default: 100,000)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parsear_argumentos()

    if args.registros < 1:
        print("❌ El número de registros debe ser mayor a 0.")
    else:
        generar_csv(args.registros)