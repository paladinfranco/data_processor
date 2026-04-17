# Data Processor — CSV / XLSX

Procesador concurrente de archivos CSV y XLSX con filtros dinámicos, múltiples workers configurables, benchmark de rendimiento y medición de tiempo de ejecución.

Desarrollado en Python con `concurrent.futures`, soporta dos modos de ejecución: **hilos** (`ThreadPoolExecutor`) y **procesos** (`ProcessPoolExecutor`), con límites automáticos de seguridad según los recursos del sistema.

---

## Requisitos

- Python 3.8 o superior
- Dependencias listadas en `requirements.txt`

---

## Instalación

```bash
# 1. Clona o descarga el proyecto
cd ~/data_processor

# 2. Crea y activa el entorno virtual
python3 -m venv venv
source venv/bin/activate        # Linux / Mac
venv\Scripts\activate           # Windows

# 3. Instala las dependencias
pip install -r requirements.txt
```

---

## Uso

El script soporta dos modos de ejecución:

### Modo interactivo (recomendado)

Ejecuta el script sin argumentos y sigue el asistente guiado paso a paso:

```bash
python main.py
```

El sistema solicitará:
- Ruta del archivo (CSV o XLSX, cualquier ubicación del sistema)
- Campo de filtro
- Operador de comparación
- Valor de búsqueda
- Modo de ejecución (hilos o procesos)
- Número de workers
- Si desea ejecutar benchmark comparativo
- Si desea exportar los resultados

### Modo CLI

Pasa todos los parámetros directamente como argumentos:

```bash
python main.py \
  --archivo data/clientes_100_000.csv \
  --filtro saldo \
  --operador ">" \
  --valor 100 \
  --modo hilos \
  --hilos 4 \
  --exportar
```

#### Argumentos disponibles

| Argumento     | Descripción                                                        | Requerido | Default  |
|---------------|--------------------------------------------------------------------|-----------|----------|
| `--archivo`   | Ruta al archivo CSV o XLSX (absoluta o relativa)                   | Sí        | —        |
| `--filtro`    | Campo a filtrar: `cedula`, `telefono`, `saldo`                     | Sí        | —        |
| `--operador`  | Operador de comparación: `=`, `>`, `<`, `>=`, `<=`                 | Sí        | —        |
| `--valor`     | Valor de referencia para el filtro                                 | Sí        | —        |
| `--modo`      | Modo de ejecución: `hilos` o `procesos`                            | No        | `hilos`  |
| `--hilos`     | Número de workers (hilos o procesos según el modo)                 | No        | `4`      |
| `--exportar`  | Flag para exportar resultados a CSV automáticamente                | No        | `False`  |
| `--benchmark` | Flag para ejecutar benchmark comparativo de rendimiento            | No        | `False`  |

---

## Filtros soportados

| Campo      | Operadores permitidos         | Tipo de dato                                    |
|------------|-------------------------------|-------------------------------------------------|
| `cedula`   | `=`                           | Texto exacto                                    |
| `telefono` | `=`                           | Texto exacto (busca en telefono1 y telefono2)   |
| `saldo`    | `=`, `>`, `<`, `>=`, `<=`     | Numérico                                        |

---

## Ejemplos de uso

```bash
# Clientes con saldo mayor a 100 usando 4 hilos
python main.py --archivo data/clientes_100_000.csv --filtro saldo --operador ">" --valor 100 --hilos 4

# Clientes con saldo igual a 0 usando 2 hilos y exportando resultados
python main.py --archivo data/clientes_100_000.csv --filtro saldo --operador "=" --valor 0 --hilos 2 --exportar

# Buscar por cédula exacta
python main.py --archivo data/clientes.xlsx --filtro cedula --operador "=" --valor 912345678

# Buscar por teléfono y exportar resultado
python main.py --archivo data/clientes.xlsx --filtro telefono --operador "=" --valor 0991234567 --exportar

# Usar archivo en ruta absoluta con modo procesos
python main.py --archivo /home/usuario/datos/clientes.csv --filtro saldo --operador ">=" --valor 500 --modo procesos --hilos 8

# Ejecutar benchmark comparativo con modo hilos
python main.py --archivo data/clientes_1_000_000.csv --filtro saldo --operador ">" --valor 100 --benchmark

# Ejecutar benchmark comparativo con modo procesos
python main.py --archivo data/clientes_1_000_000.csv --filtro saldo --operador ">" --valor 100 --benchmark --modo procesos
```

---

## Generador de datos de prueba

El proyecto incluye un script para generar archivos CSV con datos sintéticos y realistas basados en ciudades y calles reales del Ecuador.

```bash
# Generar 100,000 registros (default)
python generate_data.py

# Generar cantidad personalizada
python generate_data.py --registros 500000
python generate_data.py --registros 1000000
```

Los archivos se guardan automáticamente en la carpeta `data/` con nombre descriptivo.

### Distribución de datos generados

| Campo    | Descripción                                                      |
|----------|------------------------------------------------------------------|
| cedula   | Cédulas sintéticas únicas secuenciales                          |
| login    | Usuarios únicos (user0000001, user0000002, ...)                 |
| telefono | Números celulares (09X) y fijos (0X) ecuatorianos               |
| ciudad   | 21 ciudades reales del Ecuador                                  |
| direccion| Calles reales de cada ciudad con numeración                     |
| estado   | 75% activo, 25% inactivo                                        |
| saldo    | 30% en cero, 40% entre 1-100, 30% entre 101-9,999.99           |

---

## Modos de ejecución

### Hilos (`ThreadPoolExecutor`)
- Unidades de ejecución dentro del mismo proceso
- Comparten memoria, menor overhead de inicio
- Limitados por el GIL de Python para tareas CPU-bound
- **Recomendado** para la mayoría de casos de filtrado
- Límite máximo: `CPUs lógicas × 4`

### Procesos (`ProcessPoolExecutor`)
- Procesos independientes del sistema operativo
- Sin restricción del GIL, paralelismo real en CPU
- Mayor overhead de inicio (serialización de datos)
- **Recomendado** para datasets grandes (500K+) con transformaciones CPU-intensivas
- Límite máximo: `CPUs lógicas × 2`

El sistema ajusta automáticamente el número de workers si se supera el límite permitido, mostrando un aviso claro sin interrumpir la ejecución.

---

## Benchmark de rendimiento

El modo `--benchmark` ejecuta automáticamente el mismo filtro con distintas configuraciones de workers (1, 2, 4, 8, 12, 16 y máximo permitido), calcula métricas de rendimiento y muestra una tabla comparativa:

```
╔══════════════════════════════════════════════════════════════════╗
║                   BENCHMARK DE RENDIMIENTO                      ║
║  Archivo : clientes_1_000_000.csv                                ║
║  Registros: 1,000,000                                            ║
║  Filtro  : saldo > 100                                           ║
║  Modo    : procesos                                              ║
╠═══════════╦═══════════╦══════════════╦══════════════╦═══════════╣
║  Workers  ║   Tiempo  ║  Registros   ║   Speedup    ║ Eficiencia║
╠═══════════╬═══════════╬══════════════╬══════════════╬═══════════╣
║        1  ║   2.462s  ║     300,213  ║      1.00x   ║ 🟢 100.0% ║
║        2  ║   2.107s  ║     300,213  ║      1.17x   ║ 🔴  58.4% ║
║        4  ║   1.683s  ║     300,213  ║      1.46x   ║ 🔴  36.6% ║
║        8  ║   1.640s  ║     300,213  ║      1.50x   ║ 🔴  18.8% ║
╚═══════════╩═══════════╩══════════════╩══════════════╩═══════════╝
```

**Métricas calculadas:**

- **Speedup**: cuántas veces más rápido es respecto a 1 solo worker (`tiempo_1_worker / tiempo_N_workers`)
- **Eficiencia**: qué tan bien se aprovecha cada worker adicional (`Speedup / N_workers × 100`)
- **Integridad**: verificación automática de que todos los workers retornaron la misma cantidad de registros

---

## Estructura del proyecto

```
data_processor/
│
├── main.py                  # Punto de entrada (modo interactivo y CLI)
├── generate_data.py         # Generador de datos de prueba
├── requirements.txt         # Dependencias del proyecto
├── README.md
│
├── processor/
│   ├── reader.py            # Lectura y validación de CSV/XLSX
│   ├── filters.py           # Filtros dinámicos con operadores
│   ├── worker.py            # Unidad de trabajo por worker
│   └── orchestrator.py      # División de carga y gestión de workers
│
├── utils/
│   ├── logger.py            # Logging centralizado coordinado con tqdm
│   ├── timer.py             # Medición de tiempo de ejecución
│   ├── exporter.py          # Exportación de resultados a CSV
│   └── benchmark.py         # Benchmark comparativo de rendimiento
│
├── data/                    # Carpeta para archivos de entrada
└── output/                  # Carpeta para resultados exportados (auto-generada)
```

---

## Formato del archivo de entrada

El archivo debe contener las siguientes columnas (CSV o XLSX):

```
cedula, login, telefono1, telefono2, direccion, ciudad, estado, saldo
```

---

## Resultados y logs

Los resultados exportados se guardan en `output/` con nombre descriptivo e incluyen timestamp para evitar sobreescrituras:

```
output/resultado_saldo_gt_100_20260416_213203.csv
```

Los logs de cada ejecución se guardan automáticamente en `logs/` con timestamp:

```
logs/ejecucion_20260416_213155.log
```

---

## Dependencias

| Librería   | Versión  | Uso                                          |
|------------|----------|----------------------------------------------|
| pandas     | 2.2.2    | Lectura y manipulación de datos CSV/XLSX     |
| openpyxl   | 3.1.2    | Motor de lectura para archivos `.xlsx`       |
| numpy      | 1.26.4   | Operaciones numéricas de soporte             |
| tqdm       | 4.66.4   | Barra de progreso en consola                 |

## Autor

Desarrollado por **Roberto Franco**  
GitHub: [@paladinfranco](https://github.com/paladinfranco)

---