# Pipeline de Datos de Estaciones de Bicicletas

Este repositorio contiene el código de un pipeline de datos diseñado para procesar, limpiar y analizar la disponibilidad de bicicletas en un sistema de uso compartido urbano.

## Propósito del Proyecto

Para que una ciudad pueda ofrecer un servicio de bicicletas públicas eficiente, es fundamental entender cómo se utilizan. ¿Qué barrios tienen más demanda? ¿A qué horas se vacían las estaciones? ¿Dónde hacen falta más bicicletas?

Este proyecto aborda estas preguntas mediante la creación de un pipeline de datos que transforma datos crudos de estaciones en información útil. El objetivo es generar reportes que ayuden a los gestores de la ciudad a tomar mejores decisiones, como optimizar la redistribución de bicicletas o planificar la creación de nuevas estaciones.

## Requisitos

- Python 3.10 o superior.
- Librerías: `pandas`, `pyarrow`, `tabulate`.

## Estructura del Repositorio

```
.
├── data/
│   ├── drops/              # 1. Archivos de ingesta (JSON de entrada)
│   ├── quarantine/         # 4. Registros que fallaron la validación
│   └── storage/
│       ├── bronze/         # 2. Datos crudos almacenados en Parquet
│       ├── silver/         # 3. Datos limpios y validados
│       └── gold/           # 5. Datos agregados para análisis
├── ingest/
│   ├── get_data.py       # Script para generar datos de prueba
│   └── run.py            # Script principal del pipeline ETL
├── output/
│   └── reporte.md        # 6. Reporte final para el usuario
├── site/                   # Sitio web de Quartz para documentación y reportes
├── tools/
│   └── copy_report_to_site.py # Script para publicar el reporte en el sitio
└── README.md               # Este archivo
```

## Cómo Funciona el Pipeline

El script `ingest/run.py` orquesta todo el proceso:

1.  **Ingesta (Drops)**: Lee los archivos `.json` de la carpeta `data/drops/`.
2.  **Capa Bronze**: Los datos crudos se guardan inmediatamente en `data/storage/bronze/` en formato Parquet, particionados por la fecha de ingesta. Esta es una copia fiel de los datos de origen.
3.  **Limpieza y Cuarentena**: Los datos se validan. Los registros que no cumplen las reglas (ej. `estado` incorrecto, `slots_libres` negativo) se envían a `data/quarantine/` junto con la causa del error.
4.  **Deduplicación**: Se eliminan los registros duplicados de los datos válidos para asegurar la integridad.
5.  **Capa Silver**: Los datos limpios y únicos se guardan en `data/storage/silver/`.
6.  **Capa Gold**: Se leen los datos de la capa Silver y se realizan agregaciones para obtener KPIs.
7.  **Reporte**: Se usan los datos de la capa Gold para generar un reporte profesional en `output/reporte.md`.

## Ejecución del Pipeline

### 1. Instalación
Asegúrate de tener un entorno virtual activado con las dependencias instaladas.
```bash
pip install -r requirements.txt
```

### 2. Generar Datos de Prueba (Opcional)
Si la carpeta `data/drops` está vacía, ejecuta:
```bash
python ingest/get_data.py
```

### 3. Ejecutar el Pipeline Principal
Este comando orquesta todos los pasos descritos anteriormente.
```bash
python ingest/run.py
```

### 4. Publicar el Reporte en el Sitio Web (Opcional)
Para actualizar el reporte en el sitio de Quartz:
```bash
python tools/copy_report_to_site.py
```

---

## Decisiones de Diseño y Supuestos

### Idempotencia
El pipeline es idempotente gracias a una estrategia de **sobreescritura**. En cada ejecución, las capas `silver` y `gold` son eliminadas y regeneradas por completo. Esto garantiza que ejecutar el pipeline varias veces sobre los mismos datos de origen siempre producirá el mismo resultado final, sin duplicados.

### Trazabilidad
Para poder auditar el origen de cada dato, se han añadido las siguientes columnas a todos los registros desde el momento de la ingesta:
- `_ingest_ts`: Timestamp en UTC que marca el momento exacto en que se ejecutó el pipeline.
- `_source_file`: Nombre del fichero de origen del que se leyó el registro.
- `_batch_id`: Un identificador único para cada ejecución del pipeline.

### Deduplicación
- **Clave Natural:** Se considera que un registro es único por la combinación de `id` (identificador de la estación) y `extraction_date` (fecha del dump de datos).
- **Política de "Último Gana":** Si existen registros duplicados para la misma clave natural, el pipeline conservará únicamente el último registro basándose en el `_ingest_ts`. Esto asegura que siempre procesemos la versión más reciente de un dato si este llegara repetido.

### Supuestos
- Los ficheros de entrada en la carpeta `drops` están en formato **JSON Lines** (un objeto JSON por línea).
- El nombre de los ficheros de entrada sigue el patrón `*_YYYYMMDD.json` para poder inferir la fecha de extracción. Si el formato falla, se usará la fecha del día de la ejecución.
