import pandas as pd
import json
import re
from datetime import datetime, date, timezone
from pathlib import Path
import shutil
import uuid

# --- CONFIGURACIÓN DE RUTAS Y CONSTANTES ---
RAIZ = Path(__file__).resolve().parent.parent
DIR_DATOS = RAIZ / "data"
DIR_SALIDA = RAIZ / "output"
DIR_DROPS = DIR_DATOS / "drops"
DIR_CUARENTENA = DIR_DATOS / "quarantine"
DIR_BRONZE = DIR_DATOS / "storage" / "bronze"
DIR_SILVER = DIR_DATOS / "storage" / "silver"
DIR_GOLD = DIR_DATOS / "storage" / "gold"
ARCHIVO_REPORTE = DIR_SALIDA / "reporte.md"
ESTADOS_VALIDOS = ["libre", "ocupado"]
FECHA_HOY = date.today()

def preparar_directorios():
    # Crea todos los directorios necesarios para el pipeline si no existen.
    for dir_path in [DIR_DROPS, DIR_CUARENTENA, DIR_BRONZE, DIR_SILVER, DIR_GOLD, DIR_SALIDA]:
        dir_path.mkdir(parents=True, exist_ok=True)

def ingerir_datos(ts_ingesta: datetime, id_batch: str) -> pd.DataFrame:
    # Lee archivos JSON de 'drops', los une y añade metadatos de trazabilidad.
    todos_los_datos = []
    archivos_json = list(DIR_DROPS.glob('*.json'))
    if not archivos_json:
        return pd.DataFrame()

    for ruta_archivo in archivos_json:
        match = re.search(r'estaciones_(\d{8})', ruta_archivo.name)
        if match:
            fecha_str = match.group(1)
            fecha_extraccion = datetime.strptime(fecha_str, '%Y%m%d').date()
        else:
            fecha_extraccion = FECHA_HOY
            
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            for linea in f:
                try:
                    registro = json.loads(linea)
                    registro.update({
                        '_ingest_ts': ts_ingesta,
                        '_source_file': ruta_archivo.name,
                        '_batch_id': id_batch,
                        'extraction_date': fecha_extraccion
                    })
                    todos_los_datos.append(registro)
                except json.JSONDecodeError:
                    continue
    return pd.DataFrame(todos_los_datos)

def almacenar_en_bronze(df: pd.DataFrame):
    # Guarda el DataFrame crudo en la capa Bronze.
    df['ingest_date'] = df['_ingest_ts'].dt.date
    df_existente = pd.DataFrame()
    
    if DIR_BRONZE.exists():
        try:
            df_existente = pd.read_parquet(DIR_BRONZE, engine='pyarrow')
            df_existente['_ingest_ts'] = pd.to_datetime(df_existente['_ingest_ts'])
            df_existente['extraction_date'] = pd.to_datetime(df_existente['extraction_date']).dt.date
            df_existente['ingest_date'] = pd.to_datetime(df_existente['ingest_date']).dt.date
        except:
            pass
    
    df_combinado = pd.concat([df_existente, df], ignore_index=True)
    df_combinado = df_combinado.sort_values(by='_ingest_ts').drop_duplicates(subset=['id', 'extraction_date'], keep='last')
    
    if DIR_BRONZE.exists():
        shutil.rmtree(DIR_BRONZE)
    DIR_BRONZE.mkdir()
    
    df_combinado.astype(str).to_parquet(DIR_BRONZE, partition_cols=['ingest_date'], engine='pyarrow')

def limpiar_y_validar_datos(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Limpia, valida y separa el DataFrame en registros válidos e inválidos.
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    df['_quarantine_reason'] = ''
    df.loc[~df['estado'].isin(ESTADOS_VALIDOS), '_quarantine_reason'] += 'estado_invalido;'
    df['slots_libres'] = pd.to_numeric(df['slots_libres'], errors='coerce')
    df.loc[df['slots_libres'].isna(), '_quarantine_reason'] += 'slots_no_numerico;'
    df.loc[df['slots_libres'] < 0, '_quarantine_reason'] += 'slots_negativo;'

    df_validos = df[df['_quarantine_reason'] == ''].copy().drop(columns=['_quarantine_reason'])
    df_invalidos = df[df['_quarantine_reason'] != ''].copy()
    df_validos['slots_libres'] = df_validos['slots_libres'].astype(int)
    
    return df_validos, df_invalidos

def deduplicar_datos(df: pd.DataFrame) -> pd.DataFrame:
    # Elimina duplicados, manteniendo el último registro recibido.
    if df.empty:
        return pd.DataFrame()
    return df.sort_values(by='_ingest_ts').drop_duplicates(subset=['id', 'extraction_date'], keep='last')

def poner_en_cuarentena_datos_invalidos(df: pd.DataFrame):
    # Guarda los datos inválidos en la carpeta de cuarentena.
    archivo_cuarentena = DIR_CUARENTENA / f"quarantined_{FECHA_HOY.strftime('%Y%m%d')}.json"
    df['_quarantine_reason'] = df['_quarantine_reason'].str.strip(';')
    df.to_json(archivo_cuarentena, orient='records', indent=4, date_format='iso')
    print(f"Datos inválidos guardados en: {archivo_cuarentena}")

def almacenar_en_silver(df: pd.DataFrame):
    # Almacena los datos limpios en la capa Silver.
    df['extraction_date'] = pd.to_datetime(df['extraction_date'])
    if DIR_SILVER.exists():
        shutil.rmtree(DIR_SILVER)
    DIR_SILVER.mkdir()
    
    columnas_a_mantener = [col for col in df.columns if col != 'ingest_date']
    df[columnas_a_mantener].to_parquet(DIR_SILVER, partition_cols=['extraction_date'], engine='pyarrow')
    print(f"Datos limpios guardados en (Silver): {DIR_SILVER}")

def almacenar_en_gold(df: pd.DataFrame) -> pd.DataFrame:
    # Crea y almacena un agregado en la capa Gold.
    df_agregado = df.groupby('barrio')['slots_libres'].mean().reset_index()
    df_agregado.rename(columns={'slots_libres': 'disponibilidad_media'}, inplace=True)
    df_agregado['disponibilidad_media'] = df_agregado['disponibilidad_media'].round(2)
    
    nombre_archivo_gold = DIR_GOLD / f"disponibilidad_barrio_{FECHA_HOY.strftime('%Y%m%d')}.parquet"
    df_agregado.to_parquet(nombre_archivo_gold, engine='pyarrow')
    print(f"Agregado de disponibilidad guardado en (Gold): {nombre_archivo_gold}")
    return df_agregado

def generar_reporte_markdown(df_limpio: pd.DataFrame, df_agregado: pd.DataFrame, metricas: dict):
    # Genera un reporte en formato Markdown con los resultados del pipeline.
    
    # Pre-formatea todas las variables para el reporte
    fecha_gen = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    id_batch = metricas['id_batch']
    total_leidos = metricas['total_leidos']
    total_validos = metricas['total_validos']
    total_invalidos = metricas['total_invalidos']
    fuentes = ', '.join(df_limpio['_source_file'].unique())
    periodo_inicio = df_limpio['extraction_date'].min().strftime('%Y-%m-%d')
    periodo_fin = df_limpio['extraction_date'].max().strftime('%Y-%m-%d')
    
    df_reporte_agregado = df_agregado.rename(columns={'barrio': 'Barrio', 'disponibilidad_media': 'Disponibilidad Media'})
    tabla_agg = df_reporte_agregado.to_markdown(index=False)
    
    df_reporte_estado = df_limpio['estado'].value_counts().rename('Nº Estaciones').reset_index()
    df_reporte_estado.rename(columns={'estado': 'Estado'}, inplace=True)
    tabla_estado = df_reporte_estado.to_markdown(index=False)

    # Compone el contenido del reporte usando las variables pre-formateadas
    contenido_reporte = f"""
# Reporte de Disponibilidad de Estaciones
- **Fecha de generación:** {fecha_gen}
- **ID de Ejecución (Batch ID):** {id_batch}
---
## 1. Resumen de la Ejecución
| Métrica | Valor |
|---|---|
| Total Registros Leídos | {total_leidos} |
| Registros Válidos | {total_validos} |
| Registros en Cuarentena | {total_invalidos} |
---
## 2. Contexto de los Datos
- **Fuentes de Datos:** {fuentes}
- **Periodo Analizado:** De {periodo_inicio} a {periodo_fin}
---
## 3. Análisis de Disponibilidad
### 3.1. Disponibilidad Media de Slots por Barrio
{tabla_agg}
### 3.2. Conteo de Estaciones por Estado
{tabla_estado}
"""
    with open(ARCHIVO_REPORTE, 'w', encoding='utf-8') as f:
        f.write(contenido_reporte)
    print(f"Reporte final generado en: {ARCHIVO_REPORTE}")

def main():
    # Orquesta la ejecución completa del pipeline ETL.
    print("--- INICIO DEL PIPELINE DE DATOS DE ESTACIONES ---")
    
    ts_ingesta = datetime.now(timezone.utc)
    id_batch = f"batch_{ts_ingesta.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    print("\nPaso 1: Preparando directorios...")
    preparar_directorios()

    print("\nPaso 2: Ingesta de datos crudos...")
    df_crudo = ingerir_datos(ts_ingesta, id_batch)
    if df_crudo.empty:
        print("Pipeline finalizado: No se encontraron datos para procesar.")
        return
    
    print("\nPaso 3: Almacenando datos en Bronze...")
    almacenar_en_bronze(df_crudo)

    print("\nPaso 4: Limpiando y validando datos...")
    df_validos, df_invalidos = limpiar_y_validar_datos(df_crudo)

    print("\nPaso 5: Deduplicando datos...")
    df_deduplicado = deduplicar_datos(df_validos)
    
    if not df_invalidos.empty:
        print("\nPaso 6: Guardando datos inválidos en cuarentena...")
        poner_en_cuarentena_datos_invalidos(df_invalidos)

    if not df_deduplicado.empty:
        print("\nPaso 7: Procesando y almacenando datos (Silver, Gold y Reporte)...")
        almacenar_en_silver(df_deduplicado)
        df_agregado = almacenar_en_gold(df_deduplicado)
        
        metricas = {
            'id_batch': id_batch,
            'total_leidos': len(df_crudo),
            'total_validos': len(df_deduplicado),
            'total_invalidos': len(df_invalidos)
        }
        generar_reporte_markdown(df_deduplicado, df_agregado, metricas)
    else:
        print("\nPaso 7: No hay datos válidos para procesar y generar reportes.")

    print("\n--- FIN DEL PIPELINE ---")

if __name__ == "__main__":
    main()