import json
import random
from datetime import datetime
from pathlib import Path

# --- CONFIGURACIÓN ---
NUM_REGISTROS = 5000
BARRIOS = [
    "Centro", "Retiro", "Salamanca", "Chamartín", "Tetuán", "Chamberí",
    "Fuencarral-El Pardo", "Moncloa-Aravaca", "Latina", "Carabanchel",
    "Usera", "Puente de Vallecas", "Moratalaz", "Ciudad Lineal",
    "Hortaleza", "Villaverde", "Villa de Vallecas", "Vicálvaro",
    "San Blas-Canillejas", "Barajas"
]
ESTADOS = ["libre", "ocupado"]
DIRECTORIO_SALIDA = Path(__file__).resolve().parent.parent / "data" / "drops"

def _introducir_errores(dato_estacion: dict) -> dict:
    # Introduce aleatoriamente errores en un registro para pruebas.
    if random.random() < 0.1:
        dato_estacion["estado"] = "invalido"
    if random.random() < 0.05:
        dato_estacion["slots_libres"] = -random.randint(1, 5)
    if random.random() < 0.05:
        dato_estacion["slots_libres"] = "NaN"
    return dato_estacion

def generar_datos_estaciones(num_registros: int) -> list[dict]:
    # Genera una lista de registros de estaciones, incluyendo errores.
    print(f"Generando {num_registros} registros de prueba...")
    datos_generados = []
    for i in range(1, num_registros + 1):
        dato_estacion = {
            "id": str(i),
            "barrio": random.choice(BARRIOS),
            "estado": random.choice(ESTADOS),
            "slots_libres": random.randint(0, 50)
        }
        dato_con_errores = _introducir_errores(dato_estacion)
        datos_generados.append(dato_con_errores)
    return datos_generados

def guardar_datos_en_jsonl(datos: list[dict], ruta_salida: Path):
    # Guarda una lista de registros en un archivo JSON Lines.
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)
    with open(ruta_salida, 'w', encoding='utf-8') as f:
        for dato in datos:
            f.write(json.dumps(dato) + '\n')
    print(f"Datos generados exitosamente en: '{ruta_salida}'")

def main():
    # Orquesta la generación y guardado de datos de estaciones.
    datos_estaciones = generar_datos_estaciones(NUM_REGISTROS)
    
    # Genera un nombre de archivo con fecha y hora para asegurar que sea único.
    timestamp = datetime.now().strftime('%Y%m%d')
    nombre_archivo = f"estaciones_{timestamp}.json"
    ruta_salida = DIRECTORIO_SALIDA / nombre_archivo
    
    guardar_datos_en_jsonl(datos_estaciones, ruta_salida)

if __name__ == "__main__":
    main()