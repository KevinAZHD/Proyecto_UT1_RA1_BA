import shutil
from pathlib import Path

# --- CONFIGURACIÓN DE RUTAS ---
RAIZ = Path(__file__).resolve().parent.parent
RUTA_ORIGEN = RAIZ / "output" / "reporte.md"
DIRECTORIO_DESTINO = RAIZ / "site" / "content" / "reportes"
RUTA_DESTINO = DIRECTORIO_DESTINO / "reporte-disponibilidad.md"

def copiar_reporte_al_sitio(ruta_origen: Path, ruta_destino: Path):
    # Copia el archivo del reporte al directorio del sitio web.
    if not ruta_origen.exists():
        print(f"Error: No se encontró el reporte en '{ruta_origen}'.")
        print("Asegúrate de haber ejecutado el pipeline principal ('ingest/run.py') primero.")
        return

    # Crea el directorio de destino si no existe.
    ruta_destino.parent.mkdir(parents=True, exist_ok=True)

    try:
        shutil.copy(ruta_origen, ruta_destino)
        print(f"Reporte copiado exitosamente a '{ruta_destino}'")
    except Exception as e:
        print(f"Ocurrió un error al copiar el archivo: {e}")

def main():
    # Orquesta la copia del reporte generado al sitio de Quartz.
    copiar_reporte_al_sitio(RUTA_ORIGEN, RUTA_DESTINO)

if __name__ == "__main__":
    main()
