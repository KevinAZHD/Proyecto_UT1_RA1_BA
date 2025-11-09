---
title: 1. Ingesta y Almacenamiento
description: "Fase inicial del pipeline: cómo se recogen y almacenan los datos crudos."
---

## Contexto del Proyecto

Para que una ciudad pueda ofrecer un servicio de bicicletas públicas eficiente, es fundamental entender cómo se utilizan. ¿Qué barrios tienen más demanda? ¿A qué horas se vacían las estaciones? ¿Dónde hacen falta más bicicletas?

Este proyecto aborda estas preguntas mediante la creación de un pipeline de datos que procesa la información de disponibilidad de las estaciones de bicicletas. El objetivo es transformar datos crudos en información útil que ayude a los gestores de la ciudad a tomar mejores decisiones, como optimizar la redistribución de bicicletas o planificar la creación de nuevas estaciones.

## Proceso de Ingesta

El pipeline comienza con la recogida de los datos.

1.  **Origen de Datos**: El proceso se alimenta de archivos `.json` que se depositan periódicamente en la carpeta `data/drops/`. Cada archivo representa un "dump" o volcado de datos de un momento concreto y contiene el estado de las estaciones (ej. `id`, `barrio`, `slots_libres`).

2.  **Enriquecimiento Inicial**: Al leer cada registro, el sistema lo enriquece con metadatos clave para asegurar su trazabilidad:
    *   `extraction_date`: La fecha en que se generaron los datos, extraída del nombre del archivo.
    *   `_ingest_ts`: La fecha y hora exactas en que el registro fue procesado por el pipeline.
    *   `_source_file`: El nombre del archivo del que proviene el dato.

3.  **Almacenamiento en Crudo (Capa Bronze)**:
    *   Inmediatamente después de la lectura, todos los datos, sin ninguna alteración, se guardan en la capa **Bronze** (`data/storage/bronze/`).
    *   Esta capa actúa como un archivo histórico de todo lo que se ha recibido. Se almacena en formato **Parquet** y se particiona por fecha de ingesta para facilitar consultas a futuro.

Este primer paso asegura que todos los datos de origen se respalden de forma eficiente antes de pasar a las fases de limpieza y transformación.
