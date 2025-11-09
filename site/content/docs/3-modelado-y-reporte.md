---
title: "3. Modelado y Reporte"
description: "Cómo se transforman los datos limpios en métricas útiles para el análisis."
---

## De Datos Limpios a Información Útil

Una vez que tenemos datos limpios y fiables en la capa **Silver**, el siguiente paso es transformarlos en información que responda a nuestras preguntas de negocio. Esta es la función de la capa **Gold**: convertir datos granulares en métricas agregadas o KPIs (Key Performance Indicators).

El objetivo final es generar un producto de datos claro y accionable, en este caso, un reporte que resuma la disponibilidad de bicicletas en la ciudad.

## Proceso de Modelado (Capa Gold)

El pipeline realiza una agregación para obtener una visión de alto nivel de la situación:

1.  **Agrupación por Criterio de Negocio**:
    - Los datos de la capa Silver, que contienen registros por cada estación, se agrupan por `barrio`. Este es nuestro principal eje de análisis para la gestión urbana.

2.  **Cálculo de la Métrica Principal**:
    - Para cada barrio, se calcula la **media de `slots_libres`**.
    - Esta métrica, que renombramos a `disponibilidad_media`, nos da un indicador rápido y fácil de entender sobre qué barrios tienen, en promedio, más o menos bicicletas disponibles.

3.  **Resultado Agregado**:
    - El resultado es una tabla sencilla y potente donde cada fila representa un barrio y su nivel de disponibilidad.

    | Barrio    | Disponibilidad Media |
    |-----------|----------------------|
    | Centro    | 12.75                |
    | El Carmen | 8.50                 |
    | ...       | ...                  |

## Generación del Reporte Final

Con los datos de la capa Gold listos, el último paso es comunicarlos.

- **Formato**: Se genera un reporte en formato **Markdown** (`reporte.md`) en la carpeta `output/`.
- **Contenido**: El reporte presenta los resultados del análisis de forma clara, principalmente a través de la tabla de disponibilidad media por barrio.
- **Publicación**: Este archivo Markdown se copia automáticamente al sitio web de documentación, asegurando que los stakeholders siempre tengan acceso a la información más reciente en la sección de **[[reportes/reporte-disponibilidad|Reportes]]**.

Así, el pipeline completa su ciclo: desde datos crudos hasta un reporte con información valiosa para la toma de decisiones.