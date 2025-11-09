---
title: "2. Limpieza y Calidad de Datos"
description: "Cómo el pipeline valida los datos y gestiona los registros inválidos."
---

## La Importancia de Datos Fiables

Para que los análisis sobre el uso de las bicicletas sean correctos, los datos deben ser fiables. Un solo error, como un número negativo de bicicletas o un estado mal escrito, podría distorsionar las conclusiones. La fase de limpieza es, por tanto, un pilar fundamental de este proyecto.

El proceso de limpieza toma los datos crudos de la capa **Bronze** y los somete a una serie de validaciones para asegurar su integridad y coherencia.

### Reglas de Validación Aplicadas

Cada registro de cada estación debe cumplir las siguientes reglas de negocio:

1.  **Dominio del Campo `estado`**:
    - **Regla**: El estado de una estación solo puede tener ciertos valores permitidos.
    - **Valores Válidos**: `["libre", "ocupado"]`.
    - **Motivo**: Esta regla garantiza la consistencia. Evita procesar datos con errores tipográficos o ambiguos como "Libre", "OCUPADO" o "no disponible", que podrían ser interpretados incorrectamente.

2.  **Rango del Campo `slots_libres`**:
    - **Regla**: El número de espacios libres para aparcar bicicletas debe ser un número entero igual o mayor que cero.
    - **Valores Válidos**: `0, 1, 2, ...`
    - **Motivo**: Es físicamente imposible que haya un número negativo de espacios. Un valor así indica un error en el sensor o en la transmisión de datos, y debe ser descartado.

## Gestión de Datos en Cuarentena

¿Qué pasa con los datos que no cumplen estas reglas?

En lugar de eliminarlos, el pipeline los aísla en una **zona de cuarentena**.

- **Destino**: Los registros inválidos se guardan en la carpeta `data/quarantine/`.
- **Formato**: Se almacenan en un archivo JSON que incluye no solo el dato original, sino también la razón por la que fue rechazado.
- **Propósito**: Esta separación es clave para:
    - **No contaminar los análisis**: Asegura que los reportes se basen únicamente en datos de alta calidad.
    - **Auditoría y diagnóstico**: Permite a los responsables del sistema analizar los errores. Por ejemplo, si una estación envía constantemente datos erróneos, podría necesitar mantenimiento.

Los datos que superan todas las validaciones se consideran "limpios" y avanzan a la siguiente fase: la capa **Silver**, donde se preparan para el modelado.