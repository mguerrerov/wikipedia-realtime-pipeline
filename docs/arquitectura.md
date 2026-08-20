# Arquitectura

Dos entornos, una sola lógica. Este documento existe para enseñar dónde está
exactamente la costura entre lo que cambia y lo que no.

## El pipeline

```mermaid
flowchart LR
    W["Wikimedia<br/>stream SSE"] --> P["Productor<br/>src/productor.py"]
    G["Generador sintético<br/>src/generador.py"] -.->|"solo ensayos"| P

    P --> Q(["Cola<br/>Kafka / Kinesis"])
    Q --> B["Bronze<br/>crudo + offsets"]
    B --> S["Silver<br/>tipado + dedup<br/>watermark 30 s"]
    S --> O["Gold<br/>ventanas de 1 min"]

    O --> C["Consumo<br/>DuckDB / Athena"]
    S --> C

    classDef tabla fill:#1f6feb22,stroke:#1f6feb;
    class B,S,O tabla;
```

Las tres tablas son Iceberg. Cada flecha entre ellas es un job de Spark
Structured Streaming con su propio punto de control.

## Las dos costuras

Todo lo que difiere entre local y AWS vive en dos módulos, y en ninguno más.
Si aparece un `if entorno == "aws"` dentro de un job, la abstracción está rota.

```mermaid
flowchart TB
    subgraph LOGICA["Lógica de proceso — idéntica en los dos entornos"]
        J1["bronce.py"]
        J2["plata.py"]
        J3["oro.py"]
    end

    subgraph COSTURA["Las dos costuras"]
        F["src/fuente_eventos/<br/>de dónde vienen los eventos"]
        A["src/almacenamiento.py<br/>dónde acaban las tablas"]
    end

    subgraph LOCAL["Local — FUENTE_EVENTOS=kafka, CATALOGO=hadoop"]
        L1["Redpanda"]
        L2["MinIO + catálogo de ficheros"]
    end

    subgraph AWS["AWS — FUENTE_EVENTOS=kinesis, CATALOGO=glue"]
        A1["Kinesis Data Streams"]
        A2["S3 + Glue Data Catalog"]
    end

    LOGICA --> F
    LOGICA --> A
    F --> L1
    F --> A1
    A --> L2
    A --> A2
```

La selección es por variable de entorno, no por rama en el código.

## Equivalencias

| Capa | Local | AWS |
|---|---|---|
| Cola | Redpanda (API Kafka) | Kinesis Data Streams |
| Proceso | Spark en contenedor | EMR Serverless |
| Objetos | MinIO (API S3) | S3 |
| Catálogo | ficheros (`version-hint.text`) | Glue Data Catalog |
| Consulta | DuckDB | Athena |
| Infraestructura | Docker Compose | Terraform |

MinIO habla S3 y Redpanda habla Kafka, así que la ruta `s3a://` y el cliente de
consumo son los mismos en los dos sitios. Es la razón de esta elección de stack,
no una casualidad.

## Por qué el contrato es la tabla, no el motor

Spark escribe las tablas. **DuckDB las lee sin haber participado en la
escritura y sin conocer el catálogo**: abre la tabla por su ruta y sigue el
`version-hint.text` que dejó el catálogo de ficheros. No hay exportación, no hay
copia, no hay proceso intermedio.

Ese es el argumento de Iceberg, y `src/consumo_duckdb.py` existe para
demostrarlo: las mismas preguntas que responde `src/jobs/consultas.py` desde
Spark, respondidas desde otro motor, con los mismos números.

En AWS el papel de DuckDB lo hace Athena sobre Glue. Cambia el motor; la tabla
no.
