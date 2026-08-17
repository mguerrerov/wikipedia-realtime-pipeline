# Matriz de versiones

Ninguna versión de este documento está estimada. Cada una se comprobó contra
el registro correspondiente el 17 de agosto de 2026, y las dependencias
transitivas (Hadoop, Scala, SDK de AWS) se leyeron de los POM publicados, no de
la documentación.

Prohibido `latest` y prohibido rango abierto en todo el proyecto.

## La cadena que importa

El fallo clásico de este stack es una incompatibilidad entre Spark, Scala,
Hadoop, el conector S3A y las JAR de Iceberg. La cadena se fija de arriba abajo:
**Spark manda, y todo lo demás se deriva de él.**

| Pieza | Versión | De dónde sale |
|---|---|---|
| Spark | **3.5.9** | Imagen `apache/spark:3.5.9`, la única 3.5.x publicada al día de hoy |
| Scala | **2.12.18** | `<scala.version>` del POM `spark-parent_2.12:3.5.9` |
| Hadoop | **3.3.4** | `<hadoop.version>` del mismo POM |
| Iceberg | **1.11.0** | Última con artefacto `iceberg-spark-runtime-3.5_2.12` |
| AWS SDK v1 | **1.12.262** | `<aws-java-sdk.version>` del POM `hadoop-project:3.3.4` |

Los tres primeros no son elección: vienen impuestos por la imagen de Spark.
La única decisión real es la versión de Iceberg, y está acotada por la
existencia del artefacto.

### JAR verificadas una a una

Comprobadas con petición HTTP a Maven Central. Las cinco respondieron `200`:

```
org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar
org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar
org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar
```

`iceberg-spark-runtime-3.5_2.12` existe hasta la 1.11.0. Las combinaciones que
**no** existen y que conviene tener presentes para no perder una tarde:
Spark 3.3 se quedó en Iceberg 1.8.1, Spark 3.2 en 1.4.3 y Spark 3.0 en 1.0.0.
Subir de Spark 3.5 obligaría a Scala 2.13, porque las variantes 4.0 y 4.1 de
Iceberg solo se publican para `_2.13`.

`hadoop-aws` y `aws-java-sdk-bundle` tienen que ir **exactamente** en 3.3.4 y
1.12.262. Es el punto donde más gente se estrella: mezclar un `hadoop-aws` de
otra versión con el Hadoop que trae Spark da `NoSuchMethodError` en tiempo de
ejecución, no en el arranque, así que el job parece sano hasta que escribe.

## Imágenes de contenedor

| Servicio | Imagen | Por qué esa |
|---|---|---|
| Cola | `redpandadata/redpanda:v25.3.16` | Último parche de la serie 25.3, ya madura. Se evita 26.x a propósito: es reciente y esto no necesita nada de lo que trae |
| Consola | `redpandadata/console:v3.10.0` | Para ver los mensajes llegar al topic sin escribir un consumidor |
| Productor | `python:3.10.21-slim-bookworm` | Misma serie 3.10 que el Python local (3.10.6), para que lo que funciona fuera funcione dentro |
| Proceso | `apache/spark:3.5.9` | Fija toda la cadena de arriba. Se usa a partir de la fase 2 |
| Almacenamiento | `minio/minio:RELEASE.2025-09-07T16-13-09Z` | Habla S3, se usa a partir de la fase 2 |

## Dependencias de Python

Fijadas en `requirements.txt`, ambas aprobadas explícitamente.

| Paquete | Versión | Para qué |
|---|---|---|
| `confluent-kafka` | **2.15.0** | Cliente de Kafka del productor. Envoltorio de librdkafka |
| `boto3` | **1.43.72** | Cliente de Kinesis, la otra implementación de la fuente |

`confluent-kafka` 2.15.0 publica wheel `cp310` para `win_amd64`, comprobado: no
hace falta compilar nada en el equipo de desarrollo.

Nada más. La captura y el análisis de la fase 0 siguen sin dependencias.

## Qué queda por verificar

**EMR Serverless.** La versión de Spark que ofrece la release de EMR elegida
tiene que ser 3.5.x para que los jobs sean idénticos en local y en cloud. Se
confirma en la fase 5, contra la documentación de AWS, antes de escribir el
Terraform. Si la release disponible trae otra versión de Spark, **manda EMR** y
hay que recalcular toda esta tabla desde ahí: es más fácil bajar la versión
local que discutir con AWS.
