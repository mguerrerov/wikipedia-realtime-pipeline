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
| Spark | **3.5.6** | La que trae EMR Serverless 7.13.0. Manda el destino |
| **Java** | **17** | Corretto 17 en EMR 7.x; e Iceberg lo exige. Ver abajo |
| Scala | **2.12.18** | `<scala.version>` del POM `spark-parent_2.12:3.5.6` |
| Hadoop | **3.3.4** | `<hadoop.version>` del mismo POM |
| Iceberg | **1.10.0** | La que trae EMR Serverless 7.13.0 (`1.10.0-amzn-1`) |
| AWS SDK v1 | **1.12.262** | `<aws-java-sdk.version>` del POM `hadoop-project:3.3.4` |

### Manda EMR, no el portátil

La primera versión de esta tabla fijaba Spark 3.5.9 e Iceberg 1.11.0 por ser lo
más reciente disponible. Al verificar la documentación de AWS —antes de escribir
una línea de Terraform— resultó que EMR Serverless 7.13.0 trae **Spark 3.5.6 e
Iceberg 1.10.0-amzn-1**.

Se baja el entorno local a esas versiones. El razonamiento es el de siempre: es
más fácil bajar una versión en el portátil que discutir con AWS, y el objetivo
del proyecto es que los jobs sean idénticos en los dos entornos. Hadoop y Scala
no cambian (3.3.4 y 2.12.18), así que el ajuste solo toca dos piezas.

Coincidencia afortunada: el cambio a Java 17 que hubo que hacer en la fase 2
para que Iceberg cargara nos dejó alineados con Corretto 17, el JDK por defecto
de EMR 7.x, sin haberlo buscado.

### El eje Java, que no basta con que la JAR exista

La primera versión de esta matriz daba por buena la JAR de Iceberg porque el
artefacto existe para Spark 3.5 y Scala 2.12. Existe, y aun así el job murió al
crear la sesión:

```
java.lang.UnsupportedClassVersionError: org/apache/iceberg/spark/ExtendedParser
has been compiled by a more recent version of the Java Runtime
(class file version 61.0), this version only recognizes up to 55.0
```

Class file 61 es Java 17; la 55 es Java 11. Las etiquetas cortas de
`apache/spark` traen Java 11, así que Iceberg no puede ni cargarse.

**Que la JAR exista para tu Spark y tu Scala no significa que funcione con tu
Java.** Son tres ejes, no dos. La solución fue la etiqueta larga de la misma
versión de Spark, que solo cambia el runtime — Spark 3.5 soporta Java 17
oficialmente — y además alinea con EMR 7.x, que ya va sobre Java 17.

La alternativa era bajar Iceberg a una versión compilada para Java 11, y se
descartó: ata el proyecto a una Iceberg más antigua para arreglar un problema
que no está en Iceberg.

### JAR verificadas una a una

Las siete que van dentro de la imagen, comprobadas con petición HTTP a Maven
Central. Las siete respondieron `200`:

```
org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar
org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar
org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.6/spark-token-provider-kafka-0-10_2.12-3.5.6.jar
org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
```

Las dos últimas son dependencias transitivas del conector de Kafka, leídas de su
POM. Sin ellas el job arranca y falla al leer, que es la forma más cara de
descubrir que falta una JAR.

`iceberg-spark-runtime-3.5_2.12` existe hasta la 1.11.0, pero se usa la 1.10.0
para igualar EMR. Las combinaciones que **no** existen y que conviene tener
presentes para no perder una tarde:
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
| Proceso | `apache/spark:3.5.6-scala2.12-java17-python3-r-ubuntu` | Iguala EMR Serverless 7.13.0. Se usa a partir de la fase 2 |
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

## EMR Serverless: verificado el 17/08/2026

Release de referencia: **EMR Serverless 7.13.0**.

| Pieza | En EMR | En local | Estado |
|---|---|---|---|
| Spark | 3.5.6 | 3.5.6 | Alineado |
| Iceberg | 1.10.0-amzn-1 | 1.10.0 | Alineado |
| Java | Corretto 17 | Temurin 17 | Alineado |
| Conector Kinesis | Preinstalado desde 7.1 | — | No hay que empaquetarlo |

El conector de Kinesis viene en la imagen desde EMR 7.1, en
`/usr/share/aws/kinesis/spark-sql-kinesis/lib/`. Por eso `paquetes_maven()`
devuelve cadena vacía en la implementación de Kinesis.

Nombres verificados, no supuestos: el formato es **`aws-kinesis`** (no
`kinesis`) y las opciones llevan prefijo: `kinesis.streamName`,
`kinesis.region`, `kinesis.startingposition` —esta última toda en minúscula, a
diferencia de las otras dos—.

Fuentes: documentación de EMR Serverless (versiones de release y release
7.13.0), historial de versiones de Iceberg en EMR, página del conector de
Kinesis para Structured Streaming y README de `awslabs/spark-sql-kinesis-connector`.
