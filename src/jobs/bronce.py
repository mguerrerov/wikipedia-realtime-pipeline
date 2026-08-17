"""Bronze: de la cola a Iceberg, sin transformar.

Guarda el evento tal y como llego, como texto, junto con los metadatos del
sobre (particion, desplazamiento, marca de tiempo). No interpreta el JSON: si
manana cambia el esquema de Wikimedia, esta capa sigue funcionando y el
problema se resuelve en Silver, con los datos crudos todavia disponibles.

Los metadatos del sobre no son decorativos: `(particion, desplazamiento)`
identifica de forma unica cada mensaje de la cola, y es lo que permite
demostrar que un reinicio no pierde ni duplica.

Uso:
    spark-submit /app/src/jobs/bronce.py --duracion 120
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/app")

from src import almacenamiento, fuente_eventos  # noqa: E402

log = logging.getLogger("bronce")

NOMBRE_JOB = "bronce"
TABLA = "%s.bronce.cambios" % almacenamiento.NOMBRE_CATALOGO


def crear_sesion() -> SparkSession:
    constructor = SparkSession.builder.appName("bronce")
    for clave, valor in almacenamiento.configuracion().items():
        constructor = constructor.config(clave, valor)
    return constructor.getOrCreate()


def crear_tabla(spark: SparkSession) -> None:
    """Crea la tabla si no existe. Idempotente, se puede llamar en cada arranque."""
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS %s.bronce" % almacenamiento.NOMBRE_CATALOGO
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS %s (
            clave           STRING,
            valor           STRING,
            topico          STRING,
            particion       INT,
            desplazamiento  BIGINT,
            ts_cola         TIMESTAMP,
            ingerido_en     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ts_cola))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            -- Los ficheros de un job de streaming salen pequenos. Sin esto se
            -- acumulan miles y las consultas se degradan.
            'write.target-file-size-bytes' = '134217728'
        )
        """
        % TABLA
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duracion",
        type=int,
        default=0,
        help="segundos de ejecucion. 0 = indefinido (hay que pararlo)",
    )
    parser.add_argument(
        "--intervalo",
        default="10 seconds",
        help="cada cuanto se dispara un micro-lote",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    spark = crear_sesion()
    spark.sparkContext.setLogLevel("WARN")
    crear_tabla(spark)

    # El job no sabe si detras hay Kafka o Kinesis: pide formato y opciones.
    lectura = fuente_eventos.crear_lectura()
    formato, opciones = lectura.formato_y_opciones()
    log.info("Leyendo con formato %s", formato)

    crudo = spark.readStream.format(formato).options(**opciones).load()

    # Del sobre solo se toma lo que identifica el mensaje. El contenido va tal
    # cual, sin parsear.
    eventos = crudo.select(
        F.col("key").cast("string").alias("clave"),
        F.col("value").cast("string").alias("valor"),
        F.col("topic").alias("topico"),
        F.col("partition").alias("particion"),
        F.col("offset").alias("desplazamiento"),
        F.col("timestamp").alias("ts_cola"),
        F.current_timestamp().alias("ingerido_en"),
    )

    consulta = (
        eventos.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=args.intervalo)
        .option("checkpointLocation", almacenamiento.ruta_checkpoint(NOMBRE_JOB))
        # Sin esto Iceberg ordena por particion antes de escribir, lo que en
        # streaming anade una barrera innecesaria: aqui solo hay una particion
        # de fecha viva a la vez.
        .option("fanout-enabled", "true")
        .toTable(TABLA)
    )

    log.info("Job en marcha. Tabla: %s", TABLA)
    try:
        if args.duracion:
            consulta.awaitTermination(args.duracion)
            log.info("Duracion alcanzada, parando limpiamente")
            consulta.stop()
        else:
            consulta.awaitTermination()
    except KeyboardInterrupt:
        log.info("Interrumpido, parando limpiamente")
        consulta.stop()
    finally:
        ultimo = consulta.lastProgress
        if ultimo:
            log.info(
                "Ultimo lote: %s filas, %s filas/s",
                ultimo.get("numInputRows"),
                round(ultimo.get("processedRowsPerSecond") or 0, 1),
            )
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
