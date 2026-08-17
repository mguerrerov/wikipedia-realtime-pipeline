"""Silver: tipado y deduplicacion sobre los datos crudos de Bronze.

Tres cosas ocurren aqui, y las tres salen de mediciones de la fase 0:

1. El JSON se parsea con un esquema explicito, no inferido. `log_params` se
   deja fuera a proposito: alterna struct y array vacio, y Spark no puede
   representar esa columna. Si algun dia hace falta, se parsea aparte y solo
   para los `log_type` que interesen.

2. El tiempo de evento es `meta.dt`, nunca `timestamp`. Medido: `timestamp`
   llega desordenado el 62 % de las veces, con un maximo de 19,3 anos, porque
   en los eventos `log` de Commons contiene la fecha original del fichero.
   `timestamp` se conserva como columna, pero no manda.

3. La deduplicacion es por `meta.id`, presente en el 100 % de los eventos,
   mientras que `id` falta en el 2,2 %.

Uso:
    spark-submit /app/src/jobs/plata.py --duracion 120
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, "/app")

from src import almacenamiento  # noqa: E402

log = logging.getLogger("plata")

NOMBRE_JOB = "plata"
ORIGEN = "%s.bronce.cambios" % almacenamiento.NOMBRE_CATALOGO
DESTINO = "%s.plata.cambios" % almacenamiento.NOMBRE_CATALOGO

# Valor decidido en la fase 0 (D3). El desorden real de la fuente es menor de
# un segundo; estos 30 s cubren una reconexion del productor o un retraso de
# consumo, no el desorden de Wikimedia.
WATERMARK = "30 seconds"

# Esquema explicito. Solo los campos que el pipeline usa.
ESQUEMA = T.StructType(
    [
        T.StructField(
            "meta",
            T.StructType(
                [
                    T.StructField("id", T.StringType()),
                    T.StructField("dt", T.StringType()),
                    T.StructField("domain", T.StringType()),
                ]
            ),
        ),
        T.StructField("id", T.LongType()),
        T.StructField("type", T.StringType()),
        T.StructField("namespace", T.IntegerType()),
        T.StructField("title", T.StringType()),
        T.StructField("user", T.StringType()),
        T.StructField("bot", T.BooleanType()),
        T.StructField("wiki", T.StringType()),
        T.StructField("server_name", T.StringType()),
        T.StructField("timestamp", T.LongType()),
    ]
)


def crear_sesion() -> SparkSession:
    constructor = SparkSession.builder.appName("plata")
    for clave, valor in almacenamiento.configuracion().items():
        constructor = constructor.config(clave, valor)
    return constructor.getOrCreate()


def crear_tabla(spark: SparkSession) -> None:
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS %s.plata" % almacenamiento.NOMBRE_CATALOGO
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS %s (
            meta_id       STRING,
            ts_evento     TIMESTAMP,
            tipo          STRING,
            wiki          STRING,
            servidor      STRING,
            espacio       INT,
            titulo        STRING,
            usuario       STRING,
            es_bot        BOOLEAN,
            ts_mediawiki  TIMESTAMP,
            ts_cola       TIMESTAMP,
            retraso_s     DOUBLE,
            ingerido_en   TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ts_evento))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd'
        )
        """
        % DESTINO
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duracion", type=int, default=0)
    parser.add_argument("--intervalo", default="10 seconds")
    parser.add_argument("--watermark", default=WATERMARK)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    spark = crear_sesion()
    spark.sparkContext.setLogLevel("WARN")
    crear_tabla(spark)

    crudo = spark.readStream.format("iceberg").load(ORIGEN)

    tipado = (
        crudo.select(
            F.from_json(F.col("valor"), ESQUEMA).alias("e"),
            F.col("ts_cola"),
        )
        # Un evento que no parsea no puede tumbar el job ni colarse como fila
        # vacia: se descarta aqui y se ve en la diferencia de conteos.
        .filter(F.col("e.meta.id").isNotNull())
        .select(
            F.col("e.meta.id").alias("meta_id"),
            # meta.dt viene como 2026-08-17T15:02:17.471Z. El formato lleva la
            # T y la Z literales, por eso van entrecomilladas en el patron.
            F.to_timestamp(F.col("e.meta.dt"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias(
                "ts_evento"
            ),
            F.col("e.type").alias("tipo"),
            F.col("e.wiki").alias("wiki"),
            F.col("e.server_name").alias("servidor"),
            F.col("e.namespace").alias("espacio"),
            F.col("e.title").alias("titulo"),
            F.col("e.user").alias("usuario"),
            F.coalesce(F.col("e.bot"), F.lit(False)).alias("es_bot"),
            F.col("e.timestamp").cast("timestamp").alias("ts_mediawiki"),
            F.col("ts_cola"),
        )
        .filter(F.col("ts_evento").isNotNull())
    )

    # Cuanto tardo el evento en llegar a la cola desde que se publico en el bus.
    # Es la latencia de ingesta, y se guarda para poder medirla despues sin
    # tener que recalcularla.
    con_retraso = tipado.withColumn(
        "retraso_s",
        F.col("ts_cola").cast("double") - F.col("ts_evento").cast("double"),
    ).withColumn("ingerido_en", F.current_timestamp())

    # El watermark hace dos cosas a la vez: acota el estado que guarda la
    # deduplicacion, y descarta lo que llegue mas tarde de ese margen.
    # dropDuplicatesWithinWatermark solo recuerda dentro de la ventana, asi que
    # el estado no crece sin limite, que es lo que pasa con dropDuplicates.
    deduplicado = con_retraso.withWatermark(
        "ts_evento", args.watermark
    ).dropDuplicatesWithinWatermark(["meta_id"])

    consulta = (
        deduplicado.select(
            "meta_id",
            "ts_evento",
            "tipo",
            "wiki",
            "servidor",
            "espacio",
            "titulo",
            "usuario",
            "es_bot",
            "ts_mediawiki",
            "ts_cola",
            "retraso_s",
            "ingerido_en",
        )
        .writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=args.intervalo)
        .option("checkpointLocation", almacenamiento.ruta_checkpoint(NOMBRE_JOB))
        .option("fanout-enabled", "true")
        .toTable(DESTINO)
    )

    log.info("Silver en marcha. Watermark: %s. Tabla: %s", args.watermark, DESTINO)
    try:
        if args.duracion:
            consulta.awaitTermination(args.duracion)
            consulta.stop()
        else:
            consulta.awaitTermination()
    except KeyboardInterrupt:
        consulta.stop()
    finally:
        ultimo = consulta.lastProgress
        if ultimo:
            log.info("Ultimo lote: %s filas", ultimo.get("numInputRows"))
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
