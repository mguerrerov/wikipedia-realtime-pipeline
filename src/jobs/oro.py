"""Gold: las tres preguntas del pipeline, agregadas por ventana.

P1  Actividad por wiki y espacio de nombres, minuto a minuto.
P2  Proporcion de actividad humana frente a automatica, por minuto.
P3  Paginas editadas por varias personas a la vez.

Las tres leen de Silver y escriben su propia tabla, cada una con su punto de
control. Van en el mismo job porque comparten origen y ciclo de vida; separarlas
en tres procesos multiplicaria por tres la lectura de la misma tabla.

AVISO sobre los nucleos: tres consultas de streaming simultaneas necesitan mas
de dos huecos de ejecucion. Con `--master local[2]` el job se bloquea -no va
lento: se para en seco con la CPU al 1%- porque al pararlas, el lote de una
espera un nucleo que retiene otra. Este job se lanza con `local[6]`.

En modo `append` una ventana no se emite hasta que el watermark pasa de su
final. Con watermark de 30 s, la ventana del minuto 10:00-10:01 se publica
alrededor de las 10:01:30. Ese retraso es el precio de no perder rezagados, y
es la razon por la que el valor del watermark se midio en la fase 0 en vez de
elegirlo a ojo.

Uso:
    spark-submit /app/src/jobs/oro.py --duracion 180
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/app")

from src import almacenamiento  # noqa: E402

log = logging.getLogger("oro")

CATALOGO = almacenamiento.NOMBRE_CATALOGO
ORIGEN = "%s.plata.cambios" % CATALOGO
WATERMARK = "30 seconds"

# Minimo de editores distintos para considerar que una pagina se esta editando
# "a la vez". Con 2 ya hay coincidencia; subirlo dejaria la tabla casi vacia.
MIN_EDITORES = 2


def crear_sesion() -> SparkSession:
    constructor = SparkSession.builder.appName("oro")
    for clave, valor in almacenamiento.configuracion().items():
        constructor = constructor.config(clave, valor)
    return constructor.getOrCreate()


def crear_tablas(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS %s.oro" % CATALOGO)

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS %s.oro.actividad_por_wiki (
            ventana_inicio TIMESTAMP,
            ventana_fin    TIMESTAMP,
            servidor       STRING,
            espacio        INT,
            eventos        BIGINT
        ) USING iceberg PARTITIONED BY (days(ventana_inicio))
        """
        % CATALOGO
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS %s.oro.humano_vs_bot (
            ventana_inicio TIMESTAMP,
            ventana_fin    TIMESTAMP,
            es_bot         BOOLEAN,
            eventos        BIGINT
        ) USING iceberg PARTITIONED BY (days(ventana_inicio))
        """
        % CATALOGO
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS %s.oro.paginas_concurrentes (
            ventana_inicio TIMESTAMP,
            ventana_fin    TIMESTAMP,
            wiki           STRING,
            titulo         STRING,
            editores       BIGINT,
            ediciones      BIGINT
        ) USING iceberg PARTITIONED BY (days(ventana_inicio))
        """
        % CATALOGO
    )


def _lanzar(df, tabla: str, nombre_checkpoint: str, intervalo: str):
    return (
        df.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=intervalo)
        .option("checkpointLocation", almacenamiento.ruta_checkpoint(nombre_checkpoint))
        .option("fanout-enabled", "true")
        .toTable(tabla)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duracion", type=int, default=0)
    parser.add_argument("--intervalo", default="15 seconds")
    parser.add_argument("--watermark", default=WATERMARK)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    spark = crear_sesion()
    spark.sparkContext.setLogLevel("WARN")
    crear_tablas(spark)

    plata = (
        spark.readStream.format("iceberg")
        .load(ORIGEN)
        .withWatermark("ts_evento", args.watermark)
    )

    # --- P1: actividad por wiki y espacio de nombres ---
    p1 = (
        plata.groupBy(
            F.window("ts_evento", "1 minute"), F.col("servidor"), F.col("espacio")
        )
        .count()
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "servidor",
            "espacio",
            F.col("count").alias("eventos"),
        )
    )

    # --- P2: humano frente a bot ---
    p2 = (
        plata.groupBy(F.window("ts_evento", "1 minute"), F.col("es_bot"))
        .count()
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "es_bot",
            F.col("count").alias("eventos"),
        )
    )

    # --- P3: paginas editadas por varias personas a la vez ---
    # Ventana deslizante: 5 minutos de ancho, avanzando de minuto en minuto.
    # Deslizante y no fija porque una coincidencia repartida entre dos ventanas
    # fijas contiguas no se veria, y es justo lo que se quiere detectar.
    # Se excluyen los bots: dos bots tocando la misma pagina no es colaboracion.
    p3 = (
        plata.filter(~F.col("es_bot"))
        .filter(F.col("tipo").isin("edit", "new"))
        .groupBy(
            F.window("ts_evento", "5 minutes", "1 minute"),
            F.col("wiki"),
            F.col("titulo"),
        )
        .agg(
            F.approx_count_distinct("usuario").alias("editores"),
            F.count("*").alias("ediciones"),
        )
        .filter(F.col("editores") >= MIN_EDITORES)
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "wiki",
            "titulo",
            "editores",
            "ediciones",
        )
    )

    consultas = [
        _lanzar(p1, "%s.oro.actividad_por_wiki" % CATALOGO, "oro_p1", args.intervalo),
        _lanzar(p2, "%s.oro.humano_vs_bot" % CATALOGO, "oro_p2", args.intervalo),
        _lanzar(
            p3, "%s.oro.paginas_concurrentes" % CATALOGO, "oro_p3", args.intervalo
        ),
    ]

    log.info("Gold en marcha: %d consultas, watermark %s", len(consultas), args.watermark)
    try:
        if args.duracion:
            # OJO: en PySpark el plazo va en SEGUNDOS. En la API de Scala va
            # en milisegundos, y multiplicar por 1000 aqui hace que el job
            # espere casi tres dias con la CPU al 1%, que parece un bloqueo
            # pero es obediencia.
            # Devuelve al agotarse el plazo o si alguna consulta muere. Si una
            # falla se para todo: seguir con dos de tres daria tablas
            # incoherentes entre si.
            spark.streams.awaitAnyTermination(args.duracion)
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        pass
    finally:
        for c in consultas:
            if c.isActive:
                c.stop()
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
