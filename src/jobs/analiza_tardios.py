"""Compara Bronze con Silver por tramos de retraso.

Bronze tiene todo lo que llego, incluidos duplicados y rezagados. Silver tiene
lo que sobrevivio a la deduplicacion y al watermark. Comparar ambos por tramos
de retraso ensena donde esta el corte real, en vez de suponerlo.

El retraso de un evento es `ts_cola - meta.dt`: cuanto tardo en llegar a la cola
desde que se publico. Los eventos sinteticos se distinguen por su `comment`.

Uso:
    spark-submit /app/src/jobs/analiza_tardios.py
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, "/app")

from src import almacenamiento  # noqa: E402

# Que tanda se analiza. El generador la escribe en `comment`.
ETIQUETA = os.environ.get("ETIQUETA", "evento sintetico")

CAT = almacenamiento.NOMBRE_CATALOGO
BRONCE = "%s.bronce.cambios" % CAT
PLATA = "%s.plata.cambios" % CAT

ESQUEMA = T.StructType(
    [
        T.StructField(
            "meta",
            T.StructType(
                [T.StructField("id", T.StringType()), T.StructField("dt", T.StringType())]
            ),
        ),
        T.StructField("comment", T.StringType()),
    ]
)

# Tramos elegidos alrededor del watermark de 30 s, para ver el corte.
TRAMOS = "CASE WHEN retraso_s < 5 THEN 'a) <5s' " \
         "WHEN retraso_s < 15 THEN 'b) 5-15s' " \
         "WHEN retraso_s < 30 THEN 'c) 15-30s' " \
         "WHEN retraso_s < 45 THEN 'd) 30-45s' " \
         "WHEN retraso_s < 60 THEN 'e) 45-60s' " \
         "ELSE 'f) >60s' END"


def main() -> int:
    constructor = SparkSession.builder.appName("analiza-tardios")
    for clave, valor in almacenamiento.configuracion().items():
        constructor = constructor.config(clave, valor)
    spark = constructor.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bronce = (
        spark.table(BRONCE)
        .select(F.from_json(F.col("valor"), ESQUEMA).alias("e"), F.col("ts_cola"))
        .filter(F.col("e.comment").startswith(ETIQUETA))
        .select(
            F.col("e.meta.id").alias("meta_id"),
            F.to_timestamp(F.col("e.meta.dt"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias(
                "ts_evento"
            ),
            F.col("ts_cola"),
        )
        .withColumn(
            "retraso_s",
            F.col("ts_cola").cast("double") - F.col("ts_evento").cast("double"),
        )
    )
    bronce.createOrReplaceTempView("bronce_sint")

    total_filas = bronce.count()
    distintos = bronce.select("meta_id").distinct().count()

    print("")
    print("== DUPLICADOS QUE INTRODUJO EL GENERADOR ==")
    print("Filas sinteticas en Bronze  : %d" % total_filas)
    print("meta_id distintos           : %d" % distintos)
    print("Duplicados inyectados       : %d" % (total_filas - distintos))

    # La comparacion se hace uniendo identificadores, no trayendo listas al
    # driver: la tabla puede tener millones de filas.
    spark.table(PLATA).select("meta_id").distinct().createOrReplaceTempView("plata_ids")

    print("")
    print("== SUPERVIVENCIA POR TRAMO DE RETRASO ==")
    print("Bronze cuenta identificadores distintos; Silver, los que llegaron.")
    spark.sql(
        """
        SELECT tramo,
               COUNT(*)                              AS en_bronce,
               SUM(CASE WHEN p.meta_id IS NOT NULL THEN 1 ELSE 0 END) AS en_plata,
               ROUND(100.0 * SUM(CASE WHEN p.meta_id IS NOT NULL THEN 1 ELSE 0 END)
                     / COUNT(*), 1)                  AS pct_superviviente
        FROM (
            SELECT DISTINCT meta_id, %s AS tramo
            FROM bronce_sint
        ) b
        LEFT JOIN plata_ids p USING (meta_id)
        GROUP BY tramo
        ORDER BY tramo
        """
        % TRAMOS
    ).show(truncate=False)

    print("== DEDUPLICACION ==")
    dedup = spark.sql(
        """
        SELECT COUNT(*) AS filas, COUNT(DISTINCT meta_id) AS distintos
        FROM %s
        """
        % PLATA
    ).first()
    print("Filas en Silver             : %d" % dedup["filas"])
    print("meta_id distintos en Silver : %d" % dedup["distintos"])
    print("Duplicados en Silver        : %d" % (dedup["filas"] - dedup["distintos"]))

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
