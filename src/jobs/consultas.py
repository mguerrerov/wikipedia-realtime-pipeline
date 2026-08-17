"""Consulta las tablas de Gold: las tres preguntas, respondidas.

Lectura en lote sobre las tablas Iceberg que escribe el job de streaming.

Uso:
    spark-submit /app/src/jobs/consultas.py
"""

import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/app")

from src import almacenamiento  # noqa: E402

CAT = almacenamiento.NOMBRE_CATALOGO


def titulo(texto: str) -> None:
    print("")
    print("=" * 72)
    print(texto)
    print("=" * 72)


def main() -> int:
    constructor = SparkSession.builder.appName("consultas")
    for clave, valor in almacenamiento.configuracion().items():
        constructor = constructor.config(clave, valor)
    spark = constructor.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def cuenta(nombre: str) -> int:
        """Cuenta filas, o devuelve -1 si la tabla aun no existe.

        Una tabla puede faltar legitimamente: si todavia no se ha lanzado el
        job que la escribe. El script informa y sigue, en vez de reventar.
        """
        try:
            return spark.table("%s.%s" % (CAT, nombre)).count()
        except Exception:
            return -1

    titulo("VOLUMEN DE CADA CAPA")
    tablas = [
        "bronce.cambios",
        "plata.cambios",
        "oro.actividad_por_wiki",
        "oro.humano_vs_bot",
        "oro.paginas_concurrentes",
    ]
    existentes = set()
    for nombre in tablas:
        n = cuenta(nombre)
        if n < 0:
            print("  %-28s  (sin crear todavia)" % nombre)
        else:
            existentes.add(nombre)
            print("  %-28s %8d filas" % (nombre, n))

    if not {"oro.actividad_por_wiki", "oro.humano_vs_bot"} <= existentes:
        print("")
        print("Las tablas de Gold no existen: lanza antes `docker compose run --rm oro`.")
        spark.stop()
        return 0

    titulo("LATENCIA EXTREMO A EXTREMO (segundos)")
    # Desde meta.dt -cuando Wikimedia publico el evento- hasta que la fila
    # queda escrita en Silver. Se limita al ultimo cuarto de hora de datos
    # porque las filas mas antiguas se escribieron poniendose al dia con el
    # historico de Bronze, y ese tramo mide el rendimiento del recuperado, no
    # el del pipeline en marcha.
    spark.sql(
        """
        WITH reciente AS (
            SELECT (CAST(ingerido_en AS DOUBLE) - CAST(ts_evento AS DOUBLE)) AS lat
            FROM %s.plata.cambios
            WHERE ts_evento > (SELECT MAX(ts_evento) - INTERVAL 15 MINUTES
                               FROM %s.plata.cambios)
              AND ingerido_en > ts_evento
        )
        SELECT COUNT(*)                                       AS filas,
               ROUND(MIN(lat), 2)                             AS minimo,
               ROUND(PERCENTILE_APPROX(lat, 0.50), 2)         AS p50,
               ROUND(PERCENTILE_APPROX(lat, 0.95), 2)         AS p95,
               ROUND(PERCENTILE_APPROX(lat, 0.99), 2)         AS p99,
               ROUND(MAX(lat), 2)                             AS maximo
        FROM reciente
        """
        % (CAT, CAT)
    ).show(truncate=False)

    titulo("P1 - ACTIVIDAD POR WIKI Y ESPACIO, ultimo minuto completo")
    spark.sql(
        """
        WITH ultima AS (
            SELECT MAX(ventana_inicio) AS v FROM %s.oro.actividad_por_wiki
        )
        SELECT servidor, espacio, eventos
        FROM %s.oro.actividad_por_wiki, ultima
        WHERE ventana_inicio = ultima.v
        ORDER BY eventos DESC
        LIMIT 12
        """
        % (CAT, CAT)
    ).show(truncate=False)

    titulo("P2 - HUMANO FRENTE A BOT, por minuto")
    spark.sql(
        """
        SELECT ventana_inicio,
               SUM(CASE WHEN es_bot THEN eventos ELSE 0 END)  AS bots,
               SUM(CASE WHEN es_bot THEN 0 ELSE eventos END)  AS humanos,
               ROUND(100.0 * SUM(CASE WHEN es_bot THEN eventos ELSE 0 END)
                     / SUM(eventos), 1)                       AS pct_bot
        FROM %s.oro.humano_vs_bot
        GROUP BY ventana_inicio
        ORDER BY ventana_inicio DESC
        LIMIT 12
        """
        % CAT
    ).show(truncate=False)

    titulo("P2 - cuanto oscila esa proporcion entre ventanas")
    spark.sql(
        """
        WITH por_ventana AS (
            SELECT ventana_inicio,
                   100.0 * SUM(CASE WHEN es_bot THEN eventos ELSE 0 END)
                   / SUM(eventos) AS pct_bot
            FROM %s.oro.humano_vs_bot
            GROUP BY ventana_inicio
        )
        SELECT COUNT(*)                        AS ventanas,
               ROUND(MIN(pct_bot), 1)          AS min_pct_bot,
               ROUND(AVG(pct_bot), 1)          AS media_pct_bot,
               ROUND(MAX(pct_bot), 1)          AS max_pct_bot,
               ROUND(STDDEV(pct_bot), 1)       AS desviacion
        FROM por_ventana
        """
        % CAT
    ).show(truncate=False)

    titulo("P3 - PAGINAS EDITADAS POR VARIAS PERSONAS A LA VEZ")
    spark.sql(
        """
        SELECT ventana_inicio, wiki, titulo, editores, ediciones
        FROM %s.oro.paginas_concurrentes
        ORDER BY editores DESC, ediciones DESC
        LIMIT 15
        """
        % CAT
    ).show(truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
