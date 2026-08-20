"""Consumo de la fase 4: las mismas tablas Iceberg, leidas por otro motor.

`src/jobs/consultas.py` responde estas mismas preguntas desde Spark, que es
quien escribio las tablas. Este script las responde desde DuckDB, que no ha
participado en la escritura y no conoce el catalogo: abre la tabla por su ruta
y sigue el `version-hint.text` del catalogo de ficheros.

Ese es el argumento de la fase: en Iceberg los datos no son de quien los
escribio. Si los dos motores dan el mismo numero, la tabla es el contrato.

Uso:
    docker compose run --rm consumo
"""

import sys
from typing import List, Sequence

import duckdb

sys.path.insert(0, "/app")

from src import almacenamiento  # noqa: E402

ANCHO = 78


def titulo(texto: str) -> None:
    print("")
    print("=" * ANCHO)
    print(texto)
    print("=" * ANCHO)


def imprime(cabeceras: Sequence[str], filas: List[Sequence]) -> None:
    """Tabla de texto plano. Sin dependencias: se ve igual en cualquier consola."""
    if not filas:
        print("  (sin filas)")
        return
    celdas = [[("" if v is None else str(v)) for v in fila] for fila in filas]
    anchos = [
        max(len(cab), max(len(fila[i]) for fila in celdas))
        for i, cab in enumerate(cabeceras)
    ]
    linea = "  ".join(c.ljust(a) for c, a in zip(cabeceras, anchos))
    print("  " + linea)
    print("  " + "-" * len(linea))
    for fila in celdas:
        print("  " + "  ".join(v.ljust(a) for v, a in zip(fila, anchos)))


def conecta() -> "duckdb.DuckDBPyConnection":
    """Sesion de DuckDB con acceso al almacen de objetos.

    `httpfs` da el cliente S3 e `iceberg` sabe leer los metadatos de la tabla.
    Las dos vienen de la distribucion oficial y se instalan en la imagen, no en
    cada ejecucion: ver Dockerfile.duckdb.
    """
    cfg = almacenamiento.configuracion_duckdb()
    con = duckdb.connect()
    con.execute("LOAD httpfs; LOAD iceberg;")
    con.execute(
        """
        CREATE OR REPLACE SECRET almacen (
            TYPE s3,
            KEY_ID '%(clave)s',
            SECRET '%(secreto)s',
            ENDPOINT '%(endpoint)s',
            URL_STYLE 'path',
            USE_SSL %(usar_ssl)s,
            REGION 'us-east-1'
        )
        """
        % cfg
    )
    return con


def consulta(con, sql: str) -> None:
    """Ejecuta e imprime, o explica por que no ha podido.

    Las marcas de tiempo se formatean con `strftime` dentro del SQL. No es
    cosmetico: DuckDB exige `pytz` para devolver a Python una columna
    TIMESTAMP WITH TIME ZONE, y no merece la pena arrastrar una dependencia
    solo para imprimir una fecha.
    """
    try:
        rel = con.sql(sql)
        imprime(rel.columns, rel.fetchall())
    except Exception as e:  # noqa: BLE001
        print("  No se ha podido consultar: %s" % e)


def main() -> int:
    cfg = almacenamiento.configuracion_duckdb()
    base = cfg["almacen"]

    def t(nombre: str) -> str:
        """De 'plata.cambios' a la ruta de la tabla, entre comillas para SQL."""
        return "'%s/%s'" % (base, nombre.replace(".", "/"))

    con = conecta()
    print("Motor de consulta: DuckDB %s" % duckdb.__version__)
    print("Almacen:           %s" % base)
    print("Catalogo:          ninguno. Las tablas se abren por ruta.")

    titulo("VOLUMEN DE CADA CAPA")
    tablas = [
        "bronce.cambios",
        "plata.cambios",
        "oro.actividad_por_wiki",
        "oro.humano_vs_bot",
        "oro.paginas_concurrentes",
    ]
    filas = []
    existentes = set()
    for nombre in tablas:
        try:
            n = con.sql("SELECT count(*) FROM iceberg_scan(%s)" % t(nombre)).fetchone()[0]
            existentes.add(nombre)
            filas.append([nombre, "%d" % n])
        except Exception:  # noqa: BLE001
            # Falta legitimamente si aun no se ha lanzado el job que la escribe.
            filas.append([nombre, "(sin crear todavia)"])
    imprime(["tabla", "filas"], filas)

    if not {"oro.actividad_por_wiki", "oro.humano_vs_bot"} <= existentes:
        print("")
        print("Las tablas de Gold no existen: lanza antes `docker compose run --rm oro`.")
        return 0

    titulo("HISTORIA DE LA TABLA DE SILVER (instantaneas de Iceberg)")
    # Cada micro-lote de Spark deja una instantanea. Verlas desde DuckDB
    # demuestra que los metadatos de Iceberg tambien son portables, no solo
    # los datos.
    consulta(
        con,
        """
        SELECT sequence_number, timestamp_ms AS momento, manifest_list
        FROM iceberg_snapshots(%s)
        ORDER BY sequence_number DESC
        LIMIT 8
        """
        % t("plata.cambios"),
    )

    titulo("LATENCIA EXTREMO A EXTREMO (segundos)")
    # Desde meta.dt -cuando Wikimedia publico el evento- hasta que la fila
    # queda escrita en Silver. Solo el ultimo cuarto de hora: las filas mas
    # antiguas se escribieron poniendose al dia con el historico de Bronze, y
    # ese tramo mide el recuperado, no el pipeline en marcha.
    consulta(
        con,
        """
        WITH s AS (SELECT * FROM iceberg_scan(%s)),
        reciente AS (
            SELECT epoch(ingerido_en) - epoch(ts_evento) AS lat
            FROM s
            WHERE ts_evento > (SELECT max(ts_evento) - INTERVAL 15 MINUTE FROM s)
              AND ingerido_en > ts_evento
        )
        SELECT count(*)                                  AS filas,
               round(min(lat), 2)                        AS minimo,
               round(quantile_cont(lat, 0.50), 2)        AS p50,
               round(quantile_cont(lat, 0.95), 2)        AS p95,
               round(quantile_cont(lat, 0.99), 2)        AS p99,
               round(max(lat), 2)                        AS maximo
        FROM reciente
        """
        % t("plata.cambios"),
    )

    titulo("P1 - ACTIVIDAD POR WIKI Y ESPACIO, ultimo minuto completo")
    consulta(
        con,
        """
        WITH a AS (SELECT * FROM iceberg_scan(%s))
        SELECT servidor, espacio, eventos
        FROM a
        WHERE ventana_inicio = (SELECT max(ventana_inicio) FROM a)
        ORDER BY eventos DESC
        LIMIT 12
        """
        % t("oro.actividad_por_wiki"),
    )

    titulo("P2 - HUMANO FRENTE A BOT, por minuto")
    consulta(
        con,
        """
        SELECT strftime(ventana_inicio, '%%Y-%%m-%%d %%H:%%M') AS ventana,
               sum(CASE WHEN es_bot THEN eventos ELSE 0 END) AS bots,
               sum(CASE WHEN es_bot THEN 0 ELSE eventos END) AS humanos,
               round(100.0 * sum(CASE WHEN es_bot THEN eventos ELSE 0 END)
                     / sum(eventos), 1)                      AS pct_bot
        FROM iceberg_scan(%s)
        GROUP BY ventana_inicio
        ORDER BY ventana_inicio DESC
        LIMIT 12
        """
        % t("oro.humano_vs_bot"),
    )

    titulo("P2 - cuanto oscila esa proporcion entre ventanas")
    consulta(
        con,
        """
        WITH por_ventana AS (
            SELECT ventana_inicio,
                   100.0 * sum(CASE WHEN es_bot THEN eventos ELSE 0 END)
                   / sum(eventos) AS pct_bot
            FROM iceberg_scan(%s)
            GROUP BY ventana_inicio
        )
        SELECT count(*)                    AS ventanas,
               round(min(pct_bot), 1)      AS min_pct_bot,
               round(avg(pct_bot), 1)      AS media_pct_bot,
               round(max(pct_bot), 1)      AS max_pct_bot,
               round(stddev_samp(pct_bot), 1) AS desviacion
        FROM por_ventana
        """
        % t("oro.humano_vs_bot"),
    )

    titulo("P3 - PAGINAS EDITADAS POR VARIAS PERSONAS A LA VEZ")
    consulta(
        con,
        """
        SELECT strftime(ventana_inicio, '%%Y-%%m-%%d %%H:%%M') AS ventana,
               wiki, titulo, editores, ediciones
        FROM iceberg_scan(%s)
        ORDER BY editores DESC, ediciones DESC
        LIMIT 15
        """
        % t("oro.paginas_concurrentes"),
    )

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
