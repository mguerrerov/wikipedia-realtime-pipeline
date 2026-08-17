"""Comprueba que Bronze no pierde ni duplica.

El criterio de la fase 2 es que el job sobreviva a un reinicio sin perder ni
duplicar datos, y poder demostrarlo. La demostracion se apoya en que
`(particion, desplazamiento)` identifica de forma unica cada mensaje de la
cola, y en que los desplazamientos de una particion son consecutivos:

  duplicados = filas totales - combinaciones distintas
  huecos     = (maximo - minimo + 1) - filas de esa particion

Ambos numeros tienen que ser cero. Si hay duplicados, el reinicio reproceso
algo ya escrito. Si hay huecos, se perdio.

Uso:
    spark-submit /app/src/jobs/verifica_bronce.py
"""

import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/app")

from src import almacenamiento  # noqa: E402

TABLA = "%s.bronce.cambios" % almacenamiento.NOMBRE_CATALOGO


def main() -> int:
    constructor = SparkSession.builder.appName("verifica-bronce")
    for clave, valor in almacenamiento.configuracion().items():
        constructor = constructor.config(clave, valor)
    spark = constructor.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    total, distintos = spark.sql(
        """
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT particion, desplazamiento) AS distintos
        FROM %s
        """
        % TABLA
    ).first()

    print("")
    print("== INTEGRIDAD DE BRONZE ==")
    print("Filas totales              : %d" % total)
    print("Pares (particion, offset)  : %d" % distintos)
    print("DUPLICADOS                 : %d" % (total - distintos))

    print("")
    print("== CONTINUIDAD POR PARTICION ==")
    filas = spark.sql(
        """
        SELECT particion,
               COUNT(*)              AS filas,
               MIN(desplazamiento)   AS primero,
               MAX(desplazamiento)   AS ultimo,
               (MAX(desplazamiento) - MIN(desplazamiento) + 1) - COUNT(*) AS huecos
        FROM %s
        GROUP BY particion
        ORDER BY particion
        """
        % TABLA
    ).collect()

    huecos_total = 0
    print("  part   filas   primero    ultimo   huecos")
    for f in filas:
        huecos_total += f["huecos"]
        print(
            "  %4d  %6d  %8d  %8d  %7d"
            % (f["particion"], f["filas"], f["primero"], f["ultimo"], f["huecos"])
        )
    print("HUECOS TOTALES             : %d" % huecos_total)

    print("")
    print("== INSTANTANEAS DE LA TABLA ==")
    # Cada arranque del job deja su rastro: una instantanea por micro-lote que
    # escribio algo. Sirve para ver que hubo dos ejecuciones distintas.
    spark.sql(
        "SELECT committed_at, snapshot_id, operation FROM %s.snapshots "
        "ORDER BY committed_at" % TABLA
    ).show(50, truncate=False)

    ok = (total - distintos) == 0 and huecos_total == 0
    print("VEREDICTO: %s" % ("CORRECTO, sin perdidas ni duplicados" if ok else "FALLO"))

    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
