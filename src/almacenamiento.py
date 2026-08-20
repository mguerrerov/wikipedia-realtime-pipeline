"""Configuracion del catalogo Iceberg y del acceso a objetos.

Segundo y ultimo eje de diferencia entre entornos, junto con `fuente_eventos`.
Aqui vive todo lo que cambia entre MinIO+catalogo de ficheros y S3+Glue, y solo
aqui. Los jobs piden la configuracion y escriben; no saben donde acaban los
datos.

    CATALOGO=hadoop  -> MinIO en local, catalogo de ficheros (por defecto)
    CATALOGO=glue    -> S3 y Glue Data Catalog en AWS

La ruta de la tabla es la misma cadena en los dos casos porque MinIO habla S3.
Esa es la razon de haber elegido este stack.
"""

import os
from typing import Dict

VARIABLE = "CATALOGO"
POR_DEFECTO = "hadoop"

# Nombre del catalogo dentro de Spark. Aparece en todas las consultas como
# prefijo (pipeline.bronce.cambios), asi que no cambia entre entornos.
NOMBRE_CATALOGO = "pipeline"

CLASE_CATALOGO = "org.apache.iceberg.spark.SparkCatalog"
EXTENSIONES = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def _config_hadoop() -> Dict[str, str]:
    """MinIO. El endpoint y las credenciales son lo unico especifico."""
    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    almacen = os.environ.get("ALMACEN", "s3a://almacen/warehouse")
    clave = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secreto = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    p = "spark.sql.catalog." + NOMBRE_CATALOGO
    return {
        p: CLASE_CATALOGO,
        # Catalogo de ficheros: los metadatos viven junto a los datos, sin
        # servicio aparte. Suficiente en local y una pieza menos que levantar.
        p + ".type": "hadoop",
        p + ".warehouse": almacen,
        # MinIO no resuelve subdominios por bucket, asi que hay que pedirle a
        # S3A rutas del tipo endpoint/bucket/clave en vez de bucket.endpoint.
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.access.key": clave,
        "spark.hadoop.fs.s3a.secret.key": secreto,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # En local MinIO va por HTTP.
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }


def _config_glue() -> Dict[str, str]:
    """S3 y Glue. Las credenciales las pone el rol de ejecucion de EMR."""
    almacen = os.environ.get("ALMACEN", "s3://cambiar-por-el-bucket/warehouse")

    p = "spark.sql.catalog." + NOMBRE_CATALOGO
    return {
        p: CLASE_CATALOGO,
        p + ".catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        p + ".warehouse": almacen,
        p + ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }


def configuracion(nombre: str = None) -> Dict[str, str]:
    """Devuelve la configuracion de Spark del entorno elegido."""
    nombre = (nombre or os.environ.get(VARIABLE, POR_DEFECTO)).strip().lower()

    if nombre == "hadoop":
        especifica = _config_hadoop()
    elif nombre == "glue":
        especifica = _config_glue()
    else:
        raise ValueError(
            "%s=%r no es valido. Valores admitidos: 'hadoop', 'glue'."
            % (VARIABLE, nombre)
        )

    comun = {
        "spark.sql.extensions": EXTENSIONES,
        # Que el catalogo del proyecto sea el de por defecto evita tener que
        # escribir el prefijo en cada consulta suelta.
        "spark.sql.defaultCatalog": NOMBRE_CATALOGO,
    }
    comun.update(especifica)
    return comun


def ruta_checkpoint(nombre_job: str) -> str:
    """Checkpoint dentro del mismo almacen, no en disco local.

    Va aqui y no en el job porque es una diferencia de entorno: en local es
    MinIO y en AWS es S3. Si estuviera en disco del contenedor, un reinicio en
    otra maquina perderia el punto de control y el job reprocesaria todo.
    """
    base = os.environ.get("CHECKPOINTS")
    if not base:
        almacen = configuracion().get(
            "spark.sql.catalog." + NOMBRE_CATALOGO + ".warehouse", ""
        )
        base = almacen.rstrip("/") + "/_checkpoints"
    return "%s/%s" % (base.rstrip("/"), nombre_job)


def configuracion_duckdb(nombre: str = None) -> Dict[str, str]:
    """Datos de conexion para DuckDB, el motor de consumo de la fase 4.

    DuckDB no usa el catalogo de Iceberg: abre la tabla por su ruta y sigue el
    `version-hint.text` que deja el catalogo de ficheros. Por eso solo necesita
    saber donde esta el almacen y como hablar con el servicio de objetos.

    Va aqui, y no en el script de consumo, por la misma razon que el resto del
    modulo: es lo unico que cambia entre entornos.
    """
    nombre = (nombre or os.environ.get(VARIABLE, POR_DEFECTO)).strip().lower()
    if nombre != "hadoop":
        raise ValueError(
            "DuckDB solo se usa en local sobre MinIO. En AWS el consumo es "
            "Athena sobre el catalogo de Glue, no este script."
        )

    almacen = os.environ.get("ALMACEN", "s3a://almacen/warehouse")
    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")

    return {
        # DuckDB habla s3://; el resto del proyecto escribe s3a:// porque esa
        # es la ruta que entiende el conector de Hadoop. Es el mismo sitio.
        "almacen": almacen.replace("s3a://", "s3://", 1).rstrip("/"),
        # El secreto de DuckDB quiere host:puerto, sin esquema.
        "endpoint": endpoint.split("://", 1)[-1],
        "usar_ssl": "true" if endpoint.startswith("https://") else "false",
        "clave": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "secreto": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    }
