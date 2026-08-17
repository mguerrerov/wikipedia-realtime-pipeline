"""Interfaz de la fuente de eventos.

Esta es la frontera del proyecto. Todo lo que cambia entre local (Redpanda) y
AWS (Kinesis) vive detras de estas dos clases. Nada de lo que hay al otro lado
-el productor, los jobs de Spark, las agregaciones- puede saber en que entorno
se esta ejecutando.

Si en algun momento hace falta un `if entorno == "aws"` fuera de este paquete,
la abstraccion esta mal y hay que arreglarla aqui, no alli.
"""

import abc
from typing import Dict, Tuple


class Publicador(abc.ABC):
    """Escribe eventos en la cola. Lo usa el productor."""

    @abc.abstractmethod
    def publicar(self, clave: str, valor: bytes) -> None:
        """Encola un evento. Puede ser asincrono: no garantiza entrega al volver.

        `clave` determina el reparto entre particiones y, con ello, que eventos
        conservan su orden relativo. `valor` es el evento serializado.
        """

    @abc.abstractmethod
    def vaciar(self, timeout_s: float = 10.0) -> int:
        """Fuerza la entrega de lo pendiente. Devuelve cuantos quedaron sin enviar."""

    @abc.abstractmethod
    def cerrar(self) -> None:
        """Cierra la conexion. Debe vaciar antes de cerrar."""

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.cerrar()
        return False


class LecturaSpark(abc.ABC):
    """Devuelve como lee Spark de esta cola, y normaliza lo que sale.

    Dar solo formato y opciones NO basta, y esto se descubrio verificando la
    documentacion de EMR antes de escribir el Terraform. Cada fuente devuelve un
    DataFrame con columnas distintas:

        Kafka    key, value, topic, partition, offset, timestamp
        Kinesis  data, streamName, partitionKey, sequenceNumber,
                 approximateArrivalTimestamp

    Un job que haga `F.col("offset")` funciona en local y no arranca en AWS.
    No hay ningun `if entorno == "aws"`, pero la diferencia de entorno se ha
    filtrado igual, columna a columna. Por eso `normalizar` es parte de la
    interfaz: traduce cada sobre al esquema comun y el job no conoce ninguno.
    """

    #: Esquema comun al que traducen todas las implementaciones.
    COLUMNAS = ("clave", "valor", "origen", "particion", "desplazamiento", "ts_cola")

    @abc.abstractmethod
    def formato_y_opciones(self) -> Tuple[str, Dict[str, str]]:
        """Devuelve (formato, opciones) para `spark.readStream.format(...)`."""

    @abc.abstractmethod
    def normalizar(self, df):
        """Traduce el sobre propio de la fuente al esquema comun COLUMNAS.

        `desplazamiento` es texto y no numero a proposito: en Kafka es un entero
        creciente, pero en Kinesis es un numero de secuencia de 56 digitos que
        no cabe en un BIGINT. Unificar por el lado ancho evita que el esquema
        de la tabla dependa del entorno.
        """

    @abc.abstractmethod
    def paquetes_maven(self) -> str:
        """Coordenadas Maven del conector, para `spark.jars.packages`."""

    @abc.abstractmethod
    def desplazamiento_es_consecutivo(self) -> bool:
        """Si los desplazamientos de una particion son enteros consecutivos.

        Cierto en Kafka, falso en Kinesis. Lo consulta la verificacion de
        Bronze: el conteo de huecos por resta solo tiene sentido si lo son.
        """
