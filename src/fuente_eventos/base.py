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
    """Devuelve como lee Spark de esta cola.

    Existe para que los jobs de la fase 2 en adelante no tengan que saber si
    detras hay Kafka o Kinesis: piden formato y opciones, y leen.
    """

    @abc.abstractmethod
    def formato_y_opciones(self) -> Tuple[str, Dict[str, str]]:
        """Devuelve (formato, opciones) para `spark.readStream.format(...)`."""

    @abc.abstractmethod
    def paquetes_maven(self) -> str:
        """Coordenadas Maven del conector, para `spark.jars.packages`."""
