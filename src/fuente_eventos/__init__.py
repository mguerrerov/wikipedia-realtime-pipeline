"""Seleccion de la implementacion de la fuente de eventos.

Un unico punto en todo el proyecto lee la variable de entorno que decide el
entorno. Ese punto es este fichero. En ningun otro sitio debe aparecer.

    FUENTE_EVENTOS=kafka     -> Redpanda en local (por defecto)
    FUENTE_EVENTOS=kinesis   -> Kinesis Data Streams en AWS

Publicacion y lectura se piden por separado a proposito. Viven en imagenes
distintas -el productor es Python con confluent_kafka, Spark no lo lleva- y
pedirlas juntas obligaria a cada imagen a cargar con la dependencia de la otra.
"""

import os
from typing import Tuple

from .base import LecturaSpark, Publicador

VARIABLE = "FUENTE_EVENTOS"
POR_DEFECTO = "kafka"
ADMITIDOS = ("kafka", "kinesis")


def _elegida(nombre: str = None) -> str:
    nombre = (nombre or os.environ.get(VARIABLE, POR_DEFECTO)).strip().lower()
    if nombre not in ADMITIDOS:
        raise ValueError(
            "%s=%r no es valido. Valores admitidos: %s."
            % (VARIABLE, nombre, ", ".join(repr(v) for v in ADMITIDOS))
        )
    return nombre


def _modulo(nombre: str):
    """Importa dentro de la rama elegida: asi usar Kafka no exige tener boto3."""
    if nombre == "kafka":
        from . import kafka_redpanda as modulo
    else:
        from . import kinesis as modulo
    return modulo


def crear_publicador(nombre: str = None) -> Publicador:
    """Para el productor. Necesita el cliente de la cola instalado."""
    return _modulo(_elegida(nombre)).publicador_desde_entorno()


def crear_lectura(nombre: str = None) -> LecturaSpark:
    """Para los jobs de Spark. No necesita ningun cliente de Python."""
    return _modulo(_elegida(nombre)).lectura_desde_entorno()


def crear(nombre: str = None) -> Tuple[Publicador, LecturaSpark]:
    """Ambas a la vez. Solo sirve donde estan las dos dependencias."""
    elegida = _elegida(nombre)
    modulo = _modulo(elegida)
    return modulo.publicador_desde_entorno(), modulo.lectura_desde_entorno()


__all__ = [
    "crear",
    "crear_publicador",
    "crear_lectura",
    "Publicador",
    "LecturaSpark",
    "VARIABLE",
    "POR_DEFECTO",
]
