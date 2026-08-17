"""Seleccion de la implementacion de la fuente de eventos.

Un unico punto en todo el proyecto lee la variable de entorno que decide el
entorno. Ese punto es este fichero. En ningun otro sitio debe aparecer.

    FUENTE_EVENTOS=kafka     -> Redpanda en local (por defecto)
    FUENTE_EVENTOS=kinesis   -> Kinesis Data Streams en AWS
"""

import os
from typing import Tuple

from .base import LecturaSpark, Publicador

VARIABLE = "FUENTE_EVENTOS"
POR_DEFECTO = "kafka"


def crear(nombre: str = None) -> Tuple[Publicador, LecturaSpark]:
    """Devuelve el par (publicador, lectura) de la implementacion elegida.

    Se importa dentro de cada rama a proposito: asi usar Kafka en local no
    obliga a tener boto3 instalado, ni al reves.
    """
    nombre = (nombre or os.environ.get(VARIABLE, POR_DEFECTO)).strip().lower()

    if nombre == "kafka":
        from .kafka_redpanda import desde_entorno

        return desde_entorno()

    if nombre == "kinesis":
        from .kinesis import desde_entorno

        return desde_entorno()

    raise ValueError(
        "%s=%r no es valido. Valores admitidos: 'kafka', 'kinesis'."
        % (VARIABLE, nombre)
    )


__all__ = ["crear", "Publicador", "LecturaSpark", "VARIABLE"]
