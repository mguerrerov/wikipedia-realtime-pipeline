"""Implementacion sobre Kinesis Data Streams.

Se escribe en la fase 1 a proposito, aunque no se pruebe contra AWS hasta la
fase 6. Escribirla ahora es lo que obliga a que la interfaz sea honesta: si solo
existiera la de Kafka, la interfaz acabaria siendo Kafka con otro nombre.

Diferencias reales con Kafka, que son justo lo que esta clase absorbe:

- Kinesis no tiene productor asincrono con lotes como librdkafka. Se acumula en
  memoria y se envia con `put_records`, que admite 500 registros o 5 MB por
  llamada.
- `put_records` puede fallar parcialmente: devuelve 200 con registros
  rechazados dentro. Hay que mirar `FailedRecordCount`, no el codigo HTTP.
- La clave de particion es una cadena que Kinesis convierte a un hash de 128
  bits. El reparto no coincide con el de Kafka, pero la garantia que nos
  importa -mismo valor de clave, misma particion, orden conservado- si.
"""

import logging
import os
import time
from typing import Dict, List, Tuple

from .base import LecturaSpark, Publicador

log = logging.getLogger(__name__)

MAX_REGISTROS_POR_LLAMADA = 500
MAX_BYTES_POR_LLAMADA = 4_500_000  # el limite real es 5 MB; dejamos margen


class PublicadorKinesis(Publicador):
    def __init__(self, stream: str, region: str, lote: int = 250):
        # Igual que en la implementacion de Kafka: el import va dentro para que
        # pedir solo la lectura no obligue a tener boto3 instalado.
        import boto3
        from botocore.config import Config

        self.stream = stream
        self.lote = min(lote, MAX_REGISTROS_POR_LLAMADA)
        self._pendientes: List[dict] = []
        self._bytes_pendientes = 0
        self._sin_entregar = 0
        self._cliente = boto3.client(
            "kinesis",
            region_name=region,
            config=Config(retries={"max_attempts": 5, "mode": "adaptive"}),
        )

    def publicar(self, clave: str, valor: bytes) -> None:
        self._pendientes.append({"Data": valor, "PartitionKey": clave})
        self._bytes_pendientes += len(valor) + len(clave)
        if (
            len(self._pendientes) >= self.lote
            or self._bytes_pendientes >= MAX_BYTES_POR_LLAMADA
        ):
            self._enviar_lote()

    def _enviar_lote(self, reintentos: int = 4) -> None:
        """Envia el lote pendiente reintentando solo los registros rechazados."""
        registros = self._pendientes
        self._pendientes = []
        self._bytes_pendientes = 0

        espera = 0.2
        for intento in range(reintentos):
            if not registros:
                return
            respuesta = self._cliente.put_records(
                StreamName=self.stream, Records=registros
            )
            fallidos = respuesta.get("FailedRecordCount", 0)
            if not fallidos:
                return
            # Reintentamos solo los que fallaron, no el lote entero: reenviar
            # los que si entraron los duplicaria.
            resultados = respuesta["Records"]
            registros = [
                r for r, res in zip(registros, resultados) if res.get("ErrorCode")
            ]
            log.warning(
                "put_records: %d rechazados (intento %d), reintentando",
                fallidos,
                intento + 1,
            )
            time.sleep(espera)
            espera *= 2

        if registros:
            self._sin_entregar += len(registros)
            log.error("%d registros descartados tras agotar reintentos", len(registros))

    def vaciar(self, timeout_s: float = 10.0) -> int:
        if self._pendientes:
            self._enviar_lote()
        return self._sin_entregar

    def cerrar(self) -> None:
        self.vaciar()
        if self._sin_entregar:
            log.warning("%d eventos no llegaron a Kinesis", self._sin_entregar)


class LecturaKinesis(LecturaSpark):
    """Lectura desde EMR Serverless mediante el conector de Kinesis de Spark."""

    def __init__(self, stream: str, region: str):
        self.stream = stream
        self.region = region

    def formato_y_opciones(self) -> Tuple[str, Dict[str, str]]:
        return "kinesis", {
            "streamName": self.stream,
            "region": self.region,
            "startingPosition": "TRIM_HORIZON",
        }

    def paquetes_maven(self) -> str:
        # En EMR el conector viene con la imagen. Se confirma en la fase 5,
        # junto con la version de Spark de la release de EMR elegida.
        return ""


def _destino() -> Tuple[str, str]:
    return (
        os.environ.get("KINESIS_STREAM", "wikimedia-cambios"),
        os.environ.get("AWS_REGION", "eu-west-1"),
    )


def publicador_desde_entorno() -> PublicadorKinesis:
    return PublicadorKinesis(*_destino())


def lectura_desde_entorno() -> LecturaKinesis:
    return LecturaKinesis(*_destino())
