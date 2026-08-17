"""Implementacion sobre Kafka. En local la sirve Redpanda, que habla el mismo
protocolo, asi que este codigo no distingue una de otra.
"""

import logging
import os
from typing import Dict, Tuple

from .base import LecturaSpark, Publicador

log = logging.getLogger(__name__)

# Version fijada en docs/versiones.md. Debe coincidir con la de Spark.
PAQUETE_SPARK_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"


class PublicadorKafka(Publicador):
    def __init__(self, brokers: str, topico: str):
        # El import va aqui y no arriba a proposito: la imagen de Spark no
        # lleva confluent_kafka, porque Spark lee de la cola pero no publica.
        # Con el import arriba, pedir solo la lectura reventaria alli.
        from confluent_kafka import Producer

        self.topico = topico
        self._sin_entregar = 0
        self._productor = Producer(
            {
                "bootstrap.servers": brokers,
                # Espera confirmacion de todas las replicas antes de dar el
                # evento por entregado. Con un solo nodo en local da igual, pero
                # el ajuste tiene que ser el mismo que en cloud.
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "snappy",
                # Agrupa hasta 10 ms de eventos por lote. A 37 ev/s eso son unos
                # pocos eventos por peticion, suficiente para no saturar de
                # llamadas pequenas sin anadir latencia apreciable.
                "linger.ms": 10,
            }
        )

    def _entregado(self, error, mensaje):
        if error is not None:
            self._sin_entregar += 1
            log.error("evento no entregado: %s", error)

    def publicar(self, clave: str, valor: bytes) -> None:
        try:
            self._productor.produce(
                self.topico,
                key=clave.encode("utf-8"),
                value=valor,
                on_delivery=self._entregado,
            )
        except BufferError:
            # La cola interna esta llena: el broker no traga al ritmo al que
            # producimos. Vaciamos y reintentamos una vez.
            log.warning("cola interna llena, forzando entrega")
            self._productor.flush(10)
            self._productor.produce(
                self.topico,
                key=clave.encode("utf-8"),
                value=valor,
                on_delivery=self._entregado,
            )
        # Atiende las devoluciones de entrega ya disponibles, sin bloquear.
        self._productor.poll(0)

    def vaciar(self, timeout_s: float = 10.0) -> int:
        return self._productor.flush(timeout_s)

    def cerrar(self) -> None:
        pendientes = self.vaciar()
        if pendientes:
            log.warning("quedaron %d eventos sin entregar al cerrar", pendientes)
        if self._sin_entregar:
            log.warning("%d eventos fallaron la entrega", self._sin_entregar)


class LecturaKafka(LecturaSpark):
    def __init__(self, brokers: str, topico: str):
        self.brokers = brokers
        self.topico = topico

    def formato_y_opciones(self) -> Tuple[str, Dict[str, str]]:
        return "kafka", {
            "kafka.bootstrap.servers": self.brokers,
            "subscribe": self.topico,
            # Desde el principio del topico la primera vez. En los reinicios
            # manda el checkpoint, no esta opcion.
            "startingOffsets": "earliest",
            # Que un job se quede atras no debe romperlo: preferimos procesar
            # tarde a fallar. Se vigila con la metrica de retraso.
            "failOnDataLoss": "false",
        }

    def normalizar(self, df):
        """Sobre de Kafka -> esquema comun."""
        from pyspark.sql import functions as F

        return df.select(
            F.col("key").cast("string").alias("clave"),
            F.col("value").cast("string").alias("valor"),
            F.col("topic").alias("origen"),
            F.col("partition").cast("string").alias("particion"),
            F.col("offset").cast("string").alias("desplazamiento"),
            F.col("timestamp").alias("ts_cola"),
        )

    def paquetes_maven(self) -> str:
        return PAQUETE_SPARK_KAFKA

    def desplazamiento_es_consecutivo(self) -> bool:
        return True


def _destino() -> Tuple[str, str]:
    return (
        os.environ.get("KAFKA_BROKERS", "localhost:19092"),
        os.environ.get("KAFKA_TOPICO", "wikimedia.cambios"),
    )


def publicador_desde_entorno() -> PublicadorKafka:
    return PublicadorKafka(*_destino())


def lectura_desde_entorno() -> LecturaKafka:
    return LecturaKafka(*_destino())
