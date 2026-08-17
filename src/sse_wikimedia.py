"""Lector del stream SSE de Wikimedia, como generador reconectable.

Solo libreria estandar. Es el mismo parseo que se uso en la fase 0
(`scripts/captura_sse.py`), pero aquel fichero se deja intacto: es la evidencia
de la exploracion y no debe cambiar. Aqui se anade lo que la fase 0 no
necesitaba: reconexion indefinida y reanudacion por `Last-Event-ID`.
"""

import json
import logging
import time
import urllib.request
from typing import Iterator

log = logging.getLogger(__name__)

URL_POR_DEFECTO = "https://stream.wikimedia.org/v2/stream/recentchange"

CABECERAS = {
    "User-Agent": "wikipedia-realtime-pipeline/0.1 (portfolio; proyecto personal)",
    "Accept": "text/event-stream",
}


def eventos(url: str = URL_POR_DEFECTO, timeout: int = 60) -> Iterator[dict]:
    """Emite eventos ya parseados, reconectando indefinidamente.

    Al reconectar envia `Last-Event-ID`, con lo que Wikimedia reanuda desde
    donde se corto. Eso evita perder eventos, pero **puede reenviar algunos ya
    entregados**: es una de las dos fuentes de duplicados del pipeline, junto
    con el reprocesado desde checkpoint. Por eso la deduplicacion por `meta.id`
    de la fase 3 no es opcional.
    """
    ultimo_id = None
    espera = 1.0

    while True:
        cabeceras = dict(CABECERAS)
        if ultimo_id is not None:
            cabeceras["Last-Event-ID"] = ultimo_id

        try:
            peticion = urllib.request.Request(url, headers=cabeceras)
            respuesta = urllib.request.urlopen(peticion, timeout=timeout)
            espera = 1.0  # la conexion prospero
            datos = []

            for linea_bruta in respuesta:
                linea = linea_bruta.decode("utf-8", errors="replace").rstrip("\n")

                if linea.startswith("id:"):
                    ultimo_id = linea[3:].strip()
                elif linea.startswith("data:"):
                    datos.append(linea[5:].strip())
                elif linea == "":
                    if not datos:
                        continue
                    crudo = "".join(datos)
                    datos = []
                    try:
                        yield json.loads(crudo)
                    except json.JSONDecodeError:
                        # Un evento ilegible no puede tumbar el productor.
                        log.warning("evento no parseable, descartado")

            respuesta.close()
            log.warning("el stream termino sin error, reconectando")

        except GeneratorExit:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("corte en el stream (%r), reconectando en %.0fs", exc, espera)

        time.sleep(espera)
        espera = min(espera * 2, 30.0)
