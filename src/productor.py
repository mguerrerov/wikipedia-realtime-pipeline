"""Productor: lee el stream SSE de Wikimedia y lo publica en la cola.

No sabe si detras hay Redpanda o Kinesis. Eso lo decide `fuente_eventos.crear()`
a partir de la variable de entorno, y es el unico sitio del proyecto donde esa
decision existe.

Uso:
    python -m src.productor
    python -m src.productor --limite 100      # publica 100 eventos y para
"""

import argparse
import json
import logging
import os
import signal
import sys
import time

from . import fuente_eventos
from .sse_wikimedia import URL_POR_DEFECTO, eventos

log = logging.getLogger("productor")

# Cada cuantos segundos se informa del ritmo.
INTERVALO_INFORME = 30.0


def clave_de(evento: dict) -> str:
    """Clave de particion: wiki y titulo de la pagina.

    Elegida asi por dos razones. Reparte bien -ningun valor concentra el
    trafico, y hay decenas de miles de titulos distintos por minuto- y garantiza
    que todos los cambios de una misma pagina caen en la misma particion y
    conservan su orden. Eso ultimo lo necesita la pregunta P3, que cuenta
    editores distintos por (wiki, titulo).
    """
    return "%s|%s" % (evento.get("wiki", "?"), evento.get("title", "?"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.environ.get("SSE_URL", URL_POR_DEFECTO))
    parser.add_argument(
        "--limite",
        type=int,
        default=int(os.environ.get("LIMITE_EVENTOS", "0")),
        help="publica N eventos y termina. 0 = sin limite",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    publicador, lectura = fuente_eventos.crear()
    formato, _ = lectura.formato_y_opciones()
    log.info(
        "Publicando en %s (implementación: %s)",
        os.environ.get(fuente_eventos.VARIABLE, fuente_eventos.POR_DEFECTO),
        formato,
    )

    # Al parar el contenedor, Docker manda SIGTERM. Sin esto, el proceso muere
    # con eventos aun en el buffer del cliente y se pierden.
    parar = {"ahora": False}

    def manejar(_sig, _frame):
        log.info("Señal de parada recibida, vaciando el buffer...")
        parar["ahora"] = True

    signal.signal(signal.SIGTERM, manejar)
    signal.signal(signal.SIGINT, manejar)

    publicados = 0
    inicio = time.time()
    ultimo_informe = inicio

    try:
        for evento in eventos(args.url):
            publicador.publicar(
                clave_de(evento),
                json.dumps(evento, ensure_ascii=False).encode("utf-8"),
            )
            publicados += 1

            ahora = time.time()
            if ahora - ultimo_informe >= INTERVALO_INFORME:
                log.info(
                    "%d eventos publicados (%.1f ev/s de media)",
                    publicados,
                    publicados / (ahora - inicio),
                )
                ultimo_informe = ahora

            if parar["ahora"] or (args.limite and publicados >= args.limite):
                break
    finally:
        pendientes = publicador.vaciar()
        publicador.cerrar()
        transcurrido = time.time() - inicio
        log.info(
            "Fin: %d eventos en %.1f s (%.1f ev/s). Sin entregar: %d",
            publicados,
            transcurrido,
            publicados / transcurrido if transcurrido else 0.0,
            pendientes,
        )

    return 1 if pendientes else 0


if __name__ == "__main__":
    sys.exit(main())
