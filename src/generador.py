"""Generador sintetico de eventos, con retrasos y duplicados a voluntad.

La fuente real no produce ni duplicados ni retrasos apreciables -medido en la
fase 0: cero duplicados en 22.415 eventos y un desorden maximo de 0,99 s-, asi
que sin este generador no hay forma de demostrar que la deduplicacion y el
watermark hacen lo que decimos que hacen.

Publica por la misma interfaz que el productor real, asi que lo que se prueba
aqui es exactamente el mismo camino que recorren los datos de verdad.

Uso:
    python -m src.generador --duracion 120 --tasa 50
    python -m src.generador --duracion 60 --retraso-pct 20 --retraso-max 120
    python -m src.generador --duracion 60 --duplicados-pct 10
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from . import fuente_eventos

log = logging.getLogger("generador")

WIKIS = [
    ("commons.wikimedia.org", "commonswiki"),
    ("www.wikidata.org", "wikidatawiki"),
    ("en.wikipedia.org", "enwiki"),
    ("es.wikipedia.org", "eswiki"),
    ("fr.wikipedia.org", "frwiki"),
]

# Proporciones tomadas de la fase 0, para que el perfil se parezca al real.
TIPOS = [("edit", 0.556), ("categorize", 0.377), ("log", 0.050), ("new", 0.017)]

TITULOS = ["Pagina %d" % i for i in range(1, 401)]
USUARIOS = ["Usuario%d" % i for i in range(1, 61)]


def _elige_tipo(rnd: random.Random) -> str:
    x = rnd.random()
    acumulado = 0.0
    for tipo, peso in TIPOS:
        acumulado += peso
        if x <= acumulado:
            return tipo
    return "edit"


def fabrica_evento(rnd: random.Random, momento: datetime, etiqueta: str) -> dict:
    """Construye un evento con la forma del de Wikimedia.

    Solo se rellenan los campos que el pipeline usa. No es una imitacion
    completa del esquema: es la parte que Silver y Gold leen.
    """
    servidor, wiki = rnd.choice(WIKIS)
    return {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {
            "uri": "https://%s/wiki/x" % servidor,
            "id": str(uuid.UUID(int=rnd.getrandbits(128))),
            "dt": momento.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (momento.microsecond // 1000),
            "domain": servidor,
            "stream": "mediawiki.recentchange",
        },
        "id": rnd.randint(1, 10**9),
        "type": _elige_tipo(rnd),
        "namespace": rnd.choice([0, 0, 0, 6, 14]),
        "title": rnd.choice(TITULOS),
        "comment": etiqueta,
        "timestamp": int(momento.timestamp()),
        "user": rnd.choice(USUARIOS),
        "bot": rnd.random() < 0.41,
        "server_name": servidor,
        "wiki": wiki,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duracion", type=int, default=60, help="segundos")
    parser.add_argument("--tasa", type=float, default=40.0, help="eventos por segundo")
    parser.add_argument(
        "--retraso-pct",
        type=float,
        default=0.0,
        help="porcentaje de eventos con meta.dt en el pasado",
    )
    parser.add_argument(
        "--retraso-min", type=float, default=1.0, help="retraso minimo en segundos"
    )
    parser.add_argument(
        "--retraso-max", type=float, default=60.0, help="retraso maximo en segundos"
    )
    parser.add_argument(
        "--duplicados-pct",
        type=float,
        default=0.0,
        help="porcentaje de eventos que se publican dos veces, identicos",
    )
    parser.add_argument(
        "--semilla", type=int, default=42, help="para que la prueba sea repetible"
    )
    parser.add_argument(
        "--etiqueta", default=None, help="marca esta tanda en el campo comment"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s"
    )
    rnd = random.Random(args.semilla)
    # La etiqueta va en `comment` y distingue una tanda de otra: sin ella, dos
    # ensayos consecutivos se mezclan en la tabla y el analisis no separa.
    etiqueta = args.etiqueta or ("evento sintetico s%d" % args.semilla)

    publicador = fuente_eventos.crear_publicador()
    intervalo = 1.0 / args.tasa if args.tasa > 0 else 0.0

    fin = time.time() + args.duracion
    normales = tardios = duplicados = 0
    # Se anotan los identificadores de los eventos retrasados y duplicados para
    # poder buscarlos despues en las tablas y ver si entraron o se descartaron.
    marcados = {"tardios": [], "duplicados": []}

    log.info(
        "Generando %.0f ev/s durante %ds (tardios %.0f%%, duplicados %.0f%%)",
        args.tasa,
        args.duracion,
        args.retraso_pct,
        args.duplicados_pct,
    )

    try:
        while time.time() < fin:
            ahora = datetime.now(timezone.utc)

            es_tardio = rnd.random() * 100 < args.retraso_pct
            if es_tardio:
                retraso = rnd.uniform(args.retraso_min, args.retraso_max)
                momento = ahora - timedelta(seconds=retraso)
            else:
                momento = ahora

            evento = fabrica_evento(rnd, momento, etiqueta)
            carga = json.dumps(evento, ensure_ascii=False).encode("utf-8")
            clave = "%s|%s" % (evento["wiki"], evento["title"])

            publicador.publicar(clave, carga)
            if es_tardio:
                tardios += 1
                if len(marcados["tardios"]) < 200:
                    marcados["tardios"].append(
                        {"meta_id": evento["meta"]["id"], "retraso_s": round(retraso, 1)}
                    )
            else:
                normales += 1

            # El duplicado se publica identico, con el mismo meta.id: es lo que
            # ocurre de verdad al reconectar con Last-Event-ID o al reprocesar
            # desde un checkpoint.
            if rnd.random() * 100 < args.duplicados_pct:
                publicador.publicar(clave, carga)
                duplicados += 1
                if len(marcados["duplicados"]) < 200:
                    marcados["duplicados"].append(evento["meta"]["id"])

            if intervalo:
                time.sleep(intervalo)
    finally:
        publicador.vaciar()
        publicador.cerrar()

    total = normales + tardios + duplicados
    log.info(
        "Publicados %d (%d normales, %d tardios, %d duplicados)",
        total,
        normales,
        tardios,
        duplicados,
    )

    # No se escribe fichero de marcados: el contenedor se borra al terminar.
    # La comprobacion se hace comparando Bronze con Silver por tramos de
    # retraso, que ademas es mas riguroso que fiarse de una lista.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
