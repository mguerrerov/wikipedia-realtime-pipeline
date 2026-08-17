"""Captura eventos del stream SSE de Wikimedia a un fichero JSONL.

Fase 0 (reconocimiento). Sin dependencias externas: solo libreria estandar.

Cada linea del fichero de salida es un objeto JSON con dos campos:

    _recibido : epoch en segundos (float) del momento en que llego el evento
    _evento   : el evento tal cual lo publica Wikimedia, ya parseado

El campo _recibido lo anadimos nosotros y es imprescindible: la latencia y el
desorden se miden comparando el timestamp del propio evento con el momento en
que lo recibimos. Sin esa marca local la fase 0 no puede responder cual debe
ser el watermark.

Uso:
    python scripts/captura_sse.py --minutos 10 --salida data/raw/captura.jsonl
"""

import argparse
import json
import os
import sys
import time
import urllib.request

URL_STREAM = "https://stream.wikimedia.org/v2/stream/recentchange"

# Sin User-Agent propio Wikimedia puede rechazar o limitar la conexion.
CABECERAS = {
    "User-Agent": "wikipedia-realtime-pipeline/0.1 (portfolio; fase 0 reconocimiento)",
    "Accept": "text/event-stream",
}


def abrir_stream(ultimo_id=None, timeout=30):
    """Abre la conexion SSE. Si hay ultimo_id, pide reanudar desde ahi."""
    cabeceras = dict(CABECERAS)
    if ultimo_id is not None:
        cabeceras["Last-Event-ID"] = ultimo_id
    peticion = urllib.request.Request(URL_STREAM, headers=cabeceras)
    return urllib.request.urlopen(peticion, timeout=timeout)


def capturar(segundos, ruta_salida, max_reintentos=5):
    """Captura durante N segundos. Devuelve estadisticas de la captura."""
    os.makedirs(os.path.dirname(ruta_salida) or ".", exist_ok=True)

    fin = time.time() + segundos
    total = 0
    reconexiones = []  # (momento, segundos_de_hueco, motivo)
    ultimo_id = None
    intentos = 0
    inicio = time.time()

    with open(ruta_salida, "w", encoding="utf-8") as salida:
        while time.time() < fin:
            corte = time.time()
            try:
                respuesta = abrir_stream(ultimo_id)
                intentos = 0  # la conexion prospero, reseteamos el contador
                # Buffer de un evento SSE: acumulamos hasta la linea en blanco.
                datos = []
                for linea_bruta in respuesta:
                    if time.time() >= fin:
                        break

                    linea = linea_bruta.decode("utf-8", errors="replace").rstrip("\n")

                    if linea.startswith("id:"):
                        ultimo_id = linea[3:].strip()
                    elif linea.startswith("data:"):
                        datos.append(linea[5:].strip())
                    elif linea == "":
                        # Linea en blanco: fin del evento. Los comentarios del
                        # servidor (":ok") y los eventos sin data se ignoran.
                        if not datos:
                            continue
                        recibido = time.time()
                        crudo = "".join(datos)
                        datos = []
                        try:
                            evento = json.loads(crudo)
                        except json.JSONDecodeError:
                            # Lo registramos pero no abortamos: un evento
                            # ilegible tambien es un dato de la fase 0.
                            evento = {"_error_parseo": crudo[:500]}
                        salida.write(
                            json.dumps(
                                {"_recibido": recibido, "_evento": evento},
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        total += 1
                        if total % 500 == 0:
                            salida.flush()
                            transcurrido = time.time() - inicio
                            print(
                                "  %d eventos (%.1f ev/s de media)"
                                % (total, total / transcurrido),
                                flush=True,
                            )
                respuesta.close()
            except Exception as exc:  # noqa: BLE001 - queremos capturar todo
                intentos += 1
                hueco = time.time() - corte
                reconexiones.append((corte, hueco, repr(exc)))
                print("  corte tras %.1fs: %r" % (hueco, exc), file=sys.stderr)
                if intentos >= max_reintentos:
                    print("  demasiados reintentos seguidos, abandono", file=sys.stderr)
                    break
                if time.time() >= fin:
                    break
                # Espera creciente antes de reconectar.
                time.sleep(min(2 ** intentos, 30))

    return {
        "eventos": total,
        "segundos_reales": time.time() - inicio,
        "reconexiones": reconexiones,
        "salida": ruta_salida,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutos", type=float, default=10.0)
    parser.add_argument("--salida", default="data/raw/captura.jsonl")
    args = parser.parse_args()

    segundos = args.minutos * 60
    print("Capturando %.0f minutos de %s" % (args.minutos, URL_STREAM), flush=True)
    resultado = capturar(segundos, args.salida)

    print("")
    print("Eventos capturados : %d" % resultado["eventos"])
    print("Duracion real      : %.1f s" % resultado["segundos_reales"])
    print("Reconexiones       : %d" % len(resultado["reconexiones"]))
    for momento, hueco, motivo in resultado["reconexiones"]:
        print("  - hueco de %.1fs por %s" % (hueco, motivo))
    tam = os.path.getsize(resultado["salida"])
    print("Fichero            : %s (%.1f MB)" % (resultado["salida"], tam / 1e6))


if __name__ == "__main__":
    main()
