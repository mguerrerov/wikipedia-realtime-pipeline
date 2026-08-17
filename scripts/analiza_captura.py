"""Analiza la captura de la fase 0 y saca los numeros de docs/exploracion.md.

Sin dependencias externas. Lee el JSONL que produce captura_sse.py y responde
a las cinco preguntas de la fase 0: caudal, tamano, tipos, esquema real,
desorden y duplicados.

Uso:
    python scripts/analiza_captura.py data/raw/captura.jsonl
"""

import collections
import json
import math
import sys


def percentil(valores, p):
    """Percentil por interpolacion lineal. Los valores deben venir ordenados."""
    if not valores:
        return float("nan")
    k = (len(valores) - 1) * (p / 100.0)
    bajo = math.floor(k)
    alto = math.ceil(k)
    if bajo == alto:
        return valores[int(k)]
    return valores[bajo] * (alto - k) + valores[alto] * (k - bajo)


def iso_a_epoch(texto):
    """Convierte '2026-08-17T15:02:17.471Z' a epoch en segundos (float).

    Lo hacemos a mano en vez de con datetime.fromisoformat porque en Python
    3.10 esa funcion no acepta la 'Z' final.
    """
    from datetime import datetime, timezone

    return datetime.strptime(texto, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=timezone.utc
    ).timestamp()


def tipo_json(valor):
    if valor is None:
        return "null"
    if isinstance(valor, bool):
        return "bool"
    if isinstance(valor, int):
        return "int"
    if isinstance(valor, float):
        return "double"
    if isinstance(valor, str):
        return "string"
    if isinstance(valor, list):
        return "array"
    if isinstance(valor, dict):
        return "struct"
    return type(valor).__name__


def aplanar(evento, prefijo=""):
    """Devuelve {ruta: tipo} recorriendo los structs anidados."""
    salida = {}
    for clave, valor in evento.items():
        ruta = prefijo + clave
        salida[ruta] = tipo_json(valor)
        if isinstance(valor, dict):
            salida.update(aplanar(valor, ruta + "."))
    return salida


def analizar(ruta):
    total = 0
    recibidos = []
    tamanos = []
    tipos = collections.Counter()
    esquemas = collections.Counter()
    wikis = collections.Counter()
    bots = collections.Counter()
    espacios = collections.Counter()
    campos_presencia = collections.Counter()
    campos_tipos = collections.defaultdict(collections.Counter)
    campos_por_tipo = collections.defaultdict(collections.Counter)
    por_segundo = collections.Counter()
    ids_vistos = collections.Counter()
    ids_evento = collections.Counter()
    latencias = []
    retrasos = []  # cuanto por detras del maximo dt ya visto llega un evento
    desordenados = 0
    max_dt = None
    ejemplos = {}
    sin_dt = 0
    errores_parseo = 0

    with open(ruta, encoding="utf-8") as entrada:
        for linea in entrada:
            linea = linea.strip()
            if not linea:
                continue
            registro = json.loads(linea)
            evento = registro["_evento"]
            recibido = registro["_recibido"]

            if "_error_parseo" in evento:
                errores_parseo += 1
                continue

            total += 1
            recibidos.append(recibido)
            tamanos.append(len(json.dumps(evento, ensure_ascii=False).encode("utf-8")))
            por_segundo[int(recibido)] += 1

            tipo = evento.get("type", "(sin tipo)")
            tipos[tipo] += 1
            esquemas[evento.get("$schema", "(sin schema)")] += 1
            wikis[evento.get("server_name", "(sin server_name)")] += 1
            bots["bot" if evento.get("bot") else "humano"] += 1
            espacios[evento.get("namespace", "(sin namespace)")] += 1

            if tipo not in ejemplos:
                ejemplos[tipo] = evento

            plano = aplanar(evento)
            for ruta, t in plano.items():
                campos_presencia[ruta] += 1
                campos_tipos[ruta][t] += 1
                campos_por_tipo[tipo][ruta] += 1

            meta = evento.get("meta") or {}
            id_meta = meta.get("id")
            if id_meta:
                ids_vistos[id_meta] += 1
            if evento.get("id") is not None:
                ids_evento[(evento.get("wiki"), evento.get("id"))] += 1

            dt_texto = meta.get("dt")
            if not dt_texto:
                sin_dt += 1
                continue
            try:
                dt = iso_a_epoch(dt_texto)
            except ValueError:
                sin_dt += 1
                continue

            latencias.append(recibido - dt)
            if max_dt is None:
                max_dt = dt
            elif dt < max_dt:
                desordenados += 1
                retrasos.append(max_dt - dt)
                max_dt = max(max_dt, dt)
            else:
                max_dt = dt

    duracion = (max(recibidos) - min(recibidos)) if recibidos else 0.0
    # Descartamos el primer y ultimo segundo: estan parcialmente cubiertos y
    # falsearian el pico a la baja.
    cubos = sorted(por_segundo)
    conteos_segundo = sorted(por_segundo[s] for s in cubos[1:-1]) if len(cubos) > 2 else []

    return {
        "total": total,
        "errores_parseo": errores_parseo,
        "duracion": duracion,
        "eps_medio": total / duracion if duracion else 0.0,
        "eps_p50": percentil(conteos_segundo, 50),
        "eps_p95": percentil(conteos_segundo, 95),
        "eps_pico": max(conteos_segundo) if conteos_segundo else 0,
        "tamanos": sorted(tamanos),
        "tipos": tipos,
        "esquemas": esquemas,
        "wikis": wikis,
        "bots": bots,
        "espacios": espacios,
        "campos_presencia": campos_presencia,
        "campos_tipos": campos_tipos,
        "campos_por_tipo": campos_por_tipo,
        "ids_vistos": ids_vistos,
        "ids_evento": ids_evento,
        "latencias": sorted(latencias),
        "retrasos": sorted(retrasos),
        "desordenados": desordenados,
        "sin_dt": sin_dt,
        "ejemplos": ejemplos,
    }


def informe(r):
    total = r["total"]
    salida = []
    p = salida.append

    p("== CAUDAL ==")
    p("Eventos            : %d" % total)
    p("Duracion           : %.1f s" % r["duracion"])
    p("Media              : %.1f ev/s" % r["eps_medio"])
    p("p50 por segundo    : %.1f ev/s" % r["eps_p50"])
    p("p95 por segundo    : %.1f ev/s" % r["eps_p95"])
    p("Pico (1 s)         : %d ev/s" % r["eps_pico"])
    p("Errores de parseo  : %d" % r["errores_parseo"])

    t = r["tamanos"]
    p("")
    p("== TAMANO DEL EVENTO (bytes, JSON UTF-8) ==")
    p("Media              : %.0f" % (sum(t) / len(t)) if t else "sin datos")
    p("p50 / p95 / max    : %.0f / %.0f / %d" % (percentil(t, 50), percentil(t, 95), max(t)))
    p("Volumen total      : %.1f MB" % (sum(t) / 1e6))
    p("Caudal             : %.1f KB/s" % (sum(t) / r["duracion"] / 1e3))

    p("")
    p("== TIPOS ==")
    for tipo, n in r["tipos"].most_common():
        p("  %-14s %7d  %5.1f%%" % (tipo, n, 100.0 * n / total))

    p("")
    p("== SCHEMAS ==")
    for s, n in r["esquemas"].most_common():
        p("  %-32s %7d" % (s, n))

    p("")
    p("== TOP 12 WIKIS ==")
    for w, n in r["wikis"].most_common(12):
        p("  %-28s %7d  %5.1f%%" % (w, n, 100.0 * n / total))

    p("")
    p("== BOT VS HUMANO ==")
    for k, n in r["bots"].most_common():
        p("  %-8s %7d  %5.1f%%" % (k, n, 100.0 * n / total))

    p("")
    p("== DUPLICADOS ==")
    dup_meta = {k: v for k, v in r["ids_vistos"].items() if v > 1}
    dup_ev = {k: v for k, v in r["ids_evento"].items() if v > 1}
    p("meta.id repetidos       : %d (%d entregas de mas)"
      % (len(dup_meta), sum(dup_meta.values()) - len(dup_meta)))
    p("(wiki, id) repetidos    : %d (%d entregas de mas)"
      % (len(dup_ev), sum(dup_ev.values()) - len(dup_ev)))

    p("")
    p("== DESORDEN (meta.dt frente al orden de llegada) ==")
    p("Eventos con dt          : %d (sin dt: %d)" % (total - r["sin_dt"], r["sin_dt"]))
    p("Llegan fuera de orden   : %d  (%.2f%%)"
      % (r["desordenados"], 100.0 * r["desordenados"] / total if total else 0))
    ret = r["retrasos"]
    if ret:
        p("Retraso de esos, en s   : p50 %.2f | p95 %.2f | p99 %.2f | max %.2f"
          % (percentil(ret, 50), percentil(ret, 95), percentil(ret, 99), max(ret)))

    lat = r["latencias"]
    if lat:
        p("")
        p("== LATENCIA DE INGESTA (recepcion - meta.dt, en s) ==")
        p("OJO: incluye el desfase del reloj local. Ver docs/exploracion.md.")
        p("min %.2f | p50 %.2f | p95 %.2f | p99 %.2f | max %.2f"
          % (min(lat), percentil(lat, 50), percentil(lat, 95),
             percentil(lat, 99), max(lat)))

    p("")
    p("== ESQUEMA OBSERVADO (presencia y tipo) ==")
    for ruta, n in sorted(r["campos_presencia"].items()):
        tipos_campo = r["campos_tipos"][ruta]
        desc = ", ".join("%s" % t for t, _ in tipos_campo.most_common())
        marca = "  <-- TIPO MIXTO" if len(tipos_campo) > 1 else ""
        p("  %-34s %6.1f%%  %s%s" % (ruta, 100.0 * n / total, desc, marca))

    p("")
    p("== CAMPOS EXCLUSIVOS POR TIPO DE EVENTO ==")
    comunes = {c for c, n in r["campos_presencia"].items() if n == total}
    for tipo, n in r["tipos"].most_common():
        propios = [c for c, k in r["campos_por_tipo"][tipo].items()
                   if c not in comunes and k >= n * 0.9]
        p("  %-14s (%d ev): %s" % (tipo, n, ", ".join(sorted(propios)) or "-"))

    return "\n".join(salida)


if __name__ == "__main__":
    ruta = sys.argv[1] if len(sys.argv) > 1 else "data/raw/captura.jsonl"
    resultado = analizar(ruta)
    print(informe(resultado))
    with open("data/ejemplos_por_tipo.json", "w", encoding="utf-8") as f:
        json.dump(resultado["ejemplos"], f, indent=2, ensure_ascii=False)
    print("\nEjemplo de cada tipo -> data/ejemplos_por_tipo.json")
