# Decisiones

Cinco líneas por decisión: qué decidí, qué alternativas había, por qué esa, qué
me cuesta.

## D1 — La fuente es el stream SSE de Wikimedia

- **Decidí**: usar `stream.wikimedia.org/v2/stream/recentchange` como fuente
  principal, y mantener el generador sintético como complemento obligatorio.
- **Alternativas**: solo generador sintético; otra API pública.
- **Por qué**: 37,4 ev/s sostenidos, sin autenticación, sin cortes en 10 min,
  esquema estable y un solo `$schema`. Datos reales dan mejor material de
  entrevista que datos inventados.
- **Cuesta**: dependo de un servicio externo que puede cambiar o caerse, y no
  produce ni duplicados ni retrasos, así que no puedo validar el watermark
  contra ella.

## D2 — El tiempo de evento es `meta.dt`, no `timestamp`

- **Decidí**: usar `meta.dt` (publicación en el bus) como eje temporal de todas
  las ventanas. `timestamp` se conserva como columna de negocio.
- **Alternativas**: usar `timestamp`, que es el tiempo "real" del cambio en
  MediaWiki y el que intuitivamente parece correcto.
- **Por qué**: medido en la fase 0, `timestamp` llega fuera de orden el 62,35 %
  de las veces y con un retraso máximo de **19,3 años** — eventos `log` de
  Commons donde `timestamp` es la fecha original del fichero. `meta.dt` va
  desordenado el 1,28 % con un máximo de 0,99 s.
- **Cuesta**: las ventanas miden cuándo Wikimedia publicó el cambio, no cuándo
  ocurrió. Diferencia real de ~1,7 s en la mediana; hay que decirlo en el README.

## D3 — Watermark de 30 segundos

- **Decidí**: 30 s sobre `meta.dt`. Confirmado el 17/08/2026 como punto de
  partida; se revalida en la fase 3 con el generador sintético.
- **Alternativas**: 1 s (ajustado al desorden observado); 5 min (conservador).
- **Por qué**: el desorden de la fuente es < 1 s, así que 30 s son dos órdenes
  de magnitud de margen; lo que de verdad cubre es una reconexión del productor
  o un retraso de consumo de Spark, no la fuente.
- **Cuesta**: hasta 30 s de retraso en cerrar cada ventana, y estado en memoria
  proporcional. A validar en la fase 3 con el generador sintético.

## D4 — `log_params` se guarda como string JSON en Bronze

- **Decidí**: en Bronze, `log_params` se almacena como texto JSON sin parsear.
- **Alternativas**: dejar que Spark lo infiera; declarar un struct fijo con las
  claves observadas; descartar el campo.
- **Por qué**: el campo es un objeto cuando hay parámetros y un **array vacío**
  `[]` cuando no los hay. Spark no puede inferir una columna que alterna
  `struct` y `array`, e Iceberg no admite ese tipo. Sus claves además varían
  por `log_type` (más de 30 observadas, casi todas por debajo del 0,1 %).
- **Cuesta**: para consultarlo hay que parsearlo en Silver, y solo para los
  `log_type` que interesen. Afecta al 5 % de los eventos.

## D5 — La clave de deduplicación es `meta.id`

- **Decidí**: deduplicar por `meta.id`.
- **Alternativas**: `(wiki, id)`; `(wiki, timestamp, user, title)`.
- **Por qué**: `meta.id` es un UUID presente en el 100 % de los eventos. El
  campo `id` falta en el 2,2 % (eventos `log`), así que no vale por sí solo.
- **Cuesta**: nada apreciable. La deduplicación sigue siendo necesaria aunque la
  fuente no duplique, porque los duplicados los introducen las reconexiones y el
  reprocesado desde checkpoint.

## D6 — Captura y análisis sin dependencias externas

- **Decidí**: `scripts/captura_sse.py` y `scripts/analiza_captura.py` usan solo
  la librería estándar de Python 3.10.
- **Alternativas**: `sseclient-py` + `pandas`, que habría sido más corto.
- **Por qué**: el protocolo SSE es lo bastante simple como para no justificar
  una dependencia, y así la fase 0 queda reproducible sin instalar nada.
- **Cuesta**: ~120 líneas de parseo y estadística escritas a mano, incluido el
  cálculo de percentiles.
