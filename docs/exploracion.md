# Fase 0 — Reconocimiento de la fuente

Captura del 17 de agosto de 2026, 10 minutos exactos del stream SSE público de
Wikimedia (`https://stream.wikimedia.org/v2/stream/recentchange`), sin
autenticación y sin dependencias externas.

- Script de captura: `scripts/captura_sse.py`
- Script de análisis: `scripts/analiza_captura.py`
- Fichero crudo: `data/raw/captura.jsonl` (33,0 MB, no versionado)
- Ejemplo de cada tipo: `data/ejemplos_por_tipo.json`

Todos los números de este documento salen de esa captura. Ninguno es estimado.

## 1. Caudal observado

| Métrica | Valor |
|---|---|
| Eventos capturados | 22.415 |
| Duración | 599,4 s |
| Media sostenida | **37,4 ev/s** |
| Mediana por segundo | 36,0 ev/s |
| p95 por segundo | 59,1 ev/s |
| **Pico en 1 s** | **114 ev/s** |
| Reconexiones | 0 |
| Errores de parseo | 0 |

El pico triplica la mediana. No es un caudal plano: hay ráfagas de bots.

Proyección a un día a este ritmo: ~3,2 millones de eventos, ~4,6 GB de JSON
crudo. Relevante para dimensionar el almacenamiento y para no dejar corriendo
el pipeline sin querer.

## 2. Tamaño del evento

| Métrica | Bytes |
|---|---|
| Media | 1.423 |
| p50 | 1.356 |
| p95 | 2.078 |
| Máximo | 5.582 |

Volumen total 31,9 MB, caudal 53,2 KB/s. Eventos pequeños y homogéneos: el
cuello de botella de este pipeline va a ser el número de mensajes, no su tamaño.

## 3. Tipos presentes

| Tipo | Eventos | % |
|---|---|---|
| `edit` | 12.457 | 55,6 % |
| `categorize` | 8.456 | 37,7 % |
| `log` | 1.128 | 5,0 % |
| `new` | 374 | 1,7 % |

Un solo `$schema` en toda la captura: `/mediawiki/recentchange/1.0.0`.

Reparto por origen: `commons.wikimedia.org` 42,5 %, `www.wikidata.org` 16,5 %,
`en.wikipedia.org` 9,1 %, `ce.wikipedia.org` 5,7 %, el resto muy repartido.
Un 41,1 % de los eventos los generan bots.

Ojo con `categorize`: es casi el 38 % del volumen y no es una edición, es la
consecuencia de una. Si el pipeline cuenta "cambios" sin filtrar por tipo, el
número estará inflado por bots que recategorizan en masa.

## 4. Esquema real observado

Campos presentes en el 100 % de los eventos:

```
$schema, bot, comment, namespace, parsedcomment, server_name, server_script_path,
server_url, timestamp, title, title_url, type, user, wiki,
meta.{domain, dt, id, offset, partition, request_id, stream, topic, uri}
```

Campos condicionales, con su presencia real:

| Campo | Presencia | Aparece en |
|---|---|---|
| `id` | 97,8 % | todos salvo parte de `log` |
| `notify_url` | 95,0 % | todos salvo parte de `log` |
| `length.{new,old}`, `revision.{new,old}`, `minor` | 55–57 % | `edit`, `new` |
| `patrolled` | 42,0 % | `new` y parte de `edit` |
| `log_type`, `log_action`, `log_id`, `log_action_comment`, `log_params` | 5,0 % | solo `log` |

### Dos trampas de esquema encontradas

**a) `log_params` cambia de tipo entre eventos.** Es un objeto cuando el log
tiene parámetros y un **array vacío `[]`** cuando no los tiene — el resultado
típico de serializar en PHP un array asociativo vacío. Además, cuando es objeto
sus claves varían por `log_type`: se observaron más de 30 claves distintas
(`filter`, `img_sha1`, `newgroups`, `restrictions.pages`...), muchas por debajo
del 0,1 % de presencia.

Spark no puede inferir una columna que unas veces es `struct` y otras `array`:
el job falla o descarta el campo. Ver decisión en `docs/decisiones.md`.

**b) `id` falta en un 2,2 % de los eventos**, todos de tipo `log`. No sirve como
clave por sí solo. `meta.id` (UUID) sí está en el 100 %.

## 5. Orden, retraso y duplicados

### Duplicados

**Cero duplicados** en los 22.415 eventos, ni por `meta.id` ni por
`(wiki, id)`. Con una sola conexión y sin cortes, la fuente entrega exactamente
una vez.

Esto **no** significa que el pipeline pueda saltarse la deduplicación: los
duplicados los va a introducir el propio sistema al reconectar con
`Last-Event-ID` o al reprocesar desde un checkpoint, no la fuente. La clave de
deduplicación es `meta.id`.

### El tiempo de evento: hay dos, y solo uno sirve

Cada evento trae dos marcas temporales, y la diferencia entre ellas es el
hallazgo central de esta fase.

**`meta.dt`** — momento en que el evento se publica en el bus de Wikimedia.
ISO-8601 con milisegundos, presente en el 100 %.

| Medida sobre `meta.dt` | Valor |
|---|---|
| Eventos fuera de orden respecto al máximo ya visto | 288 (1,28 %) |
| Retraso de esos, p50 / p95 / p99 | 0,00 / 0,01 / 0,06 s |
| Retraso máximo | **0,99 s** |

**`timestamp`** — momento del cambio en MediaWiki. Epoch en segundos.

| Medida sobre `timestamp` | Valor |
|---|---|
| Eventos fuera de orden | 13.975 (**62,35 %**) |
| Retraso p50 / p95 / p99 | 2 / 5 / 13 s |
| Retraso máximo | **607.725.399 s (19,3 años)** |

Ese máximo no es un error de medición ni un valor corrupto. Son 16 eventos de
tipo `log` sobre ficheros de Commons en los que `timestamp` es la fecha
**original del fichero**, no la del evento:

```
log | commons.wikimedia.org | ts=2007-05-15T18:28:19Z | File:Gretchen Wilson in performance...
log | commons.wikimedia.org | ts=2008-04-26T21:17:08Z | File:Freede Wellness Center...
log | commons.wikimedia.org | ts=2009-10-11T22:37:27Z | File:Internal hallway, Magna Vista...
```

Son solo el 0,07 % de los eventos, pero bastan para romper cualquier ventana
temporal: no existe un watermark que cubra 19 años, y con `timestamp` como
tiempo de evento esos registros caen fuera de toda ventana y desaparecen en
silencio, o fuerzan un estado que no cabe en memoria.

**Conclusión: el tiempo de evento del pipeline es `meta.dt`.** `timestamp` se
conserva como atributo de negocio, nunca como eje temporal.

### Latencia de ingesta

`meta.dt` frente al momento de recepción local:

| p50 | p95 | p99 | máx |
|---|---|---|---|
| −0,01 s | 0,20 s | 0,48 s | 1,93 s |

Los valores ligeramente negativos son desfase de reloj. Se verificó contra la
cabecera `Date` de un endpoint no cacheable de Wikimedia: el desfase está por
debajo de la resolución de 1 s de esa cabecera, y el mínimo observado (−0,03 s)
lo acota a unas decenas de milisegundos. Las cifras son utilizables.

La primera medición del reloj dio 1.396 s de desfase y era falsa: la respuesta
venía de caché con `Date` congelado. Se detectó porque el desfase crecía
exactamente lo que tardaba el bucle.

### Watermark propuesto

El desorden de la fuente sobre `meta.dt` es de **menos de 1 segundo**. El
watermark no lo manda la fuente, lo manda el sistema: reconexiones del
productor, reintentos y el retraso de consumo de Spark.

Propuesta: **watermark de 30 segundos**. Es dos órdenes de magnitud por encima
del desorden observado (0,99 s), absorbe una reconexión corta del productor, y
mantiene el estado de las ventanas pequeño. A validar en la fase 3 provocando
retrasos con el generador sintético.

Lo que **no** se debe hacer es fijarlo por el peor caso de `timestamp`, que es
justo el error que esta fase existía para evitar.

## 6. Preguntas que responderá el pipeline

Confirmadas las tres. Descartada una cuarta candidata (detección de reversiones
por heurística sobre `comment`): demasiado frágil en decenas de idiomas para lo
que aporta.

**P1 — ¿Qué wikis y espacios de nombres concentran la actividad, minuto a
minuto?**
Agregación por ventana de 1 minuto sobre `server_name` y `namespace`. Campos
presentes al 100 %. Es la más barata y la que mejor se ve en movimiento: el
ranking cambia solo, sirve de demo visual para la fase 4. Su respuesta es
previsible (Commons y Wikidata dominan), así que demuestra que el pipeline
funciona, no que el dato sorprenda.

**P2 — ¿Cómo varía la proporción de actividad humana frente a automática entre
ventanas de un minuto?**
Reformulada respecto a la propuesta original, que hablaba del ciclo diario: eso
exige un día de datos y el pipeline no va a estar corriendo un día. Sobre
ventanas de 1 minuto sí se responde con una sesión corta, y la respuesta sigue
siendo no obvia: en 10 minutos el reparto global fue 59 / 41, pero los picos de
114 ev/s son ráfagas de bots, así que la proporción instantánea oscila mucho
más que la media. Es el argumento más directo de por qué streaming y no batch:
en batch verías la media y te perderías la ráfaga.

**P3 — ¿Qué páginas están siendo editadas por varias personas a la vez?**
Ventana deslizante contando usuarios distintos por `(wiki, title)`, excluyendo
bots y quedándose con los grupos de dos o más. Es la única que en batch no se
responde bien, porque "a la vez" es una propiedad de la ventana y no del dato:
al día siguiente la coincidencia ya no existe. También es la que ejercita de
verdad la maquinaria —estado por clave, ventana deslizante, watermark— y por
tanto la que hace que el proyecto demuestre lo que dice demostrar.

Riesgo a anticipar en P3: en franjas tranquilas puede devolver resultados
vacíos. Hay que preverlo para que la demo de la fase 4 no salga en blanco —
probablemente ajustando el ancho de la ventana con datos reales.

## 7. Veredicto sobre la fuente

**La fuente sirve.** Caudal estable y suficiente para que el pipeline tenga
trabajo, sin autenticación, sin cortes en 10 minutos, esquema estable y un
volumen manejable en local.

El generador sintético sigue haciendo falta: la fuente real no produce
duplicados ni retrasos apreciables, así que sin él no hay forma de demostrar
que la deduplicación y el watermark funcionan.
