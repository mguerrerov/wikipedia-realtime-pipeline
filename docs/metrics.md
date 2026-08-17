# Métricas medidas

Ningún número de este fichero es una estimación. Cada uno viene de una
ejecución concreta, con la fecha y el comando que lo produjo. Si un número no
está aquí, no puede aparecer en el README.

Equipo de las mediciones: Windows 11, 16 núcleos, 15,6 GB de RAM disponibles
para Docker.

## Fuente (fase 0) — 17/08/2026

Captura de 10 minutos del stream SSE, `scripts/captura_sse.py`.

| Métrica | Valor |
|---|---|
| Eventos | 22.415 en 599,4 s |
| Ritmo sostenido | **37,4 ev/s** |
| Mediana por segundo | 36,0 ev/s |
| p95 por segundo | 59,1 ev/s |
| **Pico en 1 s** | **114 ev/s** |
| Tamaño medio del evento | 1.423 B |
| p95 / máximo | 2.078 B / 5.582 B |
| Caudal | 53,2 KB/s |
| Reconexiones | 0 |

Desorden sobre `meta.dt`: 1,28 % de eventos fuera de orden, máximo 0,99 s.
Duplicados en origen: 0.

## Bronze (fase 2) — 17/08/2026

Dos ejecuciones de 90 s con parada y arranque entre medias.

| Métrica | Valor |
|---|---|
| Filas ingeridas | 25.911 |
| **Duplicados** | **0** |
| **Huecos** | **0** |
| Instantáneas Iceberg | 20 (10 por ejecución) |
| Tamaño en MinIO | 6,4 MB |

Comprobado con `src/jobs/verifica_bronce.py`. El método es exacto, no
aproximado: `(particion, desplazamiento)` identifica unívocamente cada mensaje
y los desplazamientos de una partición son consecutivos.

## Silver y Gold (fase 3) — 17/08/2026

Acumulado tras los ensayos de la fase 3, con dos tandas del generador
sintético mezcladas con el flujo real.

| Tabla | Filas | Tamaño |
|---|---|---|
| `bronce.cambios` | 76.871 | 20 MB |
| `plata.cambios` | 71.484 | 5,2 MB |
| `oro.actividad_por_wiki` | 3.971 | — |
| `oro.humano_vs_bot` | 58 | — |
| `oro.paginas_concurrentes` | 4.955 | — |
| **Total en MinIO** | | **78 MB** |

Silver ocupa **cuatro veces menos que Bronze** con casi las mismas filas: es el
efecto de tipar y quedarse con los campos que se usan, en vez de guardar el
JSON entero como texto.

De los 78 MB totales, unos 52 MB son puntos de control. En un pipeline de
streaming el estado de control no es despreciable frente a los datos, y
conviene saberlo antes de dimensionar nada en S3.

### Deduplicación

| Tanda | Duplicados inyectados | Duplicados en Silver |
|---|---|---|
| Semilla 42 | 1.004 | **0** |
| Semilla 7 | 636 | **0** |

Sobre 71.484 filas en Silver, `meta_id` distintos: 71.484. Cero duplicados.

### Watermark de 30 s: supervivencia por retraso

Segunda tanda, con Silver ya en marcha y el watermark avanzado.

| Retraso del evento | En Bronze | En Silver | Supervivencia |
|---|---|---|---|
| < 5 s | 3.091 | 3.091 | **100 %** |
| 5-15 s | 258 | 258 | **100 %** |
| 15-30 s | 366 | 366 | **100 %** |
| 30-45 s | 361 | 338 | 93,6 % |
| 45-60 s | 365 | 37 | 10,1 % |
| > 60 s | 1.449 | 0 | **0 %** |

El corte no es un acantilado exacto en el segundo 30 porque el watermark solo
avanza entre micro-lotes: el umbral efectivo es 30 s más el retraso del lote en
curso. Esa banda de transición entre 30 y 60 s es el comportamiento correcto,
no una imprecisión de la medida.

### P2: oscilación de la proporción de bots

| Ventanas | Mínimo | Media | Máximo | Desviación |
|---|---|---|---|---|
| 29 | 30,8 % | 40,8 % | 55,6 % | 5,4 |

La media de 40,8 % coincide con el 41,1 % medido en la fase 0 sobre la fuente
real, pero el rango entre ventanas va de 30,8 % a 55,6 %. Es el argumento
medido de por qué streaming y no batch: la media diaria esconde una variación
de 25 puntos.

### Latencia extremo a extremo — medición contaminada, descartada

Primera medición, con Silver recuperando el histórico de Bronze: p50 de
106,84 s. **No es representativa** y no debe usarse. Se conserva la nota porque
el error es instructivo: lo que medía era la velocidad de recuperación, no la
latencia del pipeline en marcha.

## Ejecución limpia en régimen — 17/08/2026, 18:28-18:37

Volúmenes borrados antes de empezar. Solo fuente real, sin generador. Las tres
capas arrancadas escalonadas (Bronze, Silver 45 s después, Gold a los 2 min)
para que ninguna acumulase cola.

| Tabla | Filas |
|---|---|
| `bronce.cambios` | 19.647 |
| `plata.cambios` | 18.556 |
| `oro.actividad_por_wiki` | 935 |
| `oro.humano_vs_bot` | 14 |
| `oro.paginas_concurrentes` | 85 |

### Latencia extremo a extremo, en régimen

Desde `meta.dt` hasta la fila escrita en Silver, 18.556 filas:

| Mínimo | **p50** | **p95** | p99 | Máximo |
|---|---|---|---|---|
| 1,39 s | **15,59 s** | **55,75 s** | 77,43 s | 82,63 s |

Estas sí son las cifras buenas. El p50 de 15,6 s encaja con el suelo teórico:
dos disparadores encadenados de 10 s, uno en Bronze y otro en Silver, dan una
media esperable en torno a esa cifra. La cola hasta 82 s corresponde a eventos
que llegan justo después de cerrarse un micro-lote y esperan al siguiente en
cada una de las dos capas.

Bajar este número es cuestión de acortar los disparadores, a cambio de más
ficheros pequeños en Iceberg. No se ha tocado: 15 s de latencia es de sobra
para las preguntas de este pipeline.

### Almacenamiento: los puntos de control dominan

| Ruta | Tamaño |
|---|---|
| `bronce` | 7,4 MB |
| `plata` | 3,9 MB |
| `oro` | 2,4 MB |
| **`_checkpoints`** | **180 MB** |

**Los puntos de control ocupan trece veces más que los datos.** No es un error:
Spark conserva por defecto las últimas 100 versiones del estado de cada consulta
(`spark.sql.streaming.minBatchesToRetain`), y aquí hay cinco consultas con
estado, tres de ellas con agregación por ventana.

Consecuencia directa para AWS: dimensionar S3 por el volumen de datos deja fuera
la parte que más ocupa. Antes de la fase 6 conviene bajar `minBatchesToRetain`,
o al menos saber que esos 180 MB por cada ocho minutos de ejecución van a estar
ahí. **Pendiente de ajustar.**

### P2 con datos reales

| Ventanas | Mínimo | Media | Máximo | Desviación |
|---|---|---|---|---|
| 7 | 27,8 % | 30,5 % | 35,2 % | 2,9 |

Media del 30,5 %, frente al 41,1 % que midió la fase 0 en otra franja horaria.
Esa diferencia de diez puntos entre dos sesiones es en sí misma un resultado: la
proporción de bots depende de la hora, y por eso la pregunta se respondió por
ventanas y no con un número único.

Las cifras anteriores de esta sección (29 ventanas, 30,8-55,6 %) están
contaminadas por el generador sintético, que produce un 41 % de bots por
construcción. No usarlas.

### P3 con datos reales

85 filas en 8 minutos. Ejemplos reales detectados: `Friends in Love (Dionne
Warwick album)` en la Wikipedia inglesa con 2 editores y 3 ediciones sostenido
durante cinco ventanas consecutivas, `Deaths in 2026`, `Q141111347` en Wikidata,
y `Wikipedia:Löschkandidaten/17. August 2026` en la alemana.

Que aparezcan páginas de discusión y de mantenimiento tiene sentido: son donde
varias personas coinciden de verdad. Contrasta con la ejecución contaminada,
donde el generador copaba la tabla con títulos inventados.

## Recursos consumidos en local

| Job | Configuración | Observado |
|---|---|---|
| Bronze | `local[2]`, 2 GB | Suficiente. Lotes por debajo del disparador de 10 s |
| Silver | `local[2]`, 2 GB | Suficiente |
| Gold | `local[6]`, 3 GB | Con `local[2]` los lotes tardaban 18-32 s para un disparador de 15 s |

Memoria del contenedor de Gold en marcha: 2,4 GB de los 15,6 disponibles.

## Coste en AWS

**0 €.** No se ha creado ningún recurso todavía.
