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

### Latencia extremo a extremo

Desde `meta.dt` hasta la fila escrita en Silver, últimos 15 minutos de datos:

| Filas | Mínimo | p50 | p95 | p99 | Máximo |
|---|---|---|---|---|---|
| 43.854 | 3,01 s | 106,84 s | 265,81 s | 289,01 s | 344,86 s |

**Esta medición no es representativa del régimen estacionario y no debe ir al
README tal cual.** Durante el ensayo, Silver estuvo casi todo el tiempo
poniéndose al día con el histórico acumulado en Bronze, así que lo que mide es
la velocidad de recuperación, no la latencia en marcha. El mínimo de 3,01 s se
acerca más al suelo real; el suelo teórico son los dos disparadores encadenados
(10 s en Bronze más 10 s en Silver).

**Pendiente**: repetir con las tablas al día y ambos jobs corriendo en régimen,
sin cola acumulada. Tarea de la fase 4.

## Recursos consumidos en local

| Job | Configuración | Observado |
|---|---|---|
| Bronze | `local[2]`, 2 GB | Suficiente. Lotes por debajo del disparador de 10 s |
| Silver | `local[2]`, 2 GB | Suficiente |
| Gold | `local[6]`, 3 GB | Con `local[2]` los lotes tardaban 18-32 s para un disparador de 15 s |

Memoria del contenedor de Gold en marcha: 2,4 GB de los 15,6 disponibles.

## Coste en AWS

**0 €.** No se ha creado ningún recurso todavía.
