# Guion de evidencia — fase 4

El repositorio no va a estar desplegado. Quien lo revise verá capturas y un
vídeo, y nada más. Este documento es el guion exacto para producirlos, para que
grabarlos sea mecánico y repetible en vez de una sesión de improvisación.

Todo lo de aquí se hace **en local**. Coste: 0 €.

## Antes de grabar

1. Ventana de terminal ancha y con tipografía grande. Las tablas de salida
   están calculadas para 78 columnas: si la ventana es más estrecha, se parten
   y la captura no vale.
2. Cerrar lo que no sea del proyecto. En las capturas del navegador no debe
   verse ni una pestaña personal ni la barra de marcadores.
3. Empezar limpio y dejar el pipeline en marcha:

```bash
docker compose down -v
docker compose up -d
DURACION_JOB=960 docker compose run -d --name job-bronce bronce
# esperar ~100 s
DURACION_JOB=840 docker compose run -d --name job-plata  plata
# esperar ~120 s
DURACION_JOB=660 docker compose run -d --name job-oro    oro
```

El escalonado no es capricho: Silver no tiene nada que leer hasta que Bronze ha
escrito su primer micro-lote, y Gold necesita que Silver haya avanzado el
watermark. Las duraciones están calculadas para que los tres terminen a la vez.

4. **Dejar correr al menos diez minutos antes de tocar nada.** Las cifras de
   latencia de los primeros minutos miden el recuperado del histórico, no el
   régimen estacionario. Es el error que ya se cometió una vez (ver
   `docs/sesiones/`), y se nota porque el p50 sale disparado.

## Capturas

En este orden. El orden cuenta la historia: primero que hay datos moviéndose,
después que llegan bien, y al final qué se puede preguntar.

| # | Qué | Cómo se saca | Qué tiene que verse |
|---|---|---|---|
| 1 | El entorno en pie | `docker compose ps` | Los cinco servicios y el estado `healthy` |
| 2 | Eventos en la cola | http://localhost:8080 → topic `wikimedia.cambios` | Contador subiendo y las tres particiones |
| 3 | Un evento crudo | Misma consola, un mensaje desplegado | `meta.dt`, `meta.id` y un título en no latino |
| 4 | El job trabajando | `docker logs -f job-bronce` | Una línea de micro-lote con filas y duración |
| 5 | Las tablas en objetos | http://localhost:9001 → `almacen/warehouse` | Los tres esquemas: `bronce`, `plata`, `oro` |
| 6 | Ni pérdida ni duplicado | `docker compose run --rm verifica` | **0 duplicados, 0 huecos** y el recuento |
| 7 | El watermark de verdad | `docker compose run --rm tardios` | La tabla de supervivencia por tramo de retraso |
| 8 | Las preguntas, desde Spark | `docker compose run --rm consultas` | P1, P2 y P3 con la latencia p50/p95 |
| 9 | Las mismas, desde DuckDB | `docker compose run --rm consumo` | Los mismos números, otro motor |
| 10 | La arquitectura | `docs/arquitectura.md` en GitHub | Los dos diagramas renderizados |

Las capturas 8 y 9 son **la pareja que importa** y conviene ponerlas juntas en
el README: son la demostración de que la tabla Iceberg es el contrato y de que
el motor que escribe no es dueño de los datos. Sin las dos al lado, el argumento
no se ve.

La 6 y la 7 son el contenido técnico defendible en una entrevista: una prueba de
exactitud y una medición del watermark, no una afirmación.

## Vídeo corto — 90 segundos

Sin voz. Rótulos de texto sobre la imagen, que se leen igual con el sonido
apagado y no envejecen si se rehace la grabación.

| Plano | Duración | Qué se graba | Rótulo |
|---|---|---|---|
| 1 | 0:00–0:10 | El diagrama de arquitectura, quieto | «Wikimedia → Kafka → Spark → Iceberg» |
| 2 | 0:10–0:25 | Consola de Redpanda, contador subiendo, en vivo | «~37 eventos/s de la Wikipedia real» |
| 3 | 0:25–0:40 | Logs de Bronze, dos o tres micro-lotes seguidos | «Structured Streaming, checkpoint en cada lote» |
| 4 | 0:40–0:50 | MinIO, entrando de `warehouse` a `oro` | «Tres capas en tablas Iceberg» |
| 5 | 0:50–1:05 | Salida de `verifica`, con el cero en pantalla | «Reinicio en mitad: 0 perdidos, 0 duplicados» |
| 6 | 1:05–1:20 | Salida de `consumo`, hasta P2 | «Mismas tablas, otro motor: DuckDB» |
| 7 | 1:20–1:30 | El `terraform plan` del repositorio, quieto | «El mismo pipeline en AWS. Sin aplicar: 0 €» |

Notas de grabación:

- Los planos 2 y 3 tienen que ser **movimiento real**, no una imagen fija. Es lo
  único del vídeo que no se puede transmitir con una captura, así que es lo que
  justifica que haya vídeo.
- El plano 5 pide un reinicio de verdad para ser honesto: parar `job-bronce`
  con `docker stop`, volver a lanzarlo, y entonces pasar `verifica`. Si no se
  reinicia nada, el cero no demuestra nada.
- Ni una credencial en pantalla. `minioadmin` es de juguete y está en el
  repositorio, pero acostumbrarse a mirar antes de grabar sale gratis.

## Al terminar

```bash
docker stop job-bronce job-plata job-oro 2>/dev/null
docker compose down
```

Los volúmenes sobreviven a `down`; se borran con `down -v`. No dejar jobs de
streaming corriendo después de grabar.
