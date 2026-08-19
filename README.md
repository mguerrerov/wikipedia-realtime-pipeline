# Pipeline en tiempo real sobre los cambios de Wikimedia

Cada edición que ocurre en cualquier wiki de Wikimedia se publica al instante en
un stream público. Este proyecto la recoge, la ordena por capas y responde tres
preguntas que solo tienen sentido en tiempo real, porque la media diaria las
esconde.

El mismo código corre en dos sitios: en un portátil con Docker Compose, y en AWS
sobre Kinesis y EMR Serverless. No hay ningún `if entorno == "aws"` en los jobs.
La única variable que decide el entorno es `FUENTE_EVENTOS`.

> **Estado: en desarrollo.** Las fases 0 a 3 y la 5 están terminadas y medidas.
> La 4 (vitrina visual) y la 6 (ejecución real en AWS) están pendientes. Todas
> las cifras de este documento son mediciones fechadas, no estimaciones: salen de
> [`docs/metrics.md`](docs/metrics.md), y ese es el único sitio del que pueden
> salir. Lo que aún no se ha medido no aparece aquí.

## Las tres preguntas

| | Pregunta | Ventana |
|---|---|---|
| **P1** | Cuánta actividad tiene cada wiki y cada espacio de nombres, minuto a minuto | Fija, 1 min |
| **P2** | Qué proporción de los cambios la hacen personas y cuál bots | Fija, 1 min |
| **P3** | Qué páginas está editando más de una persona **a la vez** | Deslizante, 5 min / paso 1 min |

P2 es la que justifica el proyecto entero. Medida sobre datos reales, la
proporción de bots osciló entre **27,8 % y 35,2 %** en siete ventanas de una
misma ejecución, y la fase 0 había medido un 41,1 % en otra franja horaria. Un
número único al día promedia esa variación hasta hacerla desaparecer.

P3 necesita ventana deslizante y no fija: con ventanas fijas, dos personas
editando la misma página a las 10:00:59 y a las 10:01:01 caen en ventanas
distintas y la coincidencia —justo lo que se busca— no se ve.

## Arquitectura

```
stream SSE de Wikimedia
  37,4 ev/s sostenidos, pico de 114
        |
        v
  productor  --------->  Kafka (Redpanda)     [local]
  clave: wiki|title      Kinesis, 1 shard     [AWS]
        |
        v
  BRONCE   el evento como texto, sin parsear, mas el sobre
           (particion, desplazamiento, ts_cola)
        |
        v
  PLATA    esquema explicito, tipado, deduplicado por meta.id
           watermark de 30 s sobre meta.dt
        |
        v
  ORO      P1  actividad_por_wiki
           P2  humano_vs_bot
           P3  paginas_concurrentes
        |
        v
  Iceberg sobre MinIO [local]  /  S3 + Glue Data Catalog [AWS]
```

Bronze guarda el JSON **sin parsear**, a propósito. Si Wikimedia cambia el
esquema mañana, Bronze sigue ingiriendo y el problema se arregla en Silver con
los datos crudos todavía disponibles. Además, guardar `(particion,
desplazamiento)` permite demostrar que un reinicio no pierde ni duplica: el
método es exacto, no aproximado, porque esos dos campos identifican unívocamente
cada mensaje y los desplazamientos de una partición son consecutivos.

## Cómo levantarlo

Hace falta Docker y unos 4 GB de RAM libres para los contenedores.

```bash
cp .env.example .env
docker compose up -d          # cola, topico, consola, MinIO, bucket y productor
```

Con eso los eventos ya están entrando en el topic. La consola de Redpanda queda
en `localhost:8080` y la de MinIO en `localhost:9001`.

Los jobs de Spark **no arrancan solos**: un job de streaming que se levanta con
el entorno es un job que se queda corriendo sin que nadie se dé cuenta. Se lanzan
a mano, en este orden y dejando margen entre ellos para que ninguno acumule cola:

```bash
docker compose run --rm bronce      # de la cola a Iceberg
docker compose run --rm plata       # tipado y deduplicacion
docker compose run --rm oro         # las tres preguntas
```

Y para mirar lo que ha salido, o comprobar que Bronze no perdió nada:

```bash
docker compose run --rm consultas   # vuelca las tablas de Oro
docker compose run --rm verifica    # cuenta duplicados y huecos en Bronze
```

`DURACION_JOB=120` limita un job a dos minutos en vez de dejarlo indefinido.
Para provocar retrasos y duplicados —que la fuente real no produce— está el
generador sintético: `docker compose run --rm generador`.

**Importante:** los ensayos con el generador no deben compartir almacén con una
ejecución destinada a medir. El generador produce un 41 % de bots por
construcción y contamina P2, y copa la tabla de P3 con títulos inventados.
Antes de medir, `docker compose down -v` y empezar limpio.

### En AWS

`terraform/` describe el equivalente completo: Kinesis, S3, Glue, EMR Serverless,
un usuario de despliegue con política acotada y un presupuesto con avisos. Está
**validado pero sin aplicar**, y por tanto el coste real hasta hoy es de 0 €.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # y rellenarlo
terraform init && terraform plan
```

El perfil de AWS es obligatorio y no tiene valor por defecto, a propósito: así
ningún comando puede acabar apuntando a la cuenta que el CLI tuviera configurada
por casualidad. Ver [D21](docs/decisiones.md).

## Cifras medidas

Todas del 17 y 18 de agosto de 2026, sobre Windows 11 con 16 núcleos y 15,6 GB
disponibles para Docker. El detalle completo, con el comando que produjo cada
número, está en [`docs/metrics.md`](docs/metrics.md).

**La fuente**, sobre una captura de 10 minutos:

| Métrica | Valor |
|---|---|
| Ritmo sostenido | 37,4 ev/s |
| Pico en 1 s | 114 ev/s |
| Tamaño medio del evento | 1.423 B |
| Caudal | 53,2 KB/s |
| Desorden sobre `meta.dt` | 1,28 %, máximo 0,99 s |
| Reconexiones en 10 min | 0 |

**Exactitud**, sobre dos ejecuciones de Bronze con parada y arranque entre medias:

| Métrica | Valor |
|---|---|
| Filas ingeridas | 25.911 |
| Duplicados | **0** |
| Huecos | **0** |

Y en Silver, con duplicados inyectados a propósito por el generador: 1.004 en una
tanda y 636 en otra, **0 supervivientes** sobre 71.484 filas.

**Latencia extremo a extremo**, de `meta.dt` a la fila escrita en Silver, medida
sobre 18.556 filas de una ejecución limpia:

| Mínimo | p50 | p95 | p99 | Máximo |
|---|---|---|---|---|
| 1,39 s | **15,59 s** | **55,75 s** | 77,43 s | 82,63 s |

El p50 encaja con el suelo teórico: dos disparadores encadenados de 10 s, uno en
Bronze y otro en Silver. Bajarlo es cuestión de acortar los disparadores, a
cambio de más ficheros pequeños en Iceberg. No se ha tocado.

### El hallazgo que no esperaba

| Ruta | Tamaño |
|---|---|
| `bronce` | 7,4 MB |
| `plata` | 3,9 MB |
| `oro` | 2,4 MB |
| **`_checkpoints`** | **180 MB** |

**Los puntos de control ocupan trece veces más que los datos.** No es un fallo:
Spark conserva por defecto las últimas 100 versiones del estado de cada consulta,
y aquí hay cinco consultas con estado, tres de ellas con agregación por ventana.

La consecuencia es directa y va contra la intuición: dimensionar S3 por el
volumen de datos deja fuera justo la parte que más ocupa. Queda pendiente bajar
`spark.sql.streaming.minBatchesToRetain` antes de la fase 6.

### Validación del watermark

Con el generador inyectando eventos retrasados a propósito, supervivencia hasta
Silver según el retraso del evento:

| Retraso | Supervivencia |
|---|---|
| < 5 s | 100 % |
| 5-15 s | 100 % |
| 15-30 s | 100 % |
| 30-45 s | 93,6 % |
| 45-60 s | 10,1 % |
| > 60 s | **0 %** |

El corte no es un acantilado exacto en el segundo 30 porque el watermark solo
avanza entre micro-lotes: el umbral efectivo es 30 s más el retraso del lote en
curso. Esa banda de transición es el comportamiento correcto, no imprecisión de
la medida.

## Decisiones

Las veintidós decisiones del proyecto están en
[`docs/decisiones.md`](docs/decisiones.md), cada una con sus alternativas, el
motivo y lo que cuesta. Las cuatro que más cambiaron el resultado:

**El tiempo de evento es `meta.dt`, no `timestamp`.** `timestamp` es el que
intuitivamente parece correcto, y está mal: llega fuera de orden el 62,35 % de
las veces, con un retraso máximo de **19,3 años** —eventos `log` de Commons donde
`timestamp` guarda la fecha original del fichero—. `meta.dt` va desordenado el
1,28 %, con un máximo por debajo del segundo.

**La abstracción de la fuente normaliza el sobre, no solo el formato.** La
primera versión daba a Spark el formato y las opciones de lectura, y parecía
suficiente. No lo era: Kafka devuelve `offset` y Kinesis devuelve
`sequenceNumber`, y `bronce.py` hacía `F.col("offset")`. **Funcionaba en local y
no habría arrancado en AWS.** No había ningún `if entorno == "aws"`, pero la
diferencia de entorno se había filtrado igual, columna a columna. Se descubrió
leyendo la documentación de EMR antes de escribir Terraform, no pagando.

**Que una JAR exista para tu Spark y tu Scala no significa que funcione con tu
Java.** Iceberg 1.10.0 está compilado con class file 61 (Java 17) y las etiquetas
cortas de `apache/spark` traen Java 11: el job muere al crear la sesión con
`UnsupportedClassVersionError`. Son tres ejes, no dos.

**Manda EMR, no el portátil.** Las versiones locales se bajaron de Spark 3.5.9 e
Iceberg 1.11.0 a **3.5.6 y 1.10.0** para igualar EMR Serverless 7.13.0. Es más
fácil bajar una versión en el portátil que discutir con AWS, y el objetivo es que
los jobs sean idénticos en los dos entornos. Toda la cadena —Spark, Java, Scala,
Hadoop, Iceberg, SDK— está fijada y verificada contra los POM publicados en
[`docs/versiones.md`](docs/versiones.md). Prohibido `latest` y prohibido rango
abierto.

## Coste

**0 € hasta la fecha:** no se ha creado ningún recurso en AWS todavía.

La estimación de la sesión de validación es de ~1 $, y el presupuesto del
proyecto entero es de menos de 15 €. Pero el riesgo no es la sesión: es el shard
de Kinesis, que **se cobra siempre, haya tráfico o no**, unos 11 $ al mes. Un
olvido de seis semanas se come el presupuesto entero.

Por eso `terraform/presupuesto.tf` crea tres avisos por correo, y el más útil de
los tres es el de proyección del mes: detecta un recurso olvidado a los pocos
días en vez de a final de mes. Un presupuesto **avisa, no impide**; no sustituye
al `destroy`. El desglose está en [`docs/coste-aws.md`](docs/coste-aws.md).

## Mapa del repositorio

```
src/fuente_eventos/   la abstraccion Kafka | Kinesis. El unico sitio que
                      sabe en que entorno corre
src/jobs/             bronce, plata, oro, y las utilidades de verificacion
src/productor.py      lee el SSE y publica con clave wiki|title
src/generador.py      eventos sinteticos con retrasos y duplicados
scripts/              captura y analisis de la fase 0, solo libreria estandar
terraform/            el equivalente en AWS, validado sin aplicar
docs/                 exploracion, decisiones, versiones, metricas y coste
```

## Estado por fases

| Fase | Qué | Estado |
|---|---|---|
| 0 | Reconocimiento de la fuente | Terminada |
| 1 | Compose mínimo y abstracción de la fuente | Terminada |
| 2 | Bronze, de la cola a Iceberg | Terminada |
| 3 | Silver y Gold, watermark validado | Terminada |
| 4 | Vitrina visual | Pendiente |
| 5 | Terraform del equivalente en AWS | Validado sin aplicar |
| 6 | Ejecución real en AWS y lista de destrucción | Pendiente |
