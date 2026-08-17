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

## D7 — La clave de particion es `wiki|title`

- **Decidí**: publicar cada evento con clave `"<wiki>|<title>"`.
- **Alternativas**: sin clave (reparto rotatorio); clave `wiki`; clave `meta.id`.
- **Por qué**: reparte bien —hay decenas de miles de títulos distintos por
  minuto, y en la prueba 200 eventos cayeron 74/63/63 en tres particiones— y
  garantiza que todos los cambios de una misma página conservan su orden, que
  es lo que necesita la pregunta P3. Con clave `wiki` el 42 % del tráfico
  caería en una sola partición, porque Commons domina.
- **Cuesta**: el reparto depende del tráfico real; si un solo título se volviera
  muy activo, su partición se calentaría. Con este volumen no es un problema.

## D8 — El productor va en un contenedor, no en el equipo anfitrión

- **Decidí**: el productor se construye desde `Dockerfile` y se levanta con el
  resto del Compose.
- **Alternativas**: ejecutarlo a mano en el anfitrión contra `localhost:19092`.
- **Por qué**: el criterio de fin de fase es que `docker compose up` levante
  todo y los mensajes lleguen al topic. Con el productor fuera, ese criterio
  dependería de un paso manual y de que el Python del anfitrión tuviera las
  dependencias.
- **Cuesta**: reconstruir la imagen en cada cambio del productor. Se mitiga
  copiando `requirements.txt` antes que el código, para reaprovechar la capa.

## D9 — La interfaz expone publicación y lectura, no solo publicación

- **Decidí**: `fuente_eventos` devuelve un par: un `Publicador` (lo usa el
  productor, en Python) y una `LecturaSpark` que entrega formato y opciones de
  `readStream`.
- **Alternativas**: abstraer solo la publicación y dejar que el job de Spark
  configure la lectura por su cuenta.
- **Por qué**: si el job configura la lectura, acaba conteniendo la diferencia
  entre Kafka y Kinesis —que es exactamente el `if entorno == "aws"` que este
  diseño existe para impedir. Con esto, el job pide formato y opciones y no
  sabe dónde está.
- **Cuesta**: una clase más y algo de indirección en la fase 2, antes de que se
  vea para qué sirve.

## D10 — Java 17, no Java 11

- **Decidí**: imagen `apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu`.
- **Alternativas**: quedarse en Java 11 y bajar Iceberg a una versión compilada
  para ese runtime.
- **Por qué**: Iceberg 1.11.0 está compilado con class file 61 (Java 17) y la
  etiqueta corta de Spark trae Java 11 (class file 55): el job muere al crear
  la sesión con `UnsupportedClassVersionError`. Cambiar de runtime es el ajuste
  más pequeño —mismo Spark, misma Scala— y alinea con EMR 7.x, que ya va sobre
  Java 17.
- **Cuesta**: la imagen es más grande. Nada más: Spark 3.5 soporta Java 17.

## D11 — Publicación y lectura se piden por separado

- **Decidí**: `crear_publicador()` y `crear_lectura()` en vez de un único
  `crear()` que devuelva el par, y el import del cliente dentro del constructor.
- **Alternativas**: instalar `confluent-kafka` y `boto3` también en la imagen de
  Spark.
- **Por qué**: el job de Spark solo lee, pero pedir el par construía también el
  publicador y con él `confluent_kafka`, que la imagen de Spark no lleva —
  `ModuleNotFoundError` al arrancar. Las dos mitades viven en imágenes
  distintas, así que acoplarlas obliga a cada una a cargar la dependencia de la
  otra. Corrige un defecto de D9.
- **Cuesta**: dos funciones donde había una, e imports dentro de funciones, que
  no es lo idiomático pero aquí está justificado y comentado.

## D12 — Bronze guarda el evento como texto, sin parsear

- **Decidí**: columna `valor STRING` con el JSON tal cual, más los metadatos del
  sobre (`particion`, `desplazamiento`, `ts_cola`).
- **Alternativas**: parsear el JSON en Bronze con el esquema de la fase 0.
- **Por qué**: si Wikimedia cambia el esquema, Bronze sigue ingiriendo y el
  problema se resuelve en Silver con los datos crudos aún disponibles. Además
  `(particion, desplazamiento)` permite demostrar exactitud: duplicados y huecos
  se calculan de forma exacta, no aproximada.
- **Cuesta**: Silver tiene que parsear, y la tabla ocupa más que si estuviera
  tipada. Medido: 6,4 MB para 25.911 eventos.

## D13 — Deduplicación con `dropDuplicatesWithinWatermark`

- **Decidí**: deduplicar con `dropDuplicatesWithinWatermark(["meta_id"])` sobre
  un watermark de 30 s en `ts_evento`.
- **Alternativas**: `dropDuplicates(["meta_id"])` sin watermark; deduplicar en
  la consulta, no en la escritura.
- **Por qué**: `dropDuplicates` sin watermark guarda todos los identificadores
  vistos **para siempre**: el estado crece sin límite y el job acaba muriendo
  por memoria. La variante con watermark solo recuerda dentro de la ventana.
- **Cuesta**: un duplicado separado más de 30 s de su original no se detecta.
  Aceptable: los duplicados vienen de reconexiones y reprocesos, que ocurren en
  segundos, no en minutos.

## D14 — Gold: una ventana deslizante para P3, fijas para P1 y P2

- **Decidí**: P1 y P2 con ventana fija de 1 minuto; P3 con ventana deslizante
  de 5 minutos avanzando cada minuto.
- **Alternativas**: ventana fija también para P3.
- **Por qué**: "a la vez" es una propiedad de la ventana. Con ventanas fijas,
  dos personas editando la misma página a las 10:00:59 y 10:01:01 caen en
  ventanas distintas y la coincidencia no se ve, que es justo lo que P3 busca
  detectar. La deslizante la captura.
- **Cuesta**: cinco veces más estado y cinco filas por coincidencia en vez de
  una. Con este volumen no importa; con más habría que subir el paso.

## D15 — Los tres jobs de Gold van en un proceso, con seis núcleos

- **Decidí**: las tres consultas en un solo job, lanzado con `local[6]`.
- **Alternativas**: tres jobs separados; dejarlo en `local[2]`.
- **Por qué**: comparten origen y ciclo de vida, y separarlas leería Silver
  tres veces. Con `local[2]` los lotes tardaban 18-32 s para un disparador de
  15 s: tres consultas simultáneas no caben en dos núcleos.
- **Cuesta**: si una consulta falla, se paran las tres. Es deliberado: seguir
  con dos de tres dejaría las tablas incoherentes entre sí.

## D16 — Los ensayos sintéticos no comparten almacén con la evidencia

- **Decidí**: las tandas del generador se ejecutan sobre volúmenes desechables.
  Antes de una ejecución destinada a evidencia o a métricas, `docker compose
  down -v` y empezar limpio, solo con la fuente real.
- **Alternativas**: filtrar los eventos sintéticos por `comment` dentro de los
  jobs de Gold; publicarlos a un topic aparte.
- **Por qué**: en la fase 3 los sintéticos dominaron la tabla de páginas
  concurrentes —400 títulos y 60 usuarios generan más coincidencia que el
  tráfico real— y dejaron esa respuesta sin valor. Filtrar dentro de los jobs
  metería lógica de pruebas en el código de producción, que es justo lo que no
  quiero tener que explicar en una entrevista.
- **Cuesta**: hay que rehacer la ingesta antes de medir o grabar, unos diez
  minutos. Barato comparado con publicar un número contaminado.

## D17 — La interfaz normaliza el sobre, no solo el formato

- **Decidí**: `LecturaSpark` gana `normalizar(df)`, que traduce el sobre propio
  de cada fuente al esquema común `clave, valor, origen, particion,
  desplazamiento, ts_cola`. También gana `desplazamiento_es_consecutivo()`.
- **Alternativas**: dejar que cada job seleccione sus columnas; escribir dos
  versiones del job de Bronze.
- **Por qué**: dar solo formato y opciones no aislaba nada. Kafka devuelve
  `key, value, topic, partition, offset, timestamp` y Kinesis devuelve `data,
  streamName, partitionKey, sequenceNumber, approximateArrivalTimestamp`.
  `bronce.py` hacía `F.col("offset")`: **funcionaba en local y no habría
  arrancado en AWS**. No había ningún `if entorno == "aws"`, pero la diferencia
  de entorno se había filtrado igual, columna a columna. Se descubrió leyendo
  la documentación de EMR antes de escribir Terraform, no pagando.
- **Cuesta**: `desplazamiento` pasa a ser texto —el número de secuencia de
  Kinesis tiene 56 dígitos y no cabe en un BIGINT—, así que el conteo de huecos
  necesita un CAST y solo aplica a Kafka. Corrige un defecto de D9.

## D18 — Las versiones locales bajan a las de EMR

- **Decidí**: Spark 3.5.6 e Iceberg 1.10.0 en local, igualando EMR Serverless
  7.13.0. Antes eran 3.5.9 y 1.11.0.
- **Alternativas**: quedarse en lo más reciente y confiar en que 3.5.x sea
  compatible entre parches.
- **Por qué**: el objetivo del proyecto es que los jobs sean idénticos en los
  dos entornos, y el destino manda sobre el portátil. Hadoop y Scala no cambian
  (3.3.4 y 2.12.18), así que el ajuste toca solo dos piezas.
- **Cuesta**: se renuncia a lo último publicado. A cambio, lo que funciona en
  local es lo que se va a ejecutar en AWS, sin sorpresas de parche.

## D19 — EMR Serverless sin VPC, y por tanto sin NAT Gateway

- **Decidí**: no declarar `network_configuration` en la aplicación de EMR
  Serverless. Sin VPC, sin subredes, sin NAT y sin endpoints.
- **Alternativas**: VPC con NAT Gateway (~32 €/mes más tráfico); VPC con
  endpoints para S3, Glue y Kinesis (tres recursos más, dos de ellos por hora).
- **Por qué**: una aplicación de EMR Serverless solo necesita VPC si accede a
  recursos que viven dentro de una VPC —una base de datos RDS, un ElastiCache—.
  Todo lo que consume este job (S3, Glue, Kinesis) son servicios de AWS
  alcanzables por la red gestionada del propio servicio. La factura de red
  queda en cero.
- **Cuesta**: si algún día el pipeline tuviera que leer de una base de datos
  privada, habría que añadir VPC y entonces sí decidir entre NAT y endpoints.

## D20 — Kinesis aprovisionado con un shard, no bajo demanda

- **Decidí**: `PROVISIONED` con un shard y retención de 24 horas.
- **Alternativas**: `ON_DEMAND`, que escala solo.
- **Por qué**: bajo demanda cobra una cuota base por hora bastante más alta y
  compensa con tráfico irregular o grande. Aquí el caudal está medido y es
  estable: 37,4 ev/s de media, pico de 114, unos 53 KB/s. Un shard admite
  1 MB/s y 1.000 registros por segundo, veinte veces lo necesario.
- **Cuesta**: si el caudal se disparara habría que añadir shards a mano. Con
  esta fuente no va a pasar.

## D21 — El perfil de AWS es obligatorio y sin valor por defecto

- **Decidí**: `var.perfil` sin `default`, con validación de que no esté vacío, y
  `profile = var.perfil` en el proveedor.
- **Alternativas**: dejar que el proveedor use la cadena de credenciales
  habitual, que acaba en el perfil `default`.
- **Por qué**: en este equipo había un `~/.aws/credentials` de noviembre de
  2025, de una certificación anterior, que nadie recordaba haber puesto.
  Cualquier comando de Terraform habría ido a esa cuenta sin avisar. Obligar a
  nombrar el perfil convierte un accidente silencioso en un error explícito.
- **Cuesta**: hay que pasar `-var perfil=...` o tener el `tfvars`. Es
  exactamente la fricción que se busca.

## D22 — Presupuesto con avisos, porque no hay tope duro

- **Decidí**: crear un presupuesto de AWS con tres avisos por correo (50 % y
  90 % del gasto real, y 100 % de la proyección del mes).
- **Alternativas**: confiar en el tope del plan gratuito; no poner nada y
  revisar el panel a mano.
- **Por qué**: el plan gratuito quedó descartado —la cuenta ya había consumido
  esa oferta—, así que se opera en plan de pago y **no existe ningún tope
  duro**. El aviso de proyección es el que de verdad importa: detecta un
  recurso olvidado a los pocos días, no a final de mes. El riesgo del proyecto
  no es la sesión de validación (~1 $) sino el shard de Kinesis olvidado, que
  cuesta unos 11 $ al mes se use o no.
- **Cuesta**: nada, los dos primeros presupuestos de una cuenta son gratis. Y
  hay que asumir su límite: **avisa, no impide**. No sustituye al destroy.
