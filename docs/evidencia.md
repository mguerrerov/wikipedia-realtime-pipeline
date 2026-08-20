# Guion de evidencia — fase 4

El repositorio no va a estar desplegado. Quien lo revise verá capturas y un
vídeo, y nada más.

Este documento está escrito para seguirse sin conocer las interfaces que
aparecen. Cada paso dice dónde entrar, qué mirar y cómo saber si ha salido bien.
Todo es local: coste 0 €.

---

## 1. Las cuatro ventanas que vas a usar

Antes de nada, qué es cada cosa. Solo hay cuatro sitios donde mirar.

| Ventana | Dónde | Qué es | Para qué la usas aquí |
|---|---|---|---|
| **Terminal** | Tu consola, en la carpeta del proyecto | Donde lanzas todo | Lanzar el pipeline y sacar las tablas de resultados |
| **Consola de Redpanda** | http://localhost:8080 | Un panel web para ver la cola de mensajes | Ver los eventos entrando en tiempo real |
| **Consola de MinIO** | http://localhost:9001 | Un explorador de archivos web, el S3 de mentira | Ver que las tablas existen como ficheros |
| **Grabador de pantalla** | Tecla `Windows` + `G` | La barra de juego de Windows 11, que graba vídeo | El vídeo de 90 s |

La consola de Redpanda **no pide contraseña**. La de MinIO sí: usuario
`minioadmin`, contraseña `minioadmin`. Son credenciales de juguete que ya están
en el repositorio, no pasa nada porque salgan en una captura.

Las dos consolas están **en inglés**. Abajo digo el nombre en inglés de cada
sitio donde hay que pinchar.

---

## 2. Preparación

### 2.1 Deja la pantalla presentable

- Ventana de terminal **ancha y con letra grande**. Las tablas de resultados
  están calculadas para 78 columnas: si la ventana es más estrecha, las filas se
  parten por la mitad y la captura no vale. Compruébalo antes con cualquier
  comando: si las líneas de `=====` no caben enteras, ensancha.
- Cierra pestañas y ventanas que no sean del proyecto. En las capturas del
  navegador no debe verse tu barra de marcadores.

### 2.2 Arranca el pipeline

En este orden, en la carpeta del proyecto. **Los comandos van en PowerShell**,
que es la consola que abre Windows por defecto:

```powershell
docker compose down -v
docker compose up -d
$env:DURACION_JOB="960"; docker compose run -d --name job-bronce bronce
```

Ahora **espera unos 100 segundos** y lanza:

```powershell
$env:DURACION_JOB="840"; docker compose run -d --name job-plata plata
```

Espera **otros 120 segundos** y lanza:

```powershell
$env:DURACION_JOB="660"; docker compose run -d --name job-oro oro
```

Si usas Git Bash en vez de PowerShell, la variable va delante del comando y sin
`$env:`: `DURACION_JOB=960 docker compose run ...`. Las dos formas no son
intercambiables, y confundirlas da un error de «no se reconoce como nombre de un
cmdlet».

Por qué la espera y no los tres de golpe: Silver no tiene nada que leer hasta
que Bronze ha escrito su primer bloque, y Gold necesita que Silver lleve un rato
en marcha. Las duraciones (960, 840 y 660 segundos) están calculadas para que
los tres **terminen a la vez**, unos 16 minutos después del primero.

### 2.3 Comprueba que va bien antes de seguir

```powershell
docker ps
```

Tienen que aparecer siete contenedores: `redpanda`, `minio`, `productor`,
`consola-redpanda`, `job-bronce`, `job-plata` y `job-oro`.

Si falta alguno de los `job-`, mira por qué con `docker logs job-plata` (o el
que falte) y no sigas hasta que estén los tres.

### 2.4 Espera diez minutos antes de tocar nada

Esto no es opcional y es lo más fácil de saltarse.

Los primeros minutos el pipeline está poniéndose al día con lo que ya había en
la cola, así que las cifras de latencia salen infladas y no reflejan cómo va el
sistema en marcha. Ya se cometió ese error una vez (está en `docs/sesiones/`) y
se nota porque el p50 sale disparado.

Aprovecha esos diez minutos para hacer la captura 2 y la 3, que sí se pueden
hacer desde el principio.

---

## 3. Las capturas

Diez, en este orden. El orden cuenta una historia: **primero que hay datos
moviéndose, luego que llegan bien, y al final qué puedes preguntarles.**

Guárdalas numeradas (`01-entorno.png`, `02-cola.png`...) para que el orden no se
pierda.

### Captura 1 — El entorno en pie

En la terminal:

```powershell
docker compose ps
```

**La captura buena** es la que muestra la lista de servicios con la columna de
estado en `running` y, en los que lo tengan, `healthy`.

### Captura 2 — Los eventos entrando en la cola

1. Abre http://localhost:8080
2. En el menú de la izquierda pincha en **Topics**
3. Pincha en el topic `wikimedia.cambios`

**La captura buena** muestra el número de mensajes y que hay **tres particiones**.
Refresca un par de veces: el contador tiene que subir. Si no sube, el productor
no está publicando — mira `docker logs productor`.

### Captura 3 — Un evento por dentro

1. Sigues en el topic `wikimedia.cambios`
2. Entra en la pestaña de mensajes (**Messages**)
3. Despliega un mensaje cualquiera pinchando en la flecha de su izquierda

**La captura buena** deja ver los campos `meta.dt` y `meta.id`, que son los dos
que sostienen todo el diseño: `meta.dt` es el tiempo de evento y `meta.id` la
clave de deduplicación.

Si puedes, busca un mensaje cuyo título esté en un alfabeto no latino (ruso,
árabe, japonés). Demuestra que el UTF-8 sobrevive de punta a punta, y salta a la
vista sin explicar nada.

### Captura 4 — El job trabajando

**No la saques del log.** El job llama a `setLogLevel("WARN")` nada más
arrancar, así que después de las dos primeras líneas nuestras —`Leyendo con
formato kafka` y `Job en marcha`— el log se queda mudo hasta el final. No hay
una línea por lote que capturar: eso solo aparece al terminar, como
`Ultimo lote: N filas, M filas/s`.

Lo que sí se mueve son las instantáneas de Iceberg, una por micro-lote:

1. Abre http://localhost:9001 y entra con `minioadmin` / `minioadmin`
2. **Object Browser** → `almacen` → `warehouse` → `bronce` → `cambios` →
   `metadata`
3. Refresca cada 10-15 segundos

Van apareciendo ficheros `snap-....avro`, uno por cada bloque confirmado.

**La captura buena** es esa lista con las marcas de tiempo seguidas, separadas
unos 10 segundos entre sí: es el disparador del job, visible.

*(Medido en una ejecución real: 68 instantáneas y 71 veinticinco segundos
después.)*

Si prefieres una captura de terminal, espera a que el job agote su duración y
saca la línea final con el recuento. Llega al final, no durante.

### Captura 5 — Las tablas, ya en el almacén

1. Abre http://localhost:9001
2. Entra con `minioadmin` / `minioadmin`
3. Menú de la izquierda: **Object Browser**
4. Entra en el bucket `almacen`, y dentro en la carpeta `warehouse`

**La captura buena** enseña las tres carpetas juntas: `bronce`, `plata` y `oro`.
Son las tres capas del pipeline. Si te apetece, entra en `plata/cambios` y saca
otra de las carpetas `data` y `metadata`: así se ve que una tabla Iceberg es
literalmente ficheros.

*(Verás también `_checkpoints`. Es normal: son los puntos de control de Spark.
Ocupa mucho más que los datos, y eso está explicado en `docs/metrics.md`.)*

### Captura 6 — Ni un dato perdido ni uno duplicado

**Antes de esta captura, reinicia Bronze de verdad**, o el resultado no
demuestra nada:

```powershell
docker stop job-bronce
docker start job-bronce
```

Espera un minuto y lanza:

```powershell
docker compose run --rm verifica
```

**La captura buena** es el final de la salida: los ceros de `DUPLICADOS` y
`HUECOS TOTALES`, y la línea `VEREDICTO: CORRECTO`.

Esta es la captura técnicamente más valiosa de todas. Dice que el pipeline
sobrevive a que lo maten a mitad de faena sin perder ni repetir un solo evento,
y lo dice con una comprobación exacta, no con una promesa.

### Captura 7 — El watermark, medido

```powershell
docker compose run --rm tardios
```

**La captura buena** es la tabla de supervivencia: qué porcentaje de eventos
tardíos entra según cuánto se hayan retrasado. Es la justificación medida de por
qué el watermark son 30 segundos y no un número puesto a ojo.

### Captura 8 — Las preguntas, desde Spark

```powershell
docker compose run --rm consultas
```

Sale bastante texto. Interesan dos trozos, y puedes hacer dos capturas:

- El bloque de **latencia**, con p50 y p95
- El bloque de **P2**, la proporción de bots por minuto

### Captura 9 — Las mismas preguntas, desde DuckDB

```powershell
docker compose run --rm consumo
```

**La captura buena** enseña los mismos bloques que la 8, para que se vean los
mismos números.

### Captura 10 — La arquitectura

Abre `docs/arquitectura.md` en GitHub, ya subido, y captura los dos diagramas.
Tienen que verse dibujados, no como código: si ves texto suelto que empieza por
`flowchart`, es que estás mirando el fichero en crudo y no la vista renderizada.

---

## 4. La pareja que importa

**Pon las capturas 8 y 9 juntas en el README, una al lado de la otra.**

Por separado son dos listados de números y no dicen gran cosa. Juntas dicen algo
que se defiende en una entrevista: **las escribió Spark, las lee DuckDB, y
DuckDB no conoce el catálogo ni participó en la escritura**. Abre las tablas por
su ruta y ya está. No hay exportación, ni copia, ni proceso intermedio.

Eso es lo que significa que el formato de tabla sea abierto, y es la razón de
haber elegido Iceberg. Sin las dos capturas al lado, el argumento no se ve.

---

## 5. El vídeo — 90 segundos

### Cómo grabar en Windows 11

Pulsa `Windows` + `G` y se abre la barra de juego. En el panel de captura
(el del icono de la cámara) está el botón de grabar; el atajo directo es
`Windows` + `Alt` + `R`. Los vídeos acaban en `Vídeos\Capturas`.

Graba **la ventana**, no la pantalla entera, para que no salgan tu barra de
tareas ni tus notificaciones.

### Sin voz

Nada de narración. Rótulos de texto sobre la imagen: se leen igual con el sonido
apagado —que es como se ve casi todo— y no hay que regrabar audio si cambias
algo.

### Los siete planos

| Plano | Duración | Qué se graba | Rótulo que pones encima |
|---|---|---|---|
| 1 | 0:00–0:10 | El diagrama de arquitectura, quieto | «Wikimedia → Kafka → Spark → Iceberg» |
| 2 | 0:10–0:25 | Consola de Redpanda, el contador subiendo **en vivo** | «~37 eventos/s de la Wikipedia real» |
| 3 | 0:25–0:40 | MinIO en `bronce/cambios/metadata`, refrescando: aparecen instantáneas | «Un micro-lote cada 10 s, con punto de control» |
| 4 | 0:40–0:50 | MinIO, entrando de `warehouse` a `oro` | «Tres capas en tablas Iceberg» |
| 5 | 0:50–1:05 | Salida de `verifica`, con los ceros en pantalla | «Reinicio a mitad: 0 perdidos, 0 duplicados» |
| 6 | 1:05–1:20 | Salida de `consumo`, hasta P2 | «Mismas tablas, otro motor: DuckDB» |
| 7 | 1:20–1:30 | El `terraform plan` del repositorio, quieto | «El mismo pipeline en AWS. Sin aplicar: 0 €» |

### Tres avisos

1. **Los planos 2 y 3 tienen que ser movimiento de verdad**, no una imagen fija.
   Son lo único del vídeo que una captura no puede transmitir, así que son los
   que justifican que haya vídeo.
2. **El pipeline tiene que estar corriendo mientras grabas.** Los datos que
   quedan en los volúmenes valen para las capturas, pero para el vídeo hay que
   volver a lanzar la sección 2.
3. Mira la pantalla antes de darle a grabar. Aquí las credenciales son de
   juguete, pero el hábito sale gratis.

---

## 6. Al terminar

```powershell
docker stop job-bronce job-plata job-oro
docker compose down
Remove-Item Env:DURACION_JOB
```

Esa última línea borra la variable de duración. En PowerShell `$env:` dura toda
la sesión de la ventana, al contrario que el prefijo de Git Bash, que solo vale
para el comando que lo lleva. Si no la borras, el siguiente `docker compose run`
que lances en esa misma ventana heredará el tiempo sin que te lo esperes.

`down` para los contenedores pero **conserva los datos**. Si quieres empezar de
cero otra vez, `docker compose down -v`.

No dejes jobs de streaming corriendo después de grabar.
