# MinIO

## Conceptos

**MinIO** es un almacenamiento de objetos compatible con la API S3: los datos se guardan como objetos dentro de *buckets*.

**Erasure Coding.** En lugar de copias completas, cada objeto se divide en fragmentos de datos y de paridad repartidos entre las unidades de un *erasure set*. Este despliegue usa 4 unidades con 2 de datos y 2 de paridad (`EC:2`):

| | MinIO 2 + 2 | HDFS con replicación ×3 |
|---|---|---|
| Qué se guarda | 2 fragmentos de datos + 2 de paridad | 3 copias completas de cada bloque |
| Sobrecarga aproximada | ≈ 2× | 3× |

Los fragmentos de paridad permiten reconstruir los datos perdidos, pero no son copias de los originales. El reparto depende de cada objeto: una misma unidad guarda datos de unos objetos y paridad de otros. Cada unidad es un volumen o disco del erasure set, no necesariamente un nodo físico.

**Reconstrucción y quórum.** Con 2 + 2, cualquier pareja de fragmentos válidos permite reconstruir el objeto, aunque ambos sean de paridad. Escribir exige 3 unidades.

| Unidades disponibles | Leer objetos existentes | Nueva escritura |
|---|---|---|
| 4 | Sí | Sí |
| 3 (una caída) | Sí | Sí |
| 2 (dos caídas) | Sí: se puede reconstruir | No: la escritura exige 3 |
| 1 (tres caídas) | No | No |

Que MinIO responda por la red no significa que pueda leer o escribir objetos: necesita suficientes unidades.

## El despliegue

```yaml
services:
  minio:
    image: chainguard/minio:latest
    command: server http://minio-minio-{1...4}:9000/data --console-address :9001
    ports:
      - 9000-9003:9000     # API S3: un puerto del anfitrión por réplica
      - 9101-9104:9001     # consola web, igual
    volumes:
      - /data              # volumen anónimo distinto por contenedor
      - ./workspace:/workspace   # carpeta compartida con el anfitrión
    deploy:
      replicas: 4
```

| Elemento | Quién lo interpreta |
|---|---|
| `replicas: 4` | Compose: crea cuatro procesos de servidor, `minio-minio-1` a `minio-minio-4` |
| `{1...4}` | MinIO: enumera los endpoints del despliegue (no crea contenedores) |

Los cuatro procesos reciben la misma lista de endpoints y forman **un único despliegue**, no cuatro instalaciones independientes. La primera réplica no es un líder global: se usa para ejecutar `mc`, el cliente incluido en la imagen. Los nombres `minio-minio-N` dependen de arrancar desde la carpeta `minio/`.

MinIO ya no publica imágenes de su edición comunitaria; `chainguard/minio` la compila Chainguard desde el código fuente de MinIO. Solo ofrece `latest`; para fijar la versión se puede usar su digest, por ejemplo `chainguard/minio@sha256:709099319add48dfb44aa596a7070d3ddd8dd04d7ae881765719aec29df9e510` (MinIO `RELEASE.2026-09-22T19-25-18Z`).

Cada réplica guarda sus datos en un volumen anónimo `/data` (MinIO distribuido no acepta un disco dentro del sistema de ficheros raíz del contenedor). Se conservan con `docker stop`/`start` y `docker compose stop`/`start`; con `docker compose down` se pierden (`down -v` borra además los volúmenes antiguos).

## Arranque y alias de mc

Desde la carpeta `minio/`:

```bash
# En el anfitrión
docker compose up -d
docker compose ps
docker compose exec minio bash

# Dentro de la primera réplica
cd /workspace
mc alias set sdpd http://localhost:9000 minioadmin minioadmin
mc admin info sdpd
```

`mc admin info` debe indicar `4 drives online`. El alias le dice a `mc` a qué servidor y con qué credenciales conectarse; se guarda en el contenedor. **Si el alias no existe, `mc cp fichero sdpd/datos/` no da error: copia a una carpeta local `sdpd/datos/`.** Comprobarlo con `mc alias list`.

En Linux, para que `mc` pueda escribir en `workspace/`, ejecutar una vez en el anfitrión `chmod a+rwx workspace`. Dentro del contenedor no hay `grep` ni `cmp`: para comparar ficheros, usar `sha256sum`.

## Ejemplos

**Subir, listar y recuperar un objeto**, dentro de la primera réplica:

```bash
mc mb --ignore-existing sdpd/datos
mc cp /workspace/data.txt sdpd/datos/data.txt
mc ls sdpd/datos/
mc cat sdpd/datos/data.txt
mc cp sdpd/datos/data.txt /workspace/recuperado-minio.txt
sha256sum /workspace/data.txt /workspace/recuperado-minio.txt
```

`--ignore-existing` evita el error si el bucket ya existe. Un bucket no es un directorio, aunque las tareas se parezcan:

| Tarea | HDFS | MinIO (`mc`) |
|---|---|---|
| Subir | `hdfs dfs -put` | `mc cp local sdpd/…` |
| Listar | `hdfs dfs -ls` | `mc ls` |
| Recuperar | `hdfs dfs -get` | `mc cp sdpd/… local` |

**Consola web:** http://localhost:9101 (cualquiera de 9101–9104), usuario y contraseña `minioadmin`.

**Vista lógica del objeto y estado del despliegue:**

```bash
mc stat sdpd/datos/data.txt     # objeto
mc admin info sdpd              # clúster
```

`mc stat` muestra metadatos del objeto, no su distribución interna. El hash calculado al recuperar verifica el contenido; el ETag no es un hash universal del fichero. Que todos los nodos participen no significa que cada uno tenga una copia completa.

**Detener un nodo: leer y escribir.**

```bash
# En el anfitrión
docker stop minio-minio-4

# Dentro de minio-minio-1
mc cp sdpd/datos/data.txt /workspace/lectura-durante-caida.txt
mc cp /workspace/nuevo.txt sdpd/datos/nuevo.txt
mc admin info sdpd
```

Con 3 unidades se lee y se escribe; `admin info` indica `3 drives online, 1 drive offline`.

**Recuperación y comprobación:**

```bash
# En el anfitrión
docker start minio-minio-4

# Dentro de minio-minio-1
mc admin info sdpd
mc cp sdpd/datos/nuevo.txt /workspace/nuevo-recuperado.txt
sha256sum /workspace/nuevo.txt /workspace/nuevo-recuperado.txt
```

`start` vuelve a asociar el volumen del contenedor existente. Que el proceso esté activo no significa que la redundancia esté restaurada: hay que inspeccionar el estado (*healing*).

## Ejercicio 2

Repetir el ejemplo anterior con dos objetos bajo el prefijo `curso/`: leer y escribir con un nodo detenido, restaurarlo y comprobar el estado.

```bash
# Dentro de minio-minio-1
mc cp /workspace/data.txt sdpd/datos/curso/a.txt
mc cp /workspace/nuevo.txt sdpd/datos/curso/b.txt
mc ls sdpd/datos/curso/

# En el anfitrión
docker stop minio-minio-3
```

- ¿Un prefijo es un directorio HDFS?
- ¿Qué unidad protege el sistema?
- ¿Qué pasa si se detiene el endpoint que usa `mc`?

## Usar MinIO desde Python

MinIO habla la API S3, así que se usa con el mismo cliente que Amazon S3, [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html): solo cambia el `endpoint_url` (para Amazon S3 basta con quitarlo). El ejemplo de la carpeta `mapreduce/` cuenta las palabras de un objeto con MapReduce y guarda el resultado como un objeto nuevo, sin bajar el objeto ni crear ficheros locales.

| Fichero | Qué contiene |
|---|---|
| `MapReduce.py` | La librería: `emit_intermediate` agrupa valores por clave, `emit` guarda un resultado y `execute` aplica el *map* a cada línea y el *reduce* a cada clave |
| `wordcount.py` | El *mapper* (emite `(word, 1)` por cada palabra), el *reducer* (suma las apariciones) y el programa: 1) `get_object` abre `datos/data.txt` como un flujo, 2) `mr.execute` lo procesa línea a línea, 3) `put_object` sube `datos/resultados/conteo.txt` |

Con MinIO arrancado y `data.txt` subido al bucket `datos` (ejemplo anterior):

```bash
# En el anfitrión, fuera de Docker
pip install boto3
cd mapreduce
python wordcount.py
```

El resultado también se imprime por pantalla. Comprobar el objeto nuevo dentro de `minio-minio-1`:

```bash
mc ls sdpd/datos/resultados/
mc cat sdpd/datos/resultados/conteo.txt
```

**Procesar sin descargar.** `get_object` devuelve el contenido como un flujo (`StreamingBody`) que se lee de la red a medida que se pide; `io.TextIOWrapper` lo convierte en texto que se recorre línea a línea. Para que eso sirva, `MapReduce.execute` recorre los datos con `for line in data` en lugar de `data.readlines()`, que cargaría todas las líneas en memoria antes de empezar. Con un fichero local funciona igual.

Con un objeto de 100 MiB:

| | Pico de memoria | Fichero local |
|---|---|---|
| `download_file` + `readlines()` | 414 MB | 100 MiB en disco |
| `get_object` + `for line in data` | 173 MB | ninguno |

Lo que queda en memoria es `intermediate`: una lista con un `1` por cada aparición de cada palabra hasta el *reduce*. Crece con el número total de palabras, así que un fichero suficientemente grande la agota aunque se lea en streaming. Los MapReduce reales (Hadoop, Spark) vuelcan los resultados intermedios a disco y usan *combiners* que suman parcialmente antes del *reduce*.
