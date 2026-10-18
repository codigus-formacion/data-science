# HDFS

## Conceptos

**HDFS** es el sistema de ficheros distribuido de Hadoop, inspirado en GFS. Está pensado para datasets grandes leídos de forma secuencial con mucho caudal agregado: reparte los datos entre varios nodos y permite acercar el cómputo a los datos.

| Carga | Encaje | Razón |
|---|---|---|
| Ficheros grandes leídos de principio a fin (logs) | Bueno | Lectura y escritura secuenciales con alto caudal |
| Añadir datos al final de un fichero | Posible | Hay `append` y `truncate`; un único escritor por fichero |
| Muchos ficheros pequeños | Malo | Cada fichero y bloque consume metadatos en el NameNode |
| Actualizaciones aleatorias de registros | Malo | No se puede modificar en mitad de un fichero |
| Consultas de baja latencia | Malo | Optimizado para caudal, no para latencia |

**Bloques.** Un fichero se divide en bloques, la unidad de distribución y de replicación. El tamaño habitual es 128 MiB y el último bloque puede estar incompleto: un fichero de 300 MiB ocupa 3 bloques de 128, 128 y 44 MiB. Un fichero pequeño no reserva 128 MiB en disco.

**Replicación e integridad.** Cada bloque se copia en varios DataNodes (factor habitual: 3). Con factor 3, 300 MiB son 3 bloques lógicos, 9 réplicas y unos 900 MiB consumidos. El NameNode detecta los fallos por *heartbeats*, conoce el inventario por *block reports* y, si faltan copias, ordena crear réplicas nuevas en otros nodos. Los *checksums* detectan datos corruptos: replicar protege de perder un nodo; el checksum, de leer datos dañados.

**Racks.** Con factor 3 la política habitual pone la 1.ª copia en el nodo del escritor y la 2.ª y la 3.ª en dos nodos de otro rack: equilibra el tráfico entre racks y la resistencia a la caída de un rack. En nuestro Docker no hay racks.

**NameNode y DataNodes.**

| Componente | Responsabilidad |
|---|---|
| NameNode | Espacio de nombres, asignación de bloques y seguimiento de réplicas |
| DataNode | Almacenar y servir bloques, reportar su estado |
| Cliente | Pide metadatos al NameNode y transfiere los datos directamente con los DataNodes |

Los datos no pasan por el NameNode. Sus metadatos están en memoria y se persisten en **FsImage** (checkpoint) y **EditLog** (cambios posteriores); las ubicaciones de las réplicas se reconstruyen con los block reports. El **SecondaryNameNode** fusiona FsImage y EditLog periódicamente, pero no sustituye al NameNode si cae.

**Lectura y escritura.** Para leer, el cliente pide al NameNode las ubicaciones y lee cada bloque de una réplica; si una réplica falla o su checksum no es válido, lee de otra. Seguir leyendo no significa que ya exista una copia nueva: restaurar el factor lo hace el NameNode más tarde. Para escribir, el cliente envía paquetes por un *pipeline* de DataNodes y recibe ACK; si un DataNode falla, el pipeline se reorganiza y se reenvían los paquetes pendientes.

**Alta disponibilidad y federación.** La HA usa un NameNode activo y otro *standby*, JournalNodes (ediciones compartidas por quórum), ZooKeeper y ZKFC (conmutación automática), y *fencing* para que no haya dos activos. La federación reparte el espacio de nombres entre varios NameNodes independientes que comparten los DataNodes. Este despliegue no incluye ninguna de las dos.

| Decisión de diseño | Beneficio | Coste |
|---|---|---|
| Metadatos en memoria del NameNode | Operaciones de namespace rápidas | Muchos ficheros pequeños consumen metadatos |
| Un único NameNode | Arquitectura sencilla | Condiciona la disponibilidad |
| Replicación ×3 | Tolera la pérdida de nodos | Triplica la capacidad consumida |
| Datos repartidos | Caudal agregado | Depende de red y discos |

## El despliegue

```yaml
services:
  namenode:
    image: apache/hadoop:3.5.0
    hostname: namenode            # nombre en la red interna
    command: [hdfs, namenode]     # proceso que arranca
    ports:
      - 9870:9870                 # web publicada en el anfitrión
    env_file: ./config            # propiedades HDFS
    environment:
      ENSURE_NAMENODE_DIR: /tmp/hdfs/name   # formatea los metadatos la primera vez
    volumes:
      - ./workspace:/workspace    # carpeta compartida para intercambiar ficheros

  datanode:
    image: apache/hadoop:3.5.0
    command: [hdfs, datanode]
    env_file: ./config
    deploy:
      replicas: 4                 # 4 procesos DataNode
```

Son 5 contenedores: `hdfs-namenode-1` y `hdfs-datanode-1` a `hdfs-datanode-4` (al arrancar desde la carpeta `hdfs/`). La imagen incluye Java 17 y las herramientas; el comando decide qué proceso arranca, y `docker compose exec` lanza otro proceso dentro de un contenedor activo.

`deploy.replicas: 4` son 4 procesos; `dfs.replication=3` son 3 copias por bloque. Cada bloque está en 3 de los 4 DataNodes.

El fichero `config` se convierte en `core-site.xml` y `hdfs-site.xml` al arrancar:

| Propiedad | Significado |
|---|---|
| `fs.defaultFS=hdfs://namenode:8020` | Hacia dónde van las rutas HDFS |
| `dfs.namenode.name.dir`, `dfs.datanode.data.dir` | Directorios internos en `/tmp/hdfs` |
| `dfs.replication=3` | Factor de replicación |
| `dfs.namenode.heartbeat.recheck-interval=10000` | Detección de nodos caídos: 2 × 10 s + 10 × 3 s ≈ 50 s |

El bloque de 128 MiB es el valor por defecto y no aparece en `config`.

**Red y puertos.** Solo se publica la web del NameNode, http://localhost:9870. La CLI usa `hdfs://namenode:8020` dentro de la red de Compose y los datos van directos a los DataNodes, cuyos puertos no se publican: una descarga desde la web redirigida a un DataNode puede fallar desde el anfitrión. Dentro de un contenedor, `localhost` es ese mismo contenedor.

**Qué se conserva.**

| Operación | Efecto |
|---|---|
| `docker compose stop` / `start` | Mismos contenedores: se conservan metadatos y bloques |
| `docker compose down` + `up` | Contenedores nuevos: se pierde el contenido de HDFS; `./workspace` sigue |

No hay volúmenes: conservar `workspace/data.txt` no conserva el fichero guardado dentro de HDFS.

## Arranque

Desde la carpeta `hdfs/`:

```bash
# En el anfitrión
docker compose up -d
docker compose ps
docker compose exec namenode bash

# Dentro del NameNode
hdfs dfsadmin -report
```

La primera vez puede tardar hasta medio minuto; hasta entonces los comandos responden `Connection refused`. En Linux, para poder descargar ficheros a `workspace/`, ejecutar una vez en el anfitrión `chmod a+rwx workspace`.

## Ejemplos

**Rutas.** `/workspace/data.txt` es una ruta local del contenedor (compartida con el anfitrión); `/data/data.txt` es una ruta HDFS (`hdfs://namenode:8020/data/data.txt`). `-put` va de local a HDFS y `-get`, de HDFS a local.

**Subir y leer un fichero** (menor que un bloque), dentro del NameNode:

```bash
hdfs dfs -mkdir -p /data
hdfs dfs -put /workspace/data.txt /data/
hdfs dfs -ls /data/
hdfs dfs -cat /data/data.txt
```

En la web, *Utilities → Browse the file system* muestra el tamaño y el factor de replicación y, al pulsar el fichero, sus bloques y los DataNodes que los alojan. La web muestra cada DataNode por el ID de su contenedor; `docker ps` los relaciona con sus nombres.

**Dónde están los bloques:**

```bash
hdfs fsck /data/data.txt -files -blocks -locations
```

Un bloque con factor 3: 3 de los 4 DataNodes lo alojan.

**Recuperar y verificar la integridad:**

```bash
hdfs dfs -get /data/data.txt /workspace/recuperado-hdfs.txt
sha256sum /workspace/data.txt /workspace/recuperado-hdfs.txt
```

**Tamaño lógico y espacio consumido:**

```bash
hdfs dfs -du -h /data
hdfs dfs -count -h /data
```

`du` muestra el tamaño y el espacio consumido con replicación (≈ tamaño × factor).

**Cambiar el factor de replicación:**

```bash
hdfs dfs -setrep -w 2 /data/data.txt
hdfs fsck /data/data.txt -files -blocks -locations
hdfs dfs -setrep -w 3 /data/data.txt
```

`-w` espera a que se cumpla el objetivo; si se piden más réplicas que nodos disponibles, la espera no termina.

**Detener un DataNode y seguir leyendo.** Localizar un nodo con réplica: `fsck` da IPs y el report relaciona cada IP con su contenedor.

```bash
# Dentro del NameNode
hdfs fsck /data/data.txt -files -blocks -locations
hdfs dfsadmin -report | grep Name

# En el anfitrión: detener solo ese contenedor (docker compose stop datanode pararía los cuatro)
docker stop hdfs-datanode-2

# Dentro del NameNode
hdfs dfs -cat /data/data.txt
hdfs fsck /data/data.txt -files -blocks -locations

# En el anfitrión
docker start hdfs-datanode-2
```

Se sigue leyendo desde otra réplica. Tras unos 50 s el NameNode da el nodo por muerto y crea una copia nueva en otro DataNode. Al volver el nodo puede haber una copia de más, que HDFS elimina después.

**Detener el único NameNode:**

```bash
# En el anfitrión
docker compose stop namenode
# La CLI se lanza desde un DataNode que siga vivo
docker compose exec --index 1 datanode hdfs dfs -ls /data
docker compose start namenode
```

Sin NameNode la operación falla (`UnknownHostException: namenode`), aunque los bloques sigan en los DataNodes. No ejecutar `hdfs namenode -format` al reiniciar.

**Un fichero de varios bloques**, dentro del NameNode:

```bash
yes 'SDPD,datos de ejemplo HDFS' | head -c 300M > /workspace/test.txt
hdfs dfs -put -f /workspace/test.txt /data/
hdfs fsck /data/test.txt -files -blocks -locations
```

Se esperan 3 bloques: 128, 128 y 44 MiB.

## Ejercicio 1

Repetir los pasos anteriores con un fichero `big-data.txt` de 300 MiB:

- ¿Cuántos bloques tiene y cuánto mide el último?
- ¿En qué nodos está cada bloque? ¿Cuántas copias hay?
- ¿Cómo se relacionan tamaño lógico y espacio consumido?
- ¿Se puede leer tras parar un DataNode con una réplica? ¿Qué se aprecia en la web?
- ¿Qué cambia al detener el único NameNode?

```bash
# Dentro del NameNode
yes 'SDPD,datos de ejemplo HDFS' | head -c 300M > /workspace/big-data.txt
hdfs dfs -put -f /workspace/big-data.txt /data/
hdfs fsck /data/big-data.txt -files -blocks -locations
hdfs dfs -du -h /data

# En el anfitrión: parar un DataNode con réplica
docker stop hdfs-datanode-3

# De nuevo en el NameNode
hdfs dfs -cat /data/big-data.txt | tail -n 2
```

## Resumen de comandos

Dentro del NameNode, `hdfs dfs -comando`:

| Tarea | Comando |
|---|---|
| Subir (local → HDFS) | `-put origen destino`, `-copyFromLocal`, `-moveFromLocal` (borra el original) |
| Bajar (HDFS → local) | `-get origen destino`, `-copyToLocal`, `-getmerge -nl` (concatena) |
| Navegar | `-ls [-R]`, `-mkdir -p` |
| Ver contenido | `-cat`, `-tail [-f]`, `-text` |
| Copiar y borrar | `-cp`, `-mv`, `-rm -r`, `-touchz` |
| Permisos | `-chmod`, `-chown`, `-chgrp` |
| Comprobar | `-test -e`, `-test -z`, `-test -d` (devuelve 0 si se cumple) |

| Tarea | Comando |
|---|---|
| Estado del clúster | `hdfs dfsadmin -report` |
| Safe mode | `hdfs dfsadmin -safemode get` |
| Bloques y réplicas | `hdfs fsck RUTA -files -blocks -locations` |
| Tamaño | `hdfs dfs -du -h RUTA` |
| Recuento | `hdfs dfs -count -h RUTA` |
| Replicación | `hdfs dfs -setrep -w N RUTA` |

`hadoop distcp` (copiar datasets en paralelo entre clústeres) y `hadoop archive` (agrupar ficheros pequeños en un HAR) lanzan trabajos MapReduce y necesitan YARN, que este despliegue no incluye.
