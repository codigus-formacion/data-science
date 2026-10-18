# CockroachDB

## Conceptos

**De ficheros a registros.** Un dataset como `ventas.csv` se guarda y se lee para análisis, y se reescribe o se amplía por lotes: su lugar natural es HDFS o MinIO. Una tabla como `pedidos` se consulta y se actualiza registro a registro con transacciones: su lugar natural es una base de datos SQL. El almacenamiento de objetos no ejecuta por sí solo una transacción SQL.

**CockroachDB** es una base de datos SQL distribuida que habla el protocolo de PostgreSQL (compatible con sus herramientas, pero no es PostgreSQL). El cliente se conecta a cualquier nodo; ese nodo de entrada coordina la consulta y pide los datos a los demás. Es una base de datos lógica sin maestro único.

**Claves y rangos.** Tablas e índices se representan mediante claves. El espacio de claves se divide en **rangos** (particionado) y cada rango tiene réplicas en varios nodos (replicación). Dos filas y tres contenedores no bastan para ver muchos rangos: hay que observar la distribución real.

**Raft y mayoría por rango.** Las réplicas de cada rango forman un grupo Raft de 3 réplicas votantes, con su propio líder: no hay un líder global. Un cambio se confirma con la mayoría del grupo (2 de 3), así que cada rango tolera una caída. Hay que distinguir el nodo de entrada SQL, el acceso al rango y el liderazgo Raft. Los ACK del pipeline de HDFS no son el mismo protocolo.

**Transacciones entre rangos.** Una transacción puede afectar a varios rangos:

```sql
BEGIN;
UPDATE cuentas SET saldo = saldo - 100 WHERE id = 1;   -- débito, rango Ra
UPDATE cuentas SET saldo = saldo + 100 WHERE id = 2;   -- crédito, rango Rb
COMMIT;
```

Por defecto las dos cuentas comparten rango; se puede comprobar y separarlas (ver Ejercicio 3). Se aplican los dos cambios o ninguno, y el total de saldos no cambia (1000 + 500 = 1500). Replicar un rango y coordinar una transacción entre rangos son responsabilidades distintas. El aislamiento es SERIALIZABLE: ante conflictos puede haber que reintentar.

## El despliegue

```yaml
services:
  crdb:
    image: cockroachdb/cockroach:v25.4.17
    command: start --insecure --join=cockroachdb-crdb-1,cockroachdb-crdb-2,cockroachdb-crdb-3
    ports:
      - 26257-26259:26257   # SQL
      - 8080-8082:8080      # consola
    volumes:
      - ./sql:/sql          # solo ficheros de ejercicios
    deploy:
      replicas: 3
```

| Elemento | Qué hace |
|---|---|
| `replicas: 3` | El mismo servicio arranca tres procesos: `cockroachdb-crdb-1` a `-3` |
| `--join` | Direcciones para descubrir el clúster; no reparte filas ni particiones |
| `--insecure` | Configuración local, sin TLS ni contraseña |

Las tablas viven en la capa interna de cada contenedor, sin volumen de datos: se conservan con `stop`/`start` y se pierden con `docker compose down`. Los nombres dependen de arrancar desde la carpeta `cockroachdb/`.

| No confundir | Ejemplo |
|---|---|
| Número de contenedor | `cockroachdb-crdb-2` (índice 2 de Compose) |
| Node ID de CockroachDB | Lo asigna el clúster; no tiene por qué ser 2 |
| Réplica de un rango | Copia de un rango que vive en algún nodo |

**Licencia.** Sin clave, un clúster multinodo funciona completo durante 7 días desde `cockroach init`; después se limita a 5 transacciones abiertas a la vez. Un clúster recreado con `down` + `up` + `init` empieza de nuevo. Más información: [Licencias CockroachDB v25.4](https://www.cockroachlabs.com/docs/v25.4/licensing-faqs).

## Arranque

Desde la carpeta `cockroachdb/`:

```bash
# En el anfitrión
docker compose up -d
docker compose ps
```

La primera vez, crear el clúster y cargar los datos:

```bash
docker compose exec crdb cockroach init --insecure
docker compose exec crdb cockroach sql --insecure --file=/sql/01-datos.sql
```

Sin `cockroach init` los nodos esperan indefinidamente y la consola responde `404 page not found`. `01-datos.sql` crea la base de datos `sdpd` con las tablas `pedidos` y `cuentas` (saldos 1000 y 500, con `CHECK (saldo >= 0)`); se puede repetir sin sobrescribir datos.

Consolas web: http://localhost:8080, 8081 y 8082. Docker reparte los puertos entre las réplicas en orden arbitrario; cualquier nodo sirve cualquier consulta.

## Ejemplos

**Crear datos y consultar desde otra réplica:**

```bash
# En el anfitrión: cliente SQL en la réplica 1
docker compose exec crdb cockroach sql --insecure
```

```sql
-- Dentro del cliente SQL
SELECT * FROM sdpd.public.pedidos ORDER BY id;
UPDATE sdpd.public.pedidos SET estado = 'confirmado' WHERE id = 1;
SELECT * FROM sdpd.public.cuentas ORDER BY id;
```

```bash
# En otra terminal del anfitrión: réplica 2
docker compose exec --index 2 crdb cockroach sql --insecure \
  --execute='SELECT * FROM sdpd.public.pedidos;'
```

**Inspeccionar nodos, rangos y réplicas:**

```bash
docker compose exec crdb cockroach node status --insecure
docker compose exec crdb cockroach sql --insecure \
  --execute='SHOW RANGES FROM TABLE sdpd.public.pedidos WITH DETAILS;'
```

Los nodos se anuncian con el ID de su contenedor: `docker ps` lo relaciona con su nombre.

**Una caída con mayoría disponible:**

```bash
# En el anfitrión: detener una réplica que no es la 1
docker stop cockroachdb-crdb-3
docker compose exec -T crdb cockroach sql --insecure \
  --execute="SET statement_timeout = '15s';
    UPDATE sdpd.public.pedidos SET estado = 'enviado' WHERE id = 1;
    SELECT * FROM sdpd.public.pedidos WHERE id = 1;"
```

Quedan 2 de 3 réplicas votantes: hay mayoría y la escritura se confirma.

**Perder la mayoría y comprobar el resultado tras recuperar:**

```bash
# En el anfitrión, con cockroachdb-crdb-3 aún detenido
docker stop cockroachdb-crdb-2
docker compose exec -T crdb cockroach sql --insecure \
  --execute="SET statement_timeout = '15s';
    UPDATE sdpd.public.pedidos SET estado = 'sin-mayoria' WHERE id = 1;"

# Recuperar siempre las dos réplicas y volver a consultar
docker start cockroachdb-crdb-2 cockroachdb-crdb-3
docker compose exec crdb cockroach sql --insecure \
  --execute='SELECT * FROM sdpd.public.pedidos ORDER BY id;'
```

Sin mayoría, la sentencia agota el timeout (`SQLSTATE: 57014`). **Un timeout no significa que la escritura no se haya hecho:** tras recuperar los nodos, el pedido 1 aparece como `sin-mayoria`, porque la operación quedó pendiente y se confirmó al recuperar la mayoría. Ante un error ambiguo hay que comprobar el dato, no suponer el resultado.

## Ejercicio 3: transferencia e invariante

`03-transferencia.sql` mueve 100 de la cuenta 1 a la 2 dentro de una transacción y muestra los saldos y el total. `02-prueba.sql` hace la misma transferencia en una tabla aparte y termina con `ROLLBACK`.

```bash
# En el anfitrión
docker compose exec -T crdb cockroach sql --insecure --file=/sql/03-transferencia.sql
# ROLLBACK en una tabla aparte
docker compose exec -T crdb cockroach sql --insecure --file=/sql/02-prueba.sql
```

| | id 1 | id 2 | Total |
|---|---|---|---|
| Antes | 1000 | 500 | 1500 |
| 1.ª vez | 900 | 600 | 1500 |
| 2.ª vez | 800 | 700 | 1500 |
| 3.ª vez, con `cockroachdb-crdb-3` parado | 700 | 800 | 1500 |

Comprobar los saldos individuales, no solo el total. Repetir con una réplica detenida (`docker stop cockroachdb-crdb-3`; después, `docker start cockroachdb-crdb-3`).

- ¿Por qué el total sigue en 1500?
- ¿Qué impide el `CHECK` si repetimos muchas veces?

¿Mismo rango? Por defecto las dos cuentas comparten rango. Para ver una transacción que afecta a dos rangos, comprobarlo y separarlas (desde el cliente SQL de la réplica 1):

```sql
SHOW RANGE FROM TABLE sdpd.public.cuentas FOR ROW (1);
SHOW RANGE FROM TABLE sdpd.public.cuentas FOR ROW (2);
ALTER TABLE sdpd.public.cuentas SPLIT AT VALUES (2);
```

Para volver a los valores iniciales:

```sql
UPDATE sdpd.public.cuentas SET saldo = CASE id WHEN 1 THEN 1000 WHEN 2 THEN 500 END WHERE id IN (1, 2);
UPDATE sdpd.public.pedidos SET estado = 'pendiente';
```
