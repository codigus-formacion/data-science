import os
from pathlib import Path

from databricks import sql
from dotenv import load_dotenv

# Carga las variables del .env que está junto a este fichero,
# independientemente del directorio desde el que se ejecute el script.
load_dotenv(Path(__file__).parent / ".env")

# El hostname está en la URL del workspace.
# El http_path lo encuentras en SQL Warehouses > tu warehouse > Detalles de conexión.
with sql.connect(
    server_hostname = os.environ["DATABRICKS_HOST"],
    http_path       = os.environ["DATABRICKS_HTTP_PATH"],
    access_token    = os.environ["DATABRICKS_TOKEN"]
) as conexion:

    with conexion.cursor() as cursor:
        cursor.execute("""
            SELECT equipo_local, equipo_visitante, goles_local, goles_visitante
            FROM workspace.liga.partidos
            WHERE jornada = 1
        """)

        for fila in cursor.fetchall():
            print(f"{fila.equipo_local} {fila.goles_local} - "
                  f"{fila.goles_visitante} {fila.equipo_visitante}")