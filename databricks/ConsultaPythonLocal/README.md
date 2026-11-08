# databricks_sql.py

Script que se conecta a un SQL warehouse de Databricks y muestra los
resultados de la jornada 1 de `workspace.liga.partidos`.

## Preparación

1. Crear el entorno virtual e instalar las dependencias:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

   Alternativa con [uv](https://docs.astral.sh/uv/) (no requiere `python3-venv`):

   ```bash
   uv venv .venv
   uv pip install -r requirements.txt
   ```

2. Crear un fichero `.env` en esta carpeta con las credenciales:

   ```
   DATABRICKS_HOST=<hostname del workspace, sin https://>
   DATABRICKS_HTTP_PATH=<SQL Warehouses > tu warehouse > Detalles de conexión>
   DATABRICKS_TOKEN=<token de acceso personal>
   ```

   El `.env` está en el `.gitignore`: no se sube al repositorio.

## Ejecución

Con el entorno virtual activado:

```bash
python databricks_sql.py
```

O sin activarlo:

```bash
.venv/bin/python databricks_sql.py
```

El script lee el `.env` de su propia carpeta, así que puede lanzarse desde
cualquier directorio.
