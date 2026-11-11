# Trabajando con Spark

Si utilizamos Databricks, no es necesario instalar nada en local, ya que Databricks nos proporciona un entorno de trabajo con Spark preconfigurado. Sin embargo, si queremos trabajar con Spark en local, necesitaremos instalar algunas dependencias.

## Requisito previo: Java

Spark se ejecuta sobre la JVM, por lo que necesitamos tener instalado Java. Recomendamos instalar la **última versión LTS** (actualmente Java 25); la versión mínima es Java 17. Comprobamos la versión instalada:

```bash
$ java -version
```

Si no lo tenemos instalado, podemos descargarlo desde la [página oficial de Java](https://www.oracle.com/es/java/technologies/downloads/) o usar el gestor de paquetes:

```bash
$ sudo apt install openjdk-25-jdk     # Ubuntu/Debian
$ brew install openjdk@25             # macOS
```

Si tenemos varias versiones de Java instaladas, Spark utiliza la que indique la variable `JAVA_HOME`. Si al crear la `SparkSession` obtenemos un error `UnsupportedClassVersionError` o `JAVA_GATEWAY_EXITED`, es que está apuntando a una versión anterior a la 17:

```bash
$ export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64   # Ajustar a la ruta de nuestro sistema
```

## Opción recomendada: uv

[uv](https://docs.astral.sh/uv/) se encarga de descargar la versión de Python adecuada, crear el entorno virtual e instalar las dependencias.

Instalamos uv (solo la primera vez)

```bash
$ curl -LsSf https://astral.sh/uv/install.sh | sh                                   # Linux/macOS
$ powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"  # Windows
```

Desde esta carpeta, creamos el entorno e instalamos las dependencias (puede tomar unos minutos)

```bash
$ uv sync
```

Lanzamos Jupyter Lab

```bash
$ uv run jupyter lab
```

## Alternativa: venv + pip

Si preferimos no usar uv, necesitamos Python 3.10 o superior. Creamos un entorno virtual de Python.

```bash
$ python -m venv spark-env
```

Activamos el entorno virtual

```bash
$ source spark-env/bin/activate      # Linux/macOS
$ spark-env\Scripts\activate         # Windows
```

Instalamos las dependencias necesarias (puede tomar unos minutos)

```bash
$ pip install -r requirements.txt
```

Lanzamos Jupyter Lab

```bash
$ jupyter lab
```
