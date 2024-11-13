# Set up Spark

Para trabajar con Spark, lo mejor es que nos creemos un entorno virtual de Python.

```bash
$ python -m venv spark
```

Si no tenemos instalada la libreria de entornos virtuales, la instalamos con el siguiente comando.

```bash
$ sudo apt install python3-venv
```

Activamos el entorno virtual

```bash
$ source spark/bin/activate
```

Instalamos las dependencias necesarias (puede tomar unos minutos)

```bash
$ pip install -r requirements.txt
```

Lanzamos Jupyter Lab

```bash
$ jupyter lab
```