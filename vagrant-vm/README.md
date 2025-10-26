# 1. Configurar directorio de almacenamiento en VirtualBox

Dado que el espacio disponible en la carpeta en red es limitado, vamos a **almacenar la máquina virtual en la máquina física**.

⚠️ Esto significa que, siempre que queramos levantar nuestra máquina virtual, debemos estar en el **mismo equipo físico**.

**Pasos:**

1. Abrid **VirtualBox** desde el menú de programas de Ubuntu en el PC del laboratorio.  
2. Pulsad en el menú **Archivo** → **General**.  
3. Cambiad la **ruta de trabajo de VirtualBox** para las máquinas virtuales (MVs) a:

```bash
/var/tmp/VirtualBoxVMs
```

---

# 2. Cambiar el directorio de archivos auxiliares de Vagrant

Por defecto, Vagrant descarga sus archivos auxiliares en el directorio:

```bash
~/.vagrant.d
```

Para cambiar esta ruta, abrid una terminal y ejecutad:

```bash
export VAGRANT_HOME=/var/tmp/.vagrant.d
```

💡 Para que este cambio sea permanente y no tengáis que ejecutarlo cada vez que iniciáis sesión, añadidlo al archivo `~/.bash_profile` de vuestro $HOME:

```bash
echo 'export VAGRANT_HOME=/var/tmp/.vagrant.d' >> ~/.bash_profile
```