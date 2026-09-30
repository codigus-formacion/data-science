# Máquina virtual con Vagrant

Instrucciones para preparar la máquina virtual de la asignatura en los PCs del laboratorio.

Como el espacio disponible en la carpeta en red es limitado, **la máquina virtual se almacenará en el disco local del PC físico** (en `/var/tmp`).

> ⚠️ Esto significa que, cada vez que queráis levantar la máquina virtual, tendréis que hacerlo desde el **mismo PC físico** en el que la creasteis.

---

## 1. Configurar el directorio de almacenamiento en VirtualBox

1. Abrid **VirtualBox** desde el menú de aplicaciones de Ubuntu.
2. Id al menú **Archivo** → **Preferencias** → **General**.
3. Cambiad la **Carpeta predeterminada de máquinas** (*Default Machine Folder*) a:

   ```text
   /var/tmp/VirtualBoxVMs
   ```

---

## 2. Cambiar el directorio de archivos auxiliares de Vagrant

Por defecto, Vagrant guarda las imágenes base (*boxes*) y otros archivos auxiliares en `~/.vagrant.d`, es decir, en la carpeta en red. Para moverlos al disco local, abrid una terminal y ejecutad:

```bash
echo 'export VAGRANT_HOME=/var/tmp/.vagrant.d' >> ~/.bashrc
```

> 💡 Ejecutad este comando **una sola vez**. Después, cerrad la terminal y abrid una nueva para que se aplique el cambio.

Para comprobar que la variable está bien configurada, ejecutad en la nueva terminal:

```bash
echo "$VAGRANT_HOME"
```

Debería mostrar `/var/tmp/.vagrant.d`.
