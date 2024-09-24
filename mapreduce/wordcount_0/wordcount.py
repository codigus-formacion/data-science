import MapReduce
import sys
# 1. Se crea un objeto MapReduce que se utiliza para pasar datos entre map y reduce 
mr = MapReduce.MapReduce()
# 2. La función mapper tokeniza cada entrada y emite un par clave-valor. 
def mapper(line):
    words = line.split()
    for word in words:
        mr.emit_intermediate(word, 1)
# 3. La función reduce resume la lista de ocurrencias y emite un recuento en la forma clave-valor.
def reducer(key, list_of_values):
    count = sum(list_of_values)
    mr.emit((key, count))
# 4. Se carga el archivo de entrada y se ejecuta la consulta MapReduce mostrando el resultado por stdout
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)