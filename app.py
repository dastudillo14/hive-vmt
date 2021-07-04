import conexion as co
import pandas as pd

hive = co.ConexionHive('0.0.0.0', 8181)
consulta = hive.consultar(f'SELECT * FROM pokes')
print('consulta ', consulta)
#hive.insertar('query')