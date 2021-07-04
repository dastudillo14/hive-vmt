from pyhive import presto  # or import hive
import pandas as pd

class ConexionHive():
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.user = ''
        self.pwd  = ''
    
    
    def __crear_cursor(self):
        try:
            return presto.connect(self.host,port=self.port).cursor()
        except Exception as e :
            print('Error en __crear_cursor >>> ', str(e))
            return False
    
    def consultar(self, query ):
        try:
            cursor = self.__crear_cursor()
            cursor.execute( query )
            return cursor.fetchall()
        except Exception as e :
            print('Error en consultar >>> ', str(e))
            return False
        finally:
            cursor.close()
    
    
    def insertar( self, query ):
        try:
            cursor = self.__crear_cursor()
            
            row = cursor.execute("INSERT INTO pokes (foo,bar) VALUES(101 , '1110')")
            print('row ', row)
        except Exception as e:
            print('Error en insertar >>> ', str(e))
            return False
        finally:
            cursor.close()        