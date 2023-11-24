# Import necesarios para la conversión de los archivos PSV

import pandas as pd
import os
from os import listdir
from os.path import isfile, join

# Rutas locales de los archivos

ruta2 = '/home/sdrivert/nucleusai/training_setA/'

# Función para la conversión de los archivos PSV a CSV, 
# Creación de las columnas Paciente y Hora. Ya que los datos no contaban con identificación se tomo el numero del paciente

def listar_directorio(ruta):

    for paciente in listdir(ruta):
    
        ruta3 = ruta2+paciente
        data1 = pd.read_table(ruta3, sep='|')
        data= data1.fillna(0)

        nombre= os.path.basename(ruta3)
        nombre= os.path.splitext(nombre)[0]
        hora = 0
    
        for i in data.index :
        
            data.loc[i, "Hora"] = hora
            data.loc[i, "Paciente"] = nombre
            hora=hora+1
    
        print(paciente)
        csv = '.csv'
        Narchivo = nombre + csv
        directorio_salida = 'files/'
        data.to_csv( directorio_salida + Narchivo, sep=',')

listar_directorio(ruta2)
      

print("Datos convertidos con exito")
