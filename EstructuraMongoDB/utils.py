# Import necesarios para la conversión de los archivos PSV
# DEJARLO COMO FUNCION UTILS
import pandas as pd
import os
import csv
from os import listdir
from os.path import isfile, join
from mongo_class import Mongo

# Rutas locales de los archivos
# ruta2 = 'C:/Users/USUARIO/Documents/TESIS/CDA/EstructuraMongoDB/dataset_pequeno/'
#ruta2 = 'D:/Univalle/Tesis/Dataset/'
ruta2 = '/input_data/'

# Función para la conversión de los archivos PSV a CSV, 
# Creación de las columnas Paciente y Hora. Ya que los datos no contaban con identificación se tomo el numero del paciente

def psv_to_csv(ruta):

    for paciente in listdir(ruta):
    
        ruta3 = ruta2+paciente
        data1 = pd.read_table(ruta3, sep='|')
        data= data1.fillna(-9999)

        nombre= os.path.basename(ruta3)
        nombre= os.path.splitext(nombre)[0]
        hora = 0
    
        for i in data.index :
        
            data.loc[i, "Hora"] = hora
            data.loc[i, "Paciente"] = nombre
            hora=hora+1
    
        print(paciente)
        csv = '.csv'
        print(nombre)
        Narchivo = nombre + csv
        # directorio_salida = 'C:/Users/USUARIO/Documents/TESIS/CDA/EstructuraMongoDB/CsvDatasetPequeno/'
        directorio_salida = '/output_data/'
        data.to_csv( directorio_salida + Narchivo, sep=',')

def read_psv(ruta, host, db, col):
    accPSV = 0
    mongo = Mongo(host, db, col)
    accMongo = mongo.obtener_coleccion().count_documents({})
    for paciente in listdir(ruta):
        nombre = paciente.split('.')[0]
        hora = 0
        with open(ruta + paciente, 'r') as file:
            reader = csv.reader(file, delimiter = '|')  
            encabezados = next(reader)
            print(encabezados)
            for fila in reader:
                # print(fila)
                filtro = {"Paciente": nombre}

                for encabezado, medida in zip(encabezados, fila):
                    filtro[encabezado] = -9999 if medida == "NaN" else float(medida)

                filtro['Hora'] = hora
                # filtro['Hoja'] = 'Verde'
                registro = mongo.obtener_coleccion().find_one(filtro)
                if registro:
                    accPSV = accPSV + 1
                else:
                    raise Exception("Error: No se encontró ningún registro para el filtro:", filtro)
                hora = hora + 1
                
    print(accPSV, accMongo)
    if(accPSV == accMongo):
        print('Validación exitosa')
        return True
    else:
        print('Validación incorrecta')
        return False

# read_psv('./dataset_pequeno/','mongodb://127.0.0.1:27017','SepsisTraining', 'NewDataComplet')

