# Librerias necesarias para la importación de los nuevos archivos CSV 
from pymongo import MongoClient
import pandas as pd
import json
import os
from os import listdir
from os.path import isfile, join

# Rutas de acceso locales

#ruta2='D:/Univalle/Tesis/CSV Dataset/'
ruta2 = '/output_data/'
#Función importando los archivos individuales estructurando y organizando los pacientes 

def mongoimport(csv_path, db_name, coll_name, db_url='localhost'):
        
        client = MongoClient(db_url)
        db = client[db_name]
        coll = db[coll_name]
        data = pd.read_csv(archivo, sep=',')
        payload = json.loads(data.to_json(orient='records'))
        coll.insert_many(payload)
        #return coll.count()


for paciente in listdir(ruta2):
        
         ndb = "SepsisTraining"
         col = "DataPacientes"
         archivo = ruta2+paciente
         try:
            mongoimport(archivo, ndb, col)
         except Exception as e:
             print(f"Se produjo un error con el paciente: {paciente}")

print("Datos importados con exito")

