# Librerias necesarias para la importación de los nuevos archivos CSV 
from pymongo import MongoClient
import pandas as pd
import json
import os
from os import listdir
from os.path import isfile, join

# Rutas de acceso locales

ruta = r'C:\Users\Cristian Toro\Desktop\Adelantos Proyecto de Grado\AnamnesisGIT\anamnesis\Anamnesis\sespsis_exploration_dataset\Hospital_A'
ruta2='C:/Users/Cristian Toro/Desktop/Adelantos Proyecto de Grado/AnamnesisGIT/anamnesis/Anamnesis/sespsis_exploration_dataset/Hospital_A/'


#Función importando los archivos individuales estructurando y organizando los pacientes 

def mongoimport(csv_path, db_name, coll_name, db_url='localhost'):
        
        client = MongoClient(db_url)
        db = client[db_name]
        coll = db[coll_name]
        data = pd.read_csv(archivo, sep=',')
        payload = json.loads(data.to_json(orient='records'))
        coll.insert(payload)
        #return coll.count()


for paciente in listdir(ruta):
        
         ndb = "SepsisTraining"
         col = "DataPacientes"
         archivo = ruta2+paciente
         mongoimport(archivo, ndb, col)

print("Datos importados con exito")


