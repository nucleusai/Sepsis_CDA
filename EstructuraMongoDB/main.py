from mongo_class import Mongo
from utils import psv_to_csv, read_psv
import subprocess

import os
from dotenv import load_dotenv

load_dotenv()

mongoHost = os.getenv("HOST_BD")
mongoHostPort = os.getenv("HOST_BD_PORT")

# # ConversorPSV
# ruta2 = './dataset_pequeno/'
ruta2 = '/input_data/'
# rutaOutput = 'C:/Users/USUARIO/Documents/TESIS/CDA/EstructuraMongoDB/CsvDatasetPequeno/'

psv_to_csv(ruta2)
print("Datos convertidos con exito")

mongo = Mongo(f"mongodb://{mongoHost}:{mongoHostPort}", 'SepsisTraining', 'DataPacientes')
mongo.importar_csv()
mongo.group_aggregation()
mongo.aggregation_variables()

# Validar la consistencia de los datos
read_psv(ruta2, f"mongodb://{mongoHost}:{mongoHostPort}", 'SepsisTraining', 'NewDataComplet')
print('VALIDACION TERMINADA')

# # Ejecutar mongoexport
# mongoexport_command = [
#     "mongoexport",
#     "--host", "mongodb-container",  
#     "--db", "SepsisTraining",
#     "--collection", "NewDataComplet",  
#     "--out", "/data/db/NewDataComplet.json"]

# subprocess.run(mongoexport_command)