from mongo_class import Mongo
from utils import psv_to_csv, read_psv
import subprocess

# # ConversorPSV
# ruta2 = './dataset_pequeno/'
ruta2 = '/input_data/'
# rutaOutput = 'C:/Users/USUARIO/Documents/TESIS/CDA/EstructuraMongoDB/CsvDatasetPequeno/'

psv_to_csv(ruta2)
print("Datos convertidos con exito")

mongo = Mongo('mongodb_container', 'SepsisTraining', 'DataPacientes')
mongo.importar_csv()
mongo.group_aggregation()
mongo.aggregation_variables()

# Validar la consistencia de los datos
read_psv(ruta2, 'mongodb_container', 'SepsisTraining', 'NewDataComplet')
print('VALIDACION TERMINADA')

# Ejecutar mongoexport
mongoexport_command = [
    "mongoexport",
    "--host", "mongodb_container",  
    "--db", "SepsisTraining",
    "--collection", "NewDataComplet",  
    "--out", "/data/db/NewDataComplet.json"]

subprocess.run(mongoexport_command)