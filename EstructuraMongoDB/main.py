from mongo_class import Mongo
from ConversorPSV import listar_directorio

# # ConversorPSV
# ruta2 = 'D:/Univalle/Tesis/dataset_pequeno/'
# ruta1 = 'D:/Univalle/Tesis/CSV Dataset pequeno/'

ruta2 = '/input_data'
listar_directorio(ruta2)
print("Datos convertidos con exito")

mongo = Mongo('mongodb_container', 'SepsisTraining', 'DataPacientes')
mongo.importar_csv()
mongo.group_aggregation()
mongo.aggregation_variables()