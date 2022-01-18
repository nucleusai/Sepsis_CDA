from pymongo import MongoClient

#CONEXION SERVIDOR
client = MongoClient('localhost')

#CONEXION A LA BASE DE DATOS Y A LA COLECCION

db = client['SepsisTraining'] # Se esta almacenando la base de datos que se esta utilizando
col = db['DataPacientes'] # Se esta almacenando la coleccion en la base de datos


# INSERTAR DATOS
"""
col.insert_one({"name": "Cristian",
                "Edad": "26",
                "Sexo": "Masculino"
                })  # Agregar un solo Documento tipo JSON el id tiene que ser _id

#col.insert_many([]) #Insertar varios documentos
"""

#BUSCAR DATOS
"""
result = col.find({})
print(result) #Formato de cursor

for r in result:
    print(r) # Mostrar los datos de la coleccion
"""

#ELIMINAR DATOS
"""
col.delete_many({}) # Eliminar todo

col.delete_one({}) # Eliminar uno
"""

#ACTUALIZAR DATOS

col.update_one({"name": "laptop"}, {"$set":{"name": "kayboard"}}) # La primera sentencia lo que vamos a cambiar $set para el cambio y la segunda el dato nuevo


print(client.list_database_names()) # Imprimir database
print (col.count_documents({})) # Contar cuantos documentos hay
print(db.collection_names()) # Imprimir colecciones

"""

for documentos  in col.find({}) :
    print(documentos)
"""