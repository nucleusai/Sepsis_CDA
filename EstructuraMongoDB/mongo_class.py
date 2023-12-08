from pymongo import MongoClient
import pandas as pd
import json
import os
from os import listdir
from os.path import isfile, join

class Mongo:
    
    def __init__(self, client, db, col):
        self.client = MongoClient(client)
        self.db = self.client[db]
        self.col = self.db[col]
    
    
    def importar_csv(self, ruta2 = '/output_data/'):
        
        def mongoimport(archivo):
            data = pd.read_csv(archivo, sep=',')
            payload = json.loads(data.to_json(orient='records'))
            self.col.insert_many(payload)
            
        for paciente in listdir(ruta2):
        #  ndb = "SepsisTraining"
        #  col = "DataPacientes"
         archivo = ruta2+paciente
         try:
            mongoimport(archivo)
         except Exception as e:
             print(e)
             print(f"Se produjo un error con el paciente: {paciente}")

        print("Datos importados con exito")
    
    
    def group_aggregation(self):
        # Realiza una operación simple para verificar la conexión
        document_count = self.col.count_documents({})  # Cuenta todos los documentos en la colección
        print(f"Número total de documentos en la colección: {document_count}")

        # ESTA FUNCION ORGANIZA EL CDA
        # GRUPOS AGREGACIÓN---------------------------------------------------------------------------------------------------------------------------------------------------
        self.col.aggregate([
            {
                "$addFields": {
                    "x0_header": {
                        "ID": "$_id",
                        "Hora": "$Hora",
                        "patient": "$Paciente"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Hora": 0,
                    "Paciente": 0
                }
            },
            {
                "$addFields": {
                    "x1_demographics": {
                        "Age": "$Age",
                        "Gender": "$Gender",
                        "Unit_1": "$Unit1",
                        "Unit_2": "$Unit2"
                    }
                }
            },
            {
                "$project": {
                    "Age": 0,
                    "Gender": 0,
                    "Unit1": 0,
                    "Unit2": 0
                }
            },
            {
                "$addFields": {
                    "x2_Admission_details": {
                        "Hospital": "$Hospital",
                        "HospAdmTime": "$HospAdmTime",
                        "ICULOS": "$ICULOS"
                    }
                }
            },
            {
                "$project": {
                    "Hospital": 0,
                    "HospAdmTime": 0,
                    "ICULOS": 0
                }
            },
            {
                "$addFields": {
                    "x3_Vital_Signs": {
                        "Heart_rate": "$HR",
                        "Pulse_oximetry": "$O2Sat",
                        "Temperature": "$Temp",
                        "Systolic_BP": "$SBP",
                        "Mean_arterial_pressure": "$MAP",
                        "Diastolic_BP": "$DBP",
                        "Respiration_rate": "$Resp",
                        "End_tidal_carbon_dioxide": "$EtCO2"
                    }
                }
            },
            {
                "$project": {
                    "HR": 0,
                    "O2Sat": 0,
                    "Temp": 0,
                    "SBP": 0,
                    "MAP": 0,
                    "DBP": 0,
                    "Resp": 0,
                    "EtCO2": 0
                }
            },
            {
                "$addFields": {
                    "x4_Laboratory_values": {
                        "Excess_bicarbonate": "$BaseExcess",
                        "Bicarbonate": "$HCO3",
                        "Fraction_of_inspired_oxygen": "$FiO2",
                        "pH": "$pH",
                        "Partial_pressure_of_carbon_dioxide_from_arterial_blood": "$PaCO2",
                        "Oxygen_saturation_from_arterial_blood": "$SaO2",
                        "Aspartate_transaminase": "$AST",
                        "Blood_urea_nitrogen": "$BUN",
                        "Alkaline_phosphatase": "$Alkalinephos",
                        "Calcium": "$Calcium",
                        "Chloride": "$Chloride",
                        "Creatinine": "$Creatinine",
                        "Direct_bilirubin": "$Bilirubin_direct",
                        "Serum_glucose": "$Glucose",
                        "Lactic_acid": "$Lactate",
                        "Magnesium": "$Magnesium",
                        "Phosphate": "$Phosphate",
                        "Potassium": "$Potassium",
                        "Total_bilirubin": "$Bilirubin_total",
                        "Troponin": "$TroponinI",
                        "Hematocrit": "$Hct",
                        "Hemoglobin": "$Hgb",
                        "Partial_thromboplastin_time": "$PTT",
                        "Leukocyte_count": "$WBC",
                        "Fibrinogen_concentration": "$Fibrinogen",
                        "Platelet_count": "$Platelets",
                    }
                }
            },
            {
                "$project": {
                    "BaseExcess": 0,
                    "HCO3": 0,
                    "FiO2": 0,
                    "pH": 0,
                    "PaCO2": 0,
                    "SaO2": 0,
                    "AST": 0,
                    "BUN": 0,
                    "Alkalinephos": 0,
                    "Calcium": 0,
                    "Chloride": 0,
                    "Creatinine": 0,
                    "Bilirubin_direct": 0,
                    "Glucose": 0,
                    "Lactate": 0,
                    "Magnesium": 0,
                    "Phosphate": 0,
                    "Potassium": 0,
                    "Bilirubin_total": 0,
                    "TroponinI": 0,
                    "Hct": 0,
                    "Hgb": 0,
                    "PTT": 0,
                    "WBC": 0,
                    "Fibrinogen": 0,
                    "Platelets": 0
                }
            },
            {
                "$addFields": {
                    "x5_engineering_variables": {
                        "HR_SIRS": "$HR_SIRS",
                        "TEMP_SIRS": "$TEMP_SIRS",
                        "WBC_SIRS": "$WBC_SIRS",
                    }
                }
            },
            {
                "$project": {
                    "HR_SIRS": 0,
                    "TEMP_SIRS": 0,
                    "WBC_SIRS": 0
                }
            },
            {
                "$addFields": {
                    "x6_Result_Sepsis": {
                        "SepsisLabel": "$SepsisLabel",
                        "Sepsis_SOFA": "$Sepsis_SOFA",
                        "Sepsis_SIRS": "$Sepsis_SIRS",
                    }
                }
            },
            {
                "$project": {
                    "SepsisLabel": 0,
                    "Sepsis_SOFA": 0,
                    "Sepsis_SIRS": 0
                }
            },
            {
                "$out": "VisualizacionGrupos"
            }
        ]
        )
    
    def aggregation_variables(self):
        print(f"Conectado a la base de datos: {self.db.name}")
        print(f"Seleccionada la colección: {self.col.name}")


        # SERÍA UNA FUNCIÓN DE LA CLASE
        self.col.aggregate([
                            {"$addFields":{ 
                                        "HR_SIRS": {
                                                "$switch": {
                                                        "branches":[
                                                                {"case": {"$eq":  ["$HR",0]}, "then":0}, 
                                                                {"case": {"$and": [  {"$and": [{"$gte" :["$HR", 60.0]}, {"$lte" :["$HR", 100.0]}]}]}, "then" : 0}],
                                                                "default": 1}},
                                        "TEMP_SIRS": {
                                                "$switch": {
                                                        "branches":[
                                                                {"case": {"$eq":  ["$Temp",0]}, "then":0}, 
                                                                {"case": {"$and": [  {"$and": [{"$gte" :["$Temp", 36.0]}, {"$lte" :["$Temp", 38.3]}]}]}, "then" : 0}],
                                                                "default": 1}},
                                        "WBC_SIRS": {
                                                "$switch": {
                                                        "branches":[
                                                                {"case": {"$eq":  ["$WBC",0]}, "then":0}, 
                                                                {"case": {"$and": [  {"$and": [{"$gte" :["$WBC", 4.0]}, {"$lte" :["$WBC", 12.0]}]}]}, "then" : 0}],
                                                                "default": 1}},
                                        "Respiracion": {
                                                        "$switch": {
                                                                "branches":[
                                                                    {"case": {"$eq": ["$FiO2",0]}, "then":"No valido"},
                                                                    {"case": {"$ne": ["$FiO2",0]}, "then": {"$divide":["$SaO2", "$FiO2"]}}]}}, "default": "N.A"  }},
                            {"$addFields":{ 
                                        "Respiracion_SOFA" : {
                                                        "$switch" : {
                                                                "branches":[
                                                                    {"case": {"$eq":  ["$Respiracion","No valido"]}, "then":0}, 
                                                                    {"case": {"$and": [{"$eq" :["$Respiracion", "N.A" ]}]},"then" : 0},
                                                                    {"case": {"$and": [{"$gte" :["$Respiracion", 400 ]}]},"then" : 0},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Respiracion", 300]}, {"$lt" :["$Respiracion", 400]}]}]}, "then" : 1},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Respiracion", 200]}, {"$lt" :["$Respiracion", 300]}]}]}, "then" : 2},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Respiracion", 100]}, {"$lt" :["$Respiracion", 200]}]}]}, "then" : 3},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Respiracion", 1]}, {"$lt" :["$Respiracion", 100]}]}]}, "then" : 4}],
                                                                    "default": 0}},
                                        "Platelets_SOFA" : {
                                                        "$switch": {
                                                                "branches":[
                                                                    {"case": {"$eq":  ["$Platelets",0]}, "then":0}, 
                                                                    {"case": {"$and": [{"$gte" :["$Platelets", 150 ]}]},"then" : 0},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Platelets", 100]}, {"$lt" :["$Platelets", 150]}]}]}, "then" : 1},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Platelets", 50]}, {"$lt" :["$Platelets", 100]}]}]}, "then" : 2},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Platelets", 20]}, {"$lt" :["$Platelets", 50]}]}]}, "then" : 3},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Platelets", 1]}, {"$lt" :["$Platelets", 20]}]}]}, "then" : 4}],
                                                                    "default": 0}},
                                        "Bilirubin_total_SOFA" : {
                                                        "$switch": {
                                                                "branches":[
                                                                    {"case": {"$eq":  ["$Bilirubin_total",0]}, "then":0}, 
                                                                    {"case": {"$and": [{"$lt" :["$Bilirubin_total", 1.2 ]}]},"then" : 0},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Bilirubin_total", 1.2]}, {"$lt" :["$Bilirubin_total", 2.0]}]}]}, "then" : 1},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Bilirubin_total", 2.0]}, {"$lt" :["$Bilirubin_total", 6.0]}]}]}, "then" : 2},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Bilirubin_total", 6.0]}, {"$lt" :["$Bilirubin_total", 12.0]}]}]}, "then" : 3},
                                                                    {"case": {"$and": [  {"$gt" :["$Bilirubin_total", 12.0]}]}, "then" : 4}],
                                                                    "default": 0}},
                                        "MAP_SOFA": {
                                                        "$switch": {
                                                                "branches":[
                                                                    {"case": {"$eq":  ["$MAP",0]}, "then":0}, 
                                                                    {"case": {"$and": [{"$gt" :["$MAP", 70 ]}]},"then" : 0},
                                                                    {"case": {"$and": [  {"$lt" :["$MAP", 70]}]}, "then" : 1}],
                                                                    "default": 0}},
                                        "Creatinine_SOFA" : {
                                                        "$switch": {
                                                                "branches":[
                                                                    {"case": {"$eq":  ["$Creatinine",0]}, "then":0}, 
                                                                    {"case": {"$and": [{"$gte" :["$Creatinine", 1.2 ]}]},"then" : 0},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Creatinine", 1.2]}, {"$lt" :["$Creatinine", 2.0]}]}]}, "then" : 1},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Creatinine", 2.0]}, {"$lt" :["$Creatinine", 3.4]}]}]}, "then" : 2},
                                                                    {"case": {"$and": [  {"$and": [{"$gt" :["$Creatinine", 3.4]}, {"$lt" :["$Creatinine", 5.0]}]}]}, "then" : 3},
                                                                    {"case": {"$and": [  {"$gt" :["$Creatinine", 5.0]}]}, "then" : 4}],
                                                                    "default": 0}}}},
                            {"$project":  { "_id":1, 
                                        "Paciente":1, 
                                        "Hora":1,
                                        "Hospital":1, 
                                        "HR":1, 
                                        "Temp":1, 
                                        "WBC":1,
                                        "O2Sat":1,
                                        "SBP":1,
                                        "MAP":1,
                                        "DBP":1,
                                        "Resp":1,
                                        "EtCO2":1,
                                        "BaseExcess":1,
                                        "HCO3":1,
                                        "FiO2":1,
                                        "pH":1,
                                        "PaCO2":1,
                                        "SaO2":1,
                                        "AST":1,
                                        "BUN":1,
                                        "Alkalinephos":1,
                                        "Calcium":1,
                                        "Chloride":1,
                                        "Creatinine":1,
                                        "Bilirubin_direct":1,
                                        "Glucose":1,
                                        "Lactate":1,
                                        "Magnesium":1,
                                        "Phosphate":1,
                                        "Potassium":1,
                                        "Bilirubin_total":1,
                                        "TroponinI":1,
                                        "Hct":1,
                                        "Hgb":1,
                                        "PTT":1,
                                        "Fibrinogen":1,
                                        "Platelets":1,
                                        "Age":1,
                                        "Gender":1,
                                        "Unit1":1,
                                        "Unit2":1,
                                        "HospAdmTime":1,
                                        "ICULOS":1,
                                        "HR_SIRS":1,
                                        "TEMP_SIRS":1,
                                        "WBC_SIRS":1,
                                        "Respiracion":1,
                                        "Respiracion_SOFA":1,
                                        "Platelets_SOFA":1,
                                        "Bilirubin_total_SOFA":1,
                                        "MAP_SOFA":1,
                                        "Creatinine_SOFA":1,
                                        "SepsisLabel":1,
                                        "Sepsis_SIRS": {"$cond" :[{"$or" :[ {"$and" :[{"$eq" :["$HR_SIRS",1]},{"$eq" :["$TEMP_SIRS",1]}]},
                                                                    {"$and" :[{"$eq" :["$TEMP_SIRS",1]},{"$eq" :["$HR_SIRS",1]}]},
                                                                    {"$and" :[{"$eq" :["$HR_SIRS",1]},{"$eq" :["$WBC_SIRS",1]}]},
                                                                    {"$and" :[{"$eq" :["$WBC_SIRS",1]},{"$eq" :["$HR_SIRS",1]}]},
                                                                    {"$and" :[{"$eq" :["$WBC_SIRS",1]},{"$eq" :["$TEMP_SIRS",1]}]},
                                                                    {"$and" :[{"$eq" :["$TEMP_SIRS",1]},{"$eq" :["$WBC_SIRS",1]}]} ]},"1","0"]},
                                                                    }},
                                                                    
                            {"$out": "NewDataComplet"}])