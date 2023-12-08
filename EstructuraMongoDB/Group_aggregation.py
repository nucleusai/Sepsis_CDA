from pymongo import MongoClient

#CONEXION SERVIDOR
client = MongoClient('localhost')

#CONEXION A LA BASE DE DATOS Y A LA COLECCION

db = client['SepsisTraining'] # Se esta almacenando la base de datos que se esta utilizando
col = db['DataPacientes'] # Se esta almacenando la coleccion en la base de datos

# Realiza una operación simple para verificar la conexión
document_count = col.count_documents({})  # Cuenta todos los documentos en la colección
print(f"Número total de documentos en la colección: {document_count}")

# ESTA FUNCION ORGANIZA EL CDA
# GRUPOS AGREGACIÓN---------------------------------------------------------------------------------------------------------------------------------------------------
col.aggregate([
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

# db.SepsisResult.aggregate([

#                         {$addFields: {"x0_header": {
# 					                                "ID": "$_id",
# 					                                "Hora": "$Hora",
# 					                                "patient": "$Paciente"

# 	                    }}},

#                         {$project:  { _id:0, 
#                                       Hora:0, 
#                                       Paciente:0 
                                     
#                          }}, 
                        
#                         {$addFields: {"x1_demographics": {
# 					                                           "Age": "$Age",
# 			                                               "Gender": "$Gender",
# 					                                           "Unit_1": "$Unit1",
# 					                                           "Unit_2": "$Unit2",
			
#                         }}},

#                         {$project:  {   Age:0,
#                                         Gender:0,
#                                         Unit1:0,
#                                         Unit2:0,
                                
#                          }},

#                          {$addFields: {"x2_Admission details": {
# 					                                           "Hospital": "$Hospital",
# 					                                           "HospAdmTime": "$HospAdmTime",
#                                                      "ICULOS": "$ICULOS"
#                         }}},

#                         {$project:  {   Hospital:0,
#                                         HospAdmTime:0,
#                                         ICULOS:0
#                          }},

#                         {$addFields: {"x3_Vital_Signs": {
# 					                                          "Heart_rate": "$HR",
# 			                                                "Pulse_oximetry": "$O2Sat",
# 					                                          "Temperature": "$Temp",
# 					                                          "Systolic_BP": "$SBP",
# 					                                          "Mean_arterial_pressure": "$MAP",
#                                                          "Diastolic_BP": "$DBP",
#                                                          "Respiration_rate": "$Resp",
#                                                          "End_tidal_carbon_dioxide": "$EtCO2"
#                         }}},

#                         {$project:  { HR:0,
#                                       O2Sat:0, 
#                                       Temp:0, 
#                                       SBP:0,
#                                       MAP:0,
#                                       DBP:0,
#                                       Resp:0,
#                                       EtCO2:0
#                         }},

#                          {$addFields: {"x4_Laboratory_values": {
# 					                                                 "Excess_bicarbonate": "$BaseExcess",
# 			                                                       "Bicarbonate": "$HCO3",
# 					                                                 "Fraction_of_inspired_oxygen": "$FiO2",
# 					                                                 "pH": "$pH",
# 					                                                 "Partial_pressure_of_carbon_dioxide_from_arterial blood": "$PaCO2",
#                                                                 "Oxygen_saturation_from_arterial_blood": "$SaO2",
#                                                                 "Aspartate_transaminase": "$AST",
#                                                                 "Blood_urea_nitrogen": "$BUN",
# 			                                                       "Alkaline_phosphatase": "$Alkalinephos",
# 					                                                 "Calcium": "$Calcium",
# 					                                                 "Chloride": "$Chloride",
# 					                                                 "Creatinine": "$Creatinine",
#                                                                 "Direct_bilirubin": "$Bilirubin_direct",
#                                                                 "Serum_glucose": "$Glucose",
#                                                                 "Lactic_acid": "$Lactate",
#                                                                 "Magnesium": "$Magnesium",
#                                                                 "Phosphate": "$Phosphate",
#                                                                 "Potassium": "$Potassium",
#                                                                 "Total_bilirubin": "$Bilirubin_total",
#                                                                 "Troponin": "$TroponinI",
#                                                                 "Hematocrit": "$Hct",
#                                                                 "Hemoglobin": "$Hgb",
#                                                                 "Partial_thromboplastin_time": "$PTT",
#                                                                 "Leukocyte_count": "$WBC",
#                                                                 "Fibrinogen_concentration": "$Fibrinogen",
#                                                                 "Platelet_count": "$Platelets",
#                         }}},

#                         {$project:  { 
#                                         BaseExcess:0,
#                                          HCO3:0,
#                                          FiO2:0,
#                                          pH:0,
#                                          PaCO2:0,
#                                          SaO2:0,
#                                          AST:0,
#                                          BUN:0,
#                                          Alkalinephos:0,
#                                          Calcium:0,
#                                          Chloride:0,
#                                          Creatinine:0,
#                                          Bilirubin_direct:0,
#                                          Glucose:0,
#                                          Lactate:0,
#                                          Magnesium:0,
#                                          Phosphate:0,
#                                          Potassium:0,
#                                          Bilirubin_total:0,
#                                          TroponinI:0,
#                                          Hct:0,
#                                          Hgb:0,
#                                          PTT:0,
#                                          WBC:0,
#                                          Fibrinogen:0,
#                                          Platelets:0
#                         }},

#                          {$addFields: {"x5_engineering_variables": {
# 					                                                      "HR_SIRS": "$HR_SIRS",
# 			                                                            "TEMP_SIRS": "$TEMP_SIRS",
# 					                                                      "WBC_SIRS": "$WBC_SIRS",
					                                      
#                         }}}, 

#                         {$project:  { 
#                                       HR_SIRS:0,
#                                       TEMP_SIRS:0,
#                                       WBC_SIRS:0
#                            }},

#                          {$addFields: {"x6_Result_Sepsis": {
# 					                                            "SepsisLabel": "$SepsisLabel",
# 			                                                  "Sepsis_SOFA": "$Sepsis_SOFA",
# 					                                            "Sepsis_SIRS": "$Sepsis_SIRS",
					                                      
#                         }}}, 

#                         {$project:  { 
#                                       SepsisLabel:0,
#                                       Sepsis_SOFA:0,
#                                       Sepsis_SIRS:0
#                            }},
                        
#                         {$out: "VisualizacionGrupos"}])
