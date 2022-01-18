from pymongo import MongoClient

#CONEXION SERVIDOR
client = MongoClient('localhost')

#CONEXION A LA BASE DE DATOS Y A LA COLECCION

db = client['datos'] # Se esta almacenando la base de datos que se esta utilizando
col = db['pacientes'] # Se esta almacenando la coleccion en la base de 

 {$addFields:{ 
                                      
                                      
                        {$addFields:{ 
                                      Grupo_SOFA: {
                                                    $switch: {
                                                              branches:[
                                                                        {case: {$lte:  ["$Puntaje_SOFA",2]}, then:"0"}, 
                                                                        {case: {$and: [  {$and: [{$gt :["$Puntaje_SOFA", 2]}, {$lt :["$Puntaje_SOFA", 5]}]}]}, then : "1"},
                                                                        {case: {$and: [  {$and: [{$gt :["$Puntaje_SOFA", 4]}, {$lt :["$Puntaje_SOFA",7 ]}]}]}, then : "2"},
                                                                        {case: {$and: [  {$and: [{$gt :["$Puntaje_SOFA", 6]}, {$lt :["$Puntaje_SOFA", 9]}]}]}, then : "3"},
                                                                        {case: {$and: [  {$and: [{$gt :["$Puntaje_SOFA", 8]}, {$lt :["$Puntaje_SOFA", 20]}]}]}, then : "4"}],
                                                                        default: "0"}}}

 Sepsis_SOFA:{
              $switch: {
                        branches:[
                                  {case: {$eq:  ["$Grupo_SOFA",0]}, then:"0"},
                                  {case: {$gte:  ["$Grupo_SOFA",1]}, then:"1"}], 
                                  default: "0"}},



# AGREGACIONES SEPARADAS ----------------------------------------------------------------------------------------------------------------------------------------------------------------


# AGREGACIÖN SIRS SEPSIS

db.pacientes.aggregate([
                        {$addFields:{ 
                                      HR_SIRS: {$and :[{$gte :["$HR", 60.0]},{$lte :["$HR", 100.0]}]}, 
                                      TEMP_SIRS: {$and :[{$gte :["$Temp", 36.0]},{$lte :["$Temp", 38.3]}]}, 
                                      WBC_SIRS: {$and :[{$gte :["$WBC", 4.0]},{$lte :["$WBC", 12.0]}]} 
                                      }},                                    
                                        {$project:  { _id:0, 
                                                      Paciente:1, 
                                                      Hora:1, 
                                                      HR:1, 
                                                      Temp:1, 
                                                      WBC:1,
                                                      HR_SIRS:1,
                                                      TEMP_SIRS:1,
                                                      WBC_SIRS:1,
                                                      SepsisLabel:1, 
                                                      Sepsis_SIRS: {$cond :[{$or :[ {$and :[{$eq :["$HR_SIRS",false]},{$eq :["$TEMP_SIRS",false]}]},
                                                                                    {$and :[{$eq :["$TEMP_SIRS",false]},{$eq :["$HR_SIRS",false]}]},
                                                                                    {$and :[{$eq :["$HR_SIRS",false]},{$eq :["$WBC_SIRS",false]}]},
                                                                                    {$and :[{$eq :["$WBC_SIRS",false]},{$eq :["$HR_SIRS",false]}]},
                                                                                    {$and :[{$eq :["$WBC_SIRS",false]},{$eq :["$TEMP_SIRS",false]}]},
                                                                                    {$and :[{$eq :["$TEMP_SIRS",false]},{$eq :["$WBC_SIRS",false]}]} ]},"1","0"]}}} ]).pretty()


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#


# AGREGACIÖN SOFA SEPSIS

db.pacientes.aggregate([
                        {$addFields:{ 
                                      RESP_DIFERENT: {$eq : ["FiO2", 0]}, 
                                      Sepsis_SOFA : {
                                                     $switch: {
                                                              branches:[

                                                                        {case: {$eq: ["$RESP_DIFERENT",true]}, then:"0"}, 

                                                                        {case: {$and:  [{$gte :["$Platelets", 150]},
                                                                                        {$lt :["$Bilirubin_total", 1.2]},
                                                                                        {$gte :["$MAP", 70]},
                                                                                        {$lte :["$Creatinine", 1.2]}]}, then : " 1-G0"},

                                                                        {case: {$and: [ {$and: [{$gte :["$Platelets", 100]}, {$lt :["$Platelets", 150]}]},
                                                                                         {$and: [{$gte :["$Bilirubin_total", 1.2]}, {$lte :["$Bilirubin_total", 2]}]},
                                                                                         {$lte :["$MAP", 70]},
                                                                                         {$and: [{$gte :["$Creatinine", 1.2]}, {$lte :["$Creatinine", 2]}]}]}, then : " 1-G1"},  

                                                                        {case: {$and: [ {$and: [{$gte :["$Platelets", 50]}, {$lt :["$Platelets", 100]}]},
                                                                                         {$and: [{$gt :["$Bilirubin_total", 2]}, {$lte :["$Bilirubin_total", 6]}]},
                                                                                         {$and: [{$gt :["$Creatinine", 2]}, {$lte :["$Creatinine", 3.4]}]}]}, then : " 1-G2"},
                                                                        
                                                                        {case: {$and: [ {$and: [{$gte :["$Platelets", 20]}, {$lt :["$Platelets", 50]}]},
                                                                                         {$and: [{$gt :["$Bilirubin_total", 6]}, {$lte :["$Bilirubin_total", 12]}]},
                                                                                         {$and: [{$gt :["$Creatinine", 3.4]}, {$lte :["$Creatinine", 5]}]}]}, then : " 1-G3"},

                                                                        {case: {$and: [ {$and: [{$gte :["$Platelets", 1]}, {$lt :["$Platelets", 20]}]},
                                                                                         {$gt :["$Bilirubin_total", 12]},
                                                                                         {$gt :["$Creatinine", 5]}]}, then : " 1-G4"} ], default: "N.A"}}}}]).pretty()


#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------





#------------------------------- AGREGACIÓN CONJUNTA SIRS-SOFA CREACIÓN COLLECIÓN NUEVA SEPSIS -------------------------------------------------------------------------------------------------------

db.DataPacientes.aggregate([
                        {$addFields:{ 
                                      HR_SIRS: {$and :[{$gte :["$HR", 60.0]},{$lte :["$HR", 100.0]}]}, 
                                      TEMP_SIRS: {$and :[{$gte :["$Temp", 36.0]},{$lte :["$Temp", 38.3]}]}, 
                                      WBC_SIRS: {$and :[{$gte :["$WBC", 4.0]},{$lte :["$WBC", 12.0]}]},
                                      Respiracion: {
                                                     $switch: {
                                                              branches:[
                                                                        {case: {$eq: ["$FiO2",0]}, then:"No valido"},
                                                                        {case: {$ne: ["$FiO2",0]}, then: {$divide:["$SaO2", "$FiO2"]}}], default: "N.A"  }}}},                         
                            {$project:  { _id:1, 
                                          Paciente:1, 
                                          Hora:1,
                                          Hospital:1, 
                                          HR:1, 
                                          Temp:1, 
                                          WBC:1,
                                          O2Sat:1,
                                          SBP:1,
                                          MAP:1,
                                          DBP:1,
                                          Resp:1,
                                          EtCO2:1,
                                          BaseExcess:1,
                                          HCO3:1,
                                          FiO2:1,
                                          pH:1,
                                          PaCO2:1,
                                          SaO2:1,
                                          AST:1,
                                          BUN:1,
                                          Alkalinephos:1,
                                          Calcium:1,
                                          Chloride:1,
                                          Creatinine:1,
                                          Bilirubin_direct:1,
                                          Glucose:1,
                                          Lactate:1,
                                          Magnesium:1,
                                          Phosphate:1,
                                          Potassium:1,
                                          Bilirubin_total:1,
                                          TroponinI:1,
                                          Hct:1,
                                          Hgb:1,
                                          PTT:1,
                                          Fibrinogen:1,
                                          Platelets:1,
                                          Age:1,
                                          Gender:1,
                                          Unit1:1,
                                          Unit2:1,
                                          HospAdmTime:1,
                                          ICULOS:1,
                                          HR_SIRS:1,
                                          TEMP_SIRS:1,
                                          WBC_SIRS:1,
                                          SepsisLabel:1,
                                          Sepsis_SIRS: {$cond :[{$or :[ {$and :[{$eq :["$HR_SIRS",false]},{$eq :["$TEMP_SIRS",false]}]},
                                                                        {$and :[{$eq :["$TEMP_SIRS",false]},{$eq :["$HR_SIRS",false]}]},
                                                                        {$and :[{$eq :["$HR_SIRS",false]},{$eq :["$WBC_SIRS",false]}]},
                                                                        {$and :[{$eq :["$WBC_SIRS",false]},{$eq :["$HR_SIRS",false]}]},
                                                                        {$and :[{$eq :["$WBC_SIRS",false]},{$eq :["$TEMP_SIRS",false]}]},
                                                                        {$and :[{$eq :["$TEMP_SIRS",false]},{$eq :["$WBC_SIRS",false]}]} ]},"1","0"]},                              
                                          Sepsis_SOFA : {
                                                     $switch: {
                                                              branches:[

                                                                        {case: {$eq: ["$Respiracion","No valido"]}, then:"0"}, 

                                                                        {case: {$and:  [{$gte :["$Platelets", 150]},
                                                                                        {$gte :["$Respiracion", 302]},
                                                                                        {$lt :["$Bilirubin_total", 1.2]},
                                                                                        {$gte :["$MAP", 70]},
                                                                                        {$lte :["$Creatinine", 1.2]}]}, then : " 1-G0"},

                                                                        {case: {$and: [  {$and: [{$gte :["$Platelets", 100]}, {$lt :["$Platelets", 150]}]},
                                                                                         {$and: [{$gt :["$Respiracion", 221]}, {$lt :["$Respiracion", 302]}]},
                                                                                         {$and: [{$gte :["$Bilirubin_total", 1.2]}, {$lte :["$Bilirubin_total", 2]}]},
                                                                                         {$lte :["$MAP", 70]},
                                                                                         {$and: [{$gte :["$Creatinine", 1.2]}, {$lte :["$Creatinine", 2]}]}]}, then : " 1-G1"},  

                                                                        {case: {$and: [  {$and: [{$gte :["$Platelets", 50]}, {$lt :["$Platelets", 100]}]},
                                                                                         {$and: [{$gt :["$Respiracion", 142]}, {$lt :["$Respiracion", 221]}]},
                                                                                         {$and: [{$gt :["$Bilirubin_total", 2]}, {$lte :["$Bilirubin_total", 6]}]},
                                                                                         {$and: [{$gt :["$Creatinine", 2]}, {$lte :["$Creatinine", 3.4]}]}]}, then : " 1-G2"},
                                                                        
                                                                        {case: {$and: [  {$and: [{$gte :["$Platelets", 20]}, {$lt :["$Platelets", 50]}]},
                                                                                         {$and: [{$gt :["$Respiracion", 67]}, {$lt :["$Respiracion", 142]}]},
                                                                                         {$and: [{$gt :["$Bilirubin_total", 6]}, {$lte :["$Bilirubin_total", 12]}]},
                                                                                         {$and: [{$gt :["$Creatinine", 3.4]}, {$lte :["$Creatinine", 5]}]}]}, then : " 1-G3"},

                                                                        {case: {$and: [  {$and: [{$gte :["$Platelets", 1]}, {$lt :["$Platelets", 20]}]},
                                                                                         {$and: [{$gt :["$Respiracion", 1]}, {$lt :["$Respiracion", 67]}]},
                                                                                         {$gt :["$Bilirubin_total", 12]},
                                                                                         {$gt :["$Creatinine", 5]}]}, then : " 1-G4"}], default: "N.A"}}}},
                                                                                         
                            {$out: "SepsisResult"}])

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#


 
 