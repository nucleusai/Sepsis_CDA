from FunctionsPython.SepsisSofa import datos_SOFA

print("Esto es una prueba")

ruta = "DataExport/GrupoPrueba.csv"
print(ruta)

prueba = datos_SOFA(ruta)

for i in range(0,len(dTabla)): # Ciclo para crear la columna de resultado de Sepsis

    if dTabla.loc[i, 'Grupo_SOFA'] == 0 :    
        dTabla.loc[i, 'Sepsis_SOFA'] = 0
    else:
        dTabla.loc[i, 'Sepsis_SOFA'] = 1




db.DataPacientes.aggregate([
                            {$addFields:{ 
                                          HR_SIRS: {
                                                 $switch: {
                                                        branches:[
                                                               {case: {$eq:  ["$HR",0]}, then:0}, 
                                                               {case: {$and: [  {$and: [{$gte :["$HR", 60.0]}, {$lte :["$HR", 100.0]}]}]}, then : 0}],
                                                               default: 1}},
                                          TEMP_SIRS: {
                                                 $switch: {
                                                        branches:[
                                                               {case: {$eq:  ["$Temp",0]}, then:0}, 
                                                               {case: {$and: [  {$and: [{$gte :["$Temp", 36.0]}, {$lte :["$Temp", 38.3]}]}]}, then : 0}],
                                                               default: 1}},
                                          WBC_SIRS: {
                                                 $switch: {
                                                        branches:[
                                                               {case: {$eq:  ["$WBC",0]}, then:0}, 
                                                               {case: {$and: [  {$and: [{$gte :["$WBC", 4.0]}, {$lte :["$WBC", 12.0]}]}]}, then : 0}],
                                                               default: 1}},
                                          Respiracion: {
                                                        $switch: {
                                                               branches:[
                                                                      {case: {$eq: ["$FiO2",0]}, then:"No valido"},
                                                                      {case: {$ne: ["$FiO2",0]}, then: {$divide:["$SaO2", "$FiO2"]}}]}}, default: "N.A"  }},
                            {$addFields:{ 
                                          Respiracion_SOFA : {
                                                        $switch: {
                                                               branches:[
                                                                      {case: {$eq:  ["$Respiracion","No valido"]}, then:0}, 
                                                                      {case: {$and: [{$eq :["$Respiracion", "N.A" ]}]},then : 0},
                                                                      {case: {$and: [{$gte :["$Respiracion", 400 ]}]},then : 0},
                                                                      {case: {$and: [  {$and: [{$gt :["$Respiracion", 300]}, {$lt :["$Respiracion", 400]}]}]}, then : 1},
                                                                      {case: {$and: [  {$and: [{$gt :["$Respiracion", 200]}, {$lt :["$Respiracion", 300]}]}]}, then : 2},
                                                                      {case: {$and: [  {$and: [{$gt :["$Respiracion", 100]}, {$lt :["$Respiracion", 200]}]}]}, then : 3},
                                                                      {case: {$and: [  {$and: [{$gt :["$Respiracion", 1]}, {$lt :["$Respiracion", 100]}]}]}, then : 4}],
                                                                      default: 0}},
                                          Platelets_SOFA : {
                                                        $switch: {
                                                               branches:[
                                                                      {case: {$eq:  ["$Platelets",0]}, then:0}, 
                                                                      {case: {$and: [{$gte :["$Platelets", 150 ]}]},then : 0},
                                                                      {case: {$and: [  {$and: [{$gt :["$Platelets", 100]}, {$lt :["$Platelets", 150]}]}]}, then : 1},
                                                                      {case: {$and: [  {$and: [{$gt :["$Platelets", 50]}, {$lt :["$Platelets", 100]}]}]}, then : 2},
                                                                      {case: {$and: [  {$and: [{$gt :["$Platelets", 20]}, {$lt :["$Platelets", 50]}]}]}, then : 3},
                                                                      {case: {$and: [  {$and: [{$gt :["$Platelets", 1]}, {$lt :["$Platelets", 20]}]}]}, then : 4}],
                                                                      default: 0}},
                                          Bilirubin_total_SOFA : {
                                                        $switch: {
                                                               branches:[
                                                                      {case: {$eq:  ["$Bilirubin_total",0]}, then:0}, 
                                                                      {case: {$and: [{$lt :["$Bilirubin_total", 1.2 ]}]},then : 0},
                                                                      {case: {$and: [  {$and: [{$gt :["$Bilirubin_total", 1.2]}, {$lt :["$Bilirubin_total", 2.0]}]}]}, then : 1},
                                                                      {case: {$and: [  {$and: [{$gt :["$Bilirubin_total", 2.0]}, {$lt :["$Bilirubin_total", 6.0]}]}]}, then : 2},
                                                                      {case: {$and: [  {$and: [{$gt :["$Bilirubin_total", 6.0]}, {$lt :["$Bilirubin_total", 12.0]}]}]}, then : 3},
                                                                      {case: {$and: [  {$gt :["$Bilirubin_total", 12.0]}]}, then : 4}],
                                                                      default: 0}},
                                          MAP_SOFA: {
                                                        $switch: {
                                                               branches:[
                                                                      {case: {$eq:  ["$MAP",0]}, then:0}, 
                                                                      {case: {$and: [{$gt :["$MAP", 70 ]}]},then : 0},
                                                                      {case: {$and: [  {$lt :["$MAP", 70]}]}, then : 1}],
                                                                      default: 0}},
                                          Creatinine_SOFA : {
                                                        $switch: {
                                                               branches:[
                                                                      {case: {$eq:  ["$Creatinine",0]}, then:0}, 
                                                                      {case: {$and: [{$gte :["$Creatinine", 1.2 ]}]},then : 0},
                                                                      {case: {$and: [  {$and: [{$gt :["$Creatinine", 1.2]}, {$lt :["$Creatinine", 2.0]}]}]}, then : 1},
                                                                      {case: {$and: [  {$and: [{$gt :["$Creatinine", 2.0]}, {$lt :["$Creatinine", 3.4]}]}]}, then : 2},
                                                                      {case: {$and: [  {$and: [{$gt :["$Creatinine", 3.4]}, {$lt :["$Creatinine", 5.0]}]}]}, then : 3},
                                                                      {case: {$and: [  {$gt :["$Creatinine", 5.0]}]}, then : 4}],
                                                                      default: 0}}}},
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
                                          Respiracion:1,
                                          Respiracion_SOFA:1,
                                          Platelets_SOFA:1,
                                          Bilirubin_total_SOFA:1,
                                          MAP_SOFA:1,
                                          Creatinine_SOFA:1,
                                          SepsisLabel:1,
                                          }},
                                                                      
                            {$out: "NewDataComplet"}])