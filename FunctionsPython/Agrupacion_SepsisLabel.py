import matplotlib.pyplot as plt
import pandas as pd

def Agrupacion_SepsisLabel(ruta):
  
  global positivos_SepsisLabel
  global control_SepsisLabel

  csv_path = ruta
  datos = pd.read_csv(csv_path, sep=',')

  paciente = datos.iloc[0]['Paciente']
  band = False
  band2 = False
  positivos_SepsisLabel = 0
  control_SepsisLabel = 0 

  for i in range(0,len(datos)):
    
     if (datos.loc[i, 'Paciente'] != paciente):
        paciente = datos.iloc[i]['Paciente']
       
        if ((band2 == True) and (band == True)):
            band2 = False
            band = False
        else:
           control_SepsisLabel = control_SepsisLabel + 1
   
     if ((datos.loc[i, 'SepsisLabel'] == 1) and (datos.loc[i, 'Paciente'] == paciente) and (band == False)): 
        positivos_SepsisLabel = positivos_SepsisLabel + 1
        band = True
     else:
        band2= True
  
  return (positivos_SepsisLabel, control_SepsisLabel)