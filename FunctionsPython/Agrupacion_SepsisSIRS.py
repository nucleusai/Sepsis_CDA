import matplotlib.pyplot as plt
import pandas as pd
  

def Agrupacion_Sepsis_SIRS(ruta):
  
  global positivos_Sepsis_SIRS
  global control_Sepsis_SIRS

  csv_path = ruta
  datos = pd.read_csv(csv_path, sep=',')

  paciente = datos.iloc[0]['Paciente']
  band = False
  band2 = False
  positivos_Sepsis_SIRS = 0
  control_Sepsis_SIRS = 0 

  for i in range(0,len(datos)):

     if (datos.loc[i, 'Paciente'] != paciente):
        paciente = datos.iloc[i]['Paciente']

        if ((band2 == True) and (band == True)):
            band2 = False
            band = False
        else:
           control_Sepsis_SIRS = control_Sepsis_SIRS + 1
   
     if (datos.loc[i, 'SIRS_Sepsis'] == 1 and datos.loc[i, 'Paciente'] == paciente and band == False): 
        positivos_Sepsis_SIRS = positivos_Sepsis_SIRS + 1
        band = True
     else:
        band2= True
    
  return (positivos_Sepsis_SIRS, control_Sepsis_SIRS)