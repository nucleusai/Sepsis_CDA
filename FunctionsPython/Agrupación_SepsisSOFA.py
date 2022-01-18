import matplotlib.pyplot as plt
import pandas as pd
  
def Agrupacion_Sepsis_SOFA(ruta):
  
  global positivos_Sepsis_SOFA
  global control_Sepsis_SOFA

  csv_path = ruta
  datos = pd.read_csv(csv_path, sep=',')

  paciente = datos.iloc[0]['Paciente']
  band = False
  band2 = False
  positivos_Sepsis_SOFA = 0
  control_Sepsis_SOFA = 0 

  for i in range(0,len(datos)):

     if (datos.loc[i, 'Paciente'] != paciente):
        paciente = datos.iloc[i]['Paciente']

        if ((band2 == True) and (band == True)):
            band2 = False
            band = False

        else:
           control_Sepsis_SOFA = control_Sepsis_SOFA + 1
   
     if (datos.loc[i, 'GROUP_SOFA'] != 0 and datos.loc[i, 'Paciente'] == paciente and band == False): 
        positivos_Sepsis_SOFA = positivos_Sepsis_SOFA + 1
        band = True
     else:
        band2= True
  
  return (positivos_Sepsis_SOFA, control_Sepsis_SOFA)