import matplotlib.pyplot as plt
import pandas as pd

#Función SIRS

def datos_SIRS(ruta):
  
  global dTabla
  global DFSIRS

  csv_path = ruta
  datos = pd.read_csv(csv_path, sep=',')

  #Ciclo 1
  for i in range(0,len(dTabla)): # Ciclo para crear las columnas de rangos Boolean
                                 #.loc para verificar un valor de posición
    
    if dTabla.loc[i, 'HR'] == 0:
       dTabla.loc[i, 'SIRS_HR'] = 0 
    else:

      if  (dTabla.loc[i, 'HR']> 60.0 and dTabla.loc[i, 'HR']< 100.0): # Creación Columna SIRS de frecuencia cardiaca
           dTabla.loc[i, 'SIRS_HR'] = False
                       
      else:
            dTabla.loc[i, 'SIRS_HR'] = True
           
        
    if dTabla.loc[i, 'Temp'] == 0:
       dTabla.loc[i, 'SIRS_Temp'] = 0
    else:

      if  (dTabla.loc[i, 'Temp']> 36.0 and dTabla.loc[i, 'Temp']< 38.3): # Creación Columna SIRS de temperatura
           dTabla.loc[i, 'SIRS_Temp'] = False 
      else:
            dTabla.loc[i, 'SIRS_Temp'] = True


    if dTabla.loc[i, 'WBC'] == 0:
       dTabla.loc[i, 'SIRS_WBC'] = 0
    else:
      
      if (dTabla.loc[i, 'WBC']> 4.0 and dTabla.loc[i, 'WBC']< 12.0): # Creación Columna SIRS de Leucocitos
          dTabla.loc[i, 'SIRS_WBC'] = False
      else:
          dTabla.loc[i, 'SIRS_WBC'] = True
          
        
  
  #Ciclo 2
  for i in range(0,len(dTabla)): # Ciclo para crear la columna de resultado de Sepsis

    cont=0

    if dTabla.loc[i, 'SIRS_HR'] == True: # Columana SIRS Frecuencia cardiaca
        cont= cont+1
        
    if dTabla.loc[i, 'SIRS_Temp'] == True: # Columna SIRS Temperatura
        cont= cont+1

    if dTabla.loc[i, 'SIRS_WBC'] == True: # Columna SIRS Leucocitos
        cont= cont+1

    if cont >= 2 :                        # Creación columna sepsis
        dTabla.loc[i, 'SIRS_Sepsis'] = 1
    else:
        dTabla.loc[i, 'SIRS_Sepsis'] = 0


  DFSIRS = dTabla[['SIRS_HR', 'SIRS_Temp', 'SIRS_WBC', 'SIRS_Sepsis', 'SepsisLabel']].copy()