#Función SOFA
import matplotlib.pyplot as plt
import pandas as pd

def datos_SOFA(ruta):
  
  global DFSOFA
  global dTabla

  csv_path = ruta
  datos = pd.read_csv(csv_path, sep=',')

  #dTabla = datos[['SaO2', 'FiO2', 'Platelets', 'Bilirubin_total', 'MAP', 'Creatinine', 'SepsisLabel', 'Sepsis_SIRS', 'Sepsis_SOFA', 'Paciente', 'Hora']].copy() # Datos que se utilizan para el analisis SOFA
  #dTabla = datos.sort_values('Paciente')
  dTabla = datos.copy()
 
  #Ciclo 1

  for i in range(0,len(dTabla)): # Ciclo para crear las columna de respiración para puntuación SOFA
     
     if dTabla.loc[i, 'FiO2'] != 0:
         dTabla.loc[i, 'Respiracion'] = dTabla.loc[i, 'SaO2'] / dTabla.loc[i, 'FiO2']
     else:
       dTabla.loc[i, 'Respiracion'] = 0;
    

  #Ciclo 2

  for i in range(0,len(dTabla)): # Ciclo para crear las columnas de rango SOFA
                                 #.loc para verificar un valor de posición

      if dTabla.loc[i, 'Respiracion']>= 302 or dTabla.loc[i, 'Respiracion'] == 0: # Creación Columna SOFA de saturación de oxigeno
         dTabla.loc[i, 'SOFA_Respiracion'] = 0
      else:
        
        if dTabla.loc[i, 'Respiracion']>= 221 and dTabla.loc[i, 'Respiracion']< 302 : 
           dTabla.loc[i, 'SOFA_Respiracion'] = 1
        else:
          
          if dTabla.loc[i, 'Respiracion']>= 142 and dTabla.loc[i, 'Respiracion']< 221 :
             dTabla.loc[i, 'SOFA_Respiracion'] = 2
          else: 
            
            if dTabla.loc[i, 'Respiracion']>= 67 and dTabla.loc[i, 'Respiracion']< 142 : 
               dTabla.loc[i, 'SOFA_Respiracion'] = 3
            else:
              
              if dTabla.loc[i, 'Respiracion']> 1 and dTabla.loc[i, 'Respiracion']< 67 : 
                 dTabla.loc[i, 'SOFA_Respiracion'] = 4
    

      if dTabla.loc[i, 'Platelets']>= 150 or dTabla.loc[i, 'Platelets'] == 0: # Creación Columna SOFA de plaquetas
         dTabla.loc[i, 'SOFA_Platelets'] = 0
      else:
        
        if dTabla.loc[i, 'Platelets']>= 100 and dTabla.loc[i, 'Platelets'] < 150 : 
           dTabla.loc[i, 'SOFA_Platelets'] = 1
        else:

          if dTabla.loc[i, 'Platelets']>= 50 and dTabla.loc[i, 'Platelets'] < 100 : 
             dTabla.loc[i, 'SOFA_Platelets'] = 2
          else: 
            
            if dTabla.loc[i, 'Platelets']>= 20 and dTabla.loc[i, 'Platelets'] < 50 : 
               dTabla.loc[i, 'SOFA_Platelets'] = 3
            else:
              
              if dTabla.loc[i, 'Platelets']> 1 and dTabla.loc[i, 'Platelets'] < 20 : 
                 dTabla.loc[i, 'SOFA_Platelets'] = 4
 

        
      if dTabla.loc[i, 'Bilirubin_total']< 1.2 or dTabla.loc[i, 'Bilirubin_total'] ==0: # Creación Columna SOFA  de Bilirrubina Total
           dTabla.loc[i, 'SOFA_Bilirubin_total'] = 0
      else:
        
        if dTabla.loc[i, 'Bilirubin_total'] >= 1.2 and dTabla.loc[i, 'Bilirubin_total'] < 2:
             dTabla.loc[i, 'SOFA_Bilirubin_total'] = 1
        else:
          
          if dTabla.loc[i, 'Bilirubin_total'] >= 2 and dTabla.loc[i, 'Bilirubin_total'] < 6:
             dTabla.loc[i, 'SOFA_Bilirubin_total'] = 2
          else: 
            
            if dTabla.loc[i, 'Bilirubin_total'] >= 6 and dTabla.loc[i, 'Bilirubin_total'] < 12:
               dTabla.loc[i, 'SOFA_Bilirubin_total'] = 3
            else:
              
              if dTabla.loc[i, 'Bilirubin_total'] >= 12:
                 dTabla.loc[i, 'SOFA_Bilirubin_total'] = 4


      if dTabla.loc[i, 'MAP']>= 70.0 or dTabla.loc[i, 'MAP']==0 : # Creación Columna SOFA de Presión arterial media
         dTabla.loc[i, 'SOFA_MAP'] = 0
      else:
         dTabla.loc[i, 'SOFA_MAP'] = 1
        
        
        
      if dTabla.loc[i, 'Creatinine'] < 1.2 or dTabla.loc[i, 'Creatinine'] == 0: # Creaion columna SOFA de Creatinine
         dTabla.loc[i, 'SOFA_Creatinine'] = 0
      else:
        
        if dTabla.loc[i, 'Creatinine'] >= 1.2 or dTabla.loc[i, 'Creatinine'] < 2 : 
           dTabla.loc[i, 'SOFA_Creatinine'] = 1
        else:
          
          if dTabla.loc[i, 'Creatinine'] >= 2 or dTabla.loc[i, 'Creatinine'] < 3.4 : 
             dTabla.loc[i, 'SOFA_Creatinine'] = 2
          else: 
            
            if dTabla.loc[i, 'Creatinine'] >= 3.4 or dTabla.loc[i, 'Creatinine'] < 5 :
               dTabla.loc[i, 'SOFA_Creatinine'] = 3
            else:
              
              if dTabla.loc[i, 'Creatinine'] >= 5 : 
                 dTabla.loc[i, 'SOFA_Creatinine'] = 4

  #Ciclo 3
  for i in range(0,len(dTabla)): # Ciclo para crear la columna de resultado de Sepsis
  
      dTabla.loc[i, 'SOFA_Sepsis'] = dTabla.loc[i, 'SOFA_Respiracion'] + dTabla.loc[i, 'SOFA_Platelets'] + dTabla.loc[i, 'SOFA_Bilirubin_total'] + dTabla.loc[i, 'SOFA_MAP'] + dTabla.loc[i, 'SOFA_Creatinine']
      
      if dTabla.loc[i, 'SOFA_Sepsis'] >= 0 and dTabla.loc[i, 'SOFA_Sepsis'] <= 2 :
         dTabla.loc[i, 'GROUP_SOFA'] = 0
      else:

          if dTabla.loc[i, 'SOFA_Sepsis'] >= 3 and dTabla.loc[i, 'SOFA_Sepsis'] <= 5 :
             dTabla.loc[i, 'GROUP_SOFA'] = 1
          else:
        
            if dTabla.loc[i, 'SOFA_Sepsis'] >= 6 and dTabla.loc[i, 'SOFA_Sepsis'] <= 8 :
               dTabla.loc[i, 'GROUP_SOFA'] = 2
            else: 
    
              if dTabla.loc[i, 'SOFA_Sepsis'] >= 9 and dTabla.loc[i, 'SOFA_Sepsis'] <= 11 : 
                 dTabla.loc[i, 'GROUP_SOFA'] = 3
              else:
        
                  if dTabla.loc[i, 'SOFA_Sepsis'] >= 12:
                     dTabla.loc[i, 'GROUP_SOFA'] = 4


  
  DFSOFA = dTabla[['SOFA_Respiracion', 'SOFA_Platelets', 'SOFA_Bilirubin_total', 'SOFA_MAP', 'SOFA_Creatinine', 'SOFA_Sepsis', 'GROUP_SOFA','SepsisLabel']].copy()
  print(DFSOFA)
  
 #return print(DFSOFA)