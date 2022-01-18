import numpy as np
import matplotlib.pyplot as plt
import math
import seaborn as sn
import pandas as pd
from math import e
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn import linear_model
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import  accuracy_score
from sklearn.metrics import roc_auc_score
from sklearn.metrics import  roc_curve
from sklearn.preprocessing import StandardScaler

# Se carga la data seleccionada para el estudio
csv_path = 'C:/Users/Cristian Toro/Desktop/Adelantos Proyecto de Grado/AnamnesisGIT/anamnesis/Anamnesis/DataUtilizada/General_SepsisCompleto.csv';
General_Sepsis = pd.read_csv(csv_path, sep=',')

for i in range(0,len(General_Sepsis)): # Ciclo para crear las columna de respiración para puntuación SOFA
     
     if General_Sepsis.loc[i, 'SOFA_Sepsis'] == 0:
         General_Sepsis.loc[i, 'Sepsis_SOFA'] = 0
     else:
       General_Sepsis.loc[i, 'Sepsis_SOFA'] = 1;

print(General_Sepsis)

# Se escogen la variables en X que se van a verificar para el analisis, y en y para el atributo que queremos identificar
X = (General_Sepsis[["HR","Respiracion","Platelets","Bilirubin_total","MAP","Creatinine","Temp","WBC"]])
y = (General_Sepsis["Sepsis_SOFA"])

#Entrenamiento
X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=0.25, random_state=0)

#Se utiliza el estandarizador de variables, media cero y desviación estandar uno
sc= StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)

# Predictoras de entrenamiento
print(X_train)

# Se utiliza regresión logistica
model = LogisticRegression(solver= 'newton-cg', multi_class='multinomial', max_iter=100)

# Se acciona el modelo
model.fit(X_train,y_train)

# Se predice
y_pred = model.predict(X_train)
print(y_pred)

# Score de predicción
model.score(X_test, y_test)

#Matriz de Confusión
cm= confusion_matrix(y_train, y_pred)
print(cm)

#Matriz de Confusión

cmPd = pd.DataFrame(cm)
sn.set(font_scale=1.4) 
sn.heatmap(cmPd, annot=True, annot_kws={"size" : 20}, fmt='g', center=0, linewidths=1, cbar=False)
plt.show()


#Curva ROC

logit_roc_auc = roc_auc_score(y_train, y_pred)
fpr, tpr, thresholds = roc_curve(y_train, y_pred)

plt.figure(figsize=(15,15))
plt.plot(fpr, tpr, label = 'Regresión Logistica (area = %0.2f)' % logit_roc_auc)
plt.plot([0,1], [0,1], 'r--')
plt.xlim([0.0,1.0])
plt.ylim([0.0,1.05])
plt.xlabel('% de Falsos positivos')
plt.ylabel('% de Verdaderos positivos')
plt.title('Curva ROC')
plt.legend(loc="lower right")
plt.show()
