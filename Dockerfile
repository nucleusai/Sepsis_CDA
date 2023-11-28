FROM mongo:latest

# Copiar los archivos al contenedor
COPY EstructuraMongoDB/Aggregation_Variables.py /scripts/Aggregation_Variables.py
COPY EstructuraMongoDB/Conversor.py /scripts/Conversor.py
COPY EstructuraMongoDB/Aggregation_Variables.py /scripts/Aggregation_Variables.py
COPY EstructuraMongoDB/ImportarCSV.py /scripts/ImportarCSV.py

# Ejecutar los archivos en un orden específico dentro del contenedor
RUN python /scripts/archivo1.py
RUN python /scripts/archivo2.py
RUN python /scripts/archivo3.py
RUN python /scripts/archivo4.py
