FROM python:3.9

# Copiar requirements.txt al contenedor en /app
COPY requirements.txt /app/requirements.txt

# Ubicarse en el directorio /app
WORKDIR /app

# Instalar las dependencias del archivo requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiar los archivos a preprocesar al contenedor
COPY ["/dataset_pequeno", "/input_data"]

# Crear directorio para archivos preprocesados
RUN mkdir -p /output_data/

# Directorio para almacenar los datos de MongoDB
RUN mkdir -p /data/db

# Configuración y ejecución de MongoDB

# Copiar los archivos al contenedor
COPY main.py /scripts/main.py
COPY mongo_class.py /scripts/mongo_class.py
COPY ConversorPSV.py /scripts/ConversorPSV.py

# Definir el comando de inicio
CMD ["python3", "/scripts/main.py"]