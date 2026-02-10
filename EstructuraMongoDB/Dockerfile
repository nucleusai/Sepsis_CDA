FROM python:3.9

# Instalar las herramientas de MongoDB
RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian92-x86_64-100.3.1.deb && \
    apt install ./mongodb-database-tools-*.deb && \
    rm -f mongodb-database-tools-*.deb
    
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
COPY utils.py /scripts/utils.py

# Definir el comando de inicio
CMD ["python3", "-u", "/scripts/main.py"]