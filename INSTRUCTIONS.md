### INSTRUCCIONES PARA LA EJECUCIÓN DE LA APLICACIÓN

1. En primer lugar se debe tener el Docker en ejecucion, esta aclaración nace del hecho que en Windows cuando se enciende el computador Docker no se inicializa por defecto, por ende es necesario buscar la aplicación de Docker y ejecutarla.
2. Instalar/activar kubernetes (kubectl). Docker Desktop trae la opción de habilitar Kubernetes, por ende si se cuenta con la herramienta de Docker Desktop no será necesario buscar Kubernetes por fuera de dicha aplicación, si no se tiene Docker Desktop entonces será necesario buscar una forma de instalar kubernetes de otra forma. Instrucciones para habilitar kubernetes en Docker Desktop:
    - Settings (Simbolo de la tuerca)
    - En el panel izquierdo hacer clic en Kubernetes
    - Clic en "Enable Kubernetes"
3. Clonar el repositorio del proyecto
    https://github.com/nucleusai/Sepsis_CDA
4. Hacer uso de la terminal de comandos y ubicarse en la siguiente ruta:
    EstructuraMongoDB\kubernetes
5. En la ruta anterior se encuentran todos los manifiestos de la aplicación que definen los componentes que son parte de ella, asi que se debe ejecutar el comando de kubectl para comenzar la ejecución de cada uno de los componentes:
		`kubectl apply -f .`
6. Por ultimo se puede ingresar al dashboard, el cual hace de frontend de la aplicación, accediendo al 
http://localhost:30003/
