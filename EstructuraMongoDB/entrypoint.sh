#!/bin/bash

# Iniciar MongoDB en segundo plano
mongod &

# Ejecutar tus scripts Python
python3 /scripts/ConversorPSV.py
python3 /scripts/ImportarCSV.py
python3 /scripts/Group_aggregation.py
python3 /scripts/Aggregation_Variables.py