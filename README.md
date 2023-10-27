# Sepsis_CDA | PyMongoDB Aggregation Pipeline for Medical Data Visualization

This documentation provides an overview and usage guide for the MongoDB Aggregation Pipeline code used to process medical data and create a structured output for visualization. 
The code converts PSV (Pipe-Separated Values) files to CSV (Comma-Separated Values) format and then uses pymongo to import the csv files. 
The code aggregates and transforms data from a collection called 'SepsisResult' and stores the results in a new collection called 'VisualizacionGrupos'. 
The code is written in Python and uses the PyMongo library for MongoDB interactions.

**Prerequisites**

Before running this code, ensure that you have the following set up:
```
    MongoDB installed and running on your local machine.
    PyMongo library installed. You can install it using pip install pymongo.
```

**Workflow overview**

### Step 1 | ConversorPSV.py:
This is a simple Python script that converts PSV (Pipe-Separated Values) files to CSV (Comma-Separated Values) format. 
The PSV files typically contain medical data with pipe '|' as the delimiter. 
This script is specifically designed for converting PSV files in a directory into CSV format. 
Additionally, it adds 'Paciente' (patient) and 'Hora' (hour) columns to the CSV data, as the original PSV files lack proper identification.

### Step 2 | ImportarCSV.py:
This script is designed to import multiple CSV files into a MongoDB database using the Python programming language. 
It is particularly useful for handling patient data in the context of a healthcare project. 
Below, you will find an explanation of how the script works and how to use it.

### Step 3 | Group_aggregation.py:
The purpose of this script is to perform a series of data transformation and aggregation operations on the SepsisResult collection to organize the data into specific categories. 
The resulting data is then stored in a new collection called VisualizacionGrupos. 

The script accomplishes the following tasks:
+ Adding Fields: It adds new fields to the documents in the collection, categorizing them into different groups, such as demographics, admission details, vital signs, laboratory values, engineering variables, and sepsis-related information.
+ Projecting Fields: It removes unnecessary fields from the documents to keep only the newly added fields, effectively organizing the data into categories.
+ Final Output: It saves the transformed data into a new collection named VisualizacionGrupos.

### Step 4 | Aggregation_Variables.py.py:
The code is written for MongoDB and is used to process a collection of patient data to calculate SOFA scores and determine sepsis according to specific criteria. 
SOFA scores are used to assess the severity of organ dysfunction in critically ill patients, and the presence of sepsis is determined using the Sepsis-3 criteria.

Pipeline Overview

The aggregation variables consists of various stages, each responsible for different calculations. 
Below is an overview of the pipeline stages:

+ $addFields: This stage adds fields to the documents, including HR_SIRS, TEMP_SIRS, WBC_SIRS, and Respiracion.
+ $addFields: This stage calculates the SOFA scores for Respiracion, Platelets, Bilirubin_total, MAP, and Creatinine.
+ $project: This stage selects and retains specific fields from the input data and the newly calculated fields.
+ $out: This stage saves the processed data into a new collection named 'NewDataComplet'.
