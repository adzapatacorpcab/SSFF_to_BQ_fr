import requests
import json
import logging
import os
import pandas as pd
from airflow.operators.python import PythonOperator
import xml.etree.ElementTree as ET
logging.captureWarnings(True)
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pytz
import pandas_gbq
from airflow import models
from airflow.models import Variable
import xml.etree.ElementTree as ET
import pyarrow as pa
import pyarrow.parquet as pq

#CONSTANTES DAG
DAG_ID = "DAG_LOAD_SSFF_TO_BQ_FREQ_REFRESH2" 
DAG_DESCRIPTION = "Prueba de carga tablas Position, PositionMatrixRelationship de Success Factors a Cloud Storage"
ACTUALIZACION ="30 8,13,17 * * 1-5" # en formato cron Lunes a Viernes 8:30 am, 1:30 pm, 5:30pm
TIMEZONE = pytz.timezone('America/Mexico_City')

#CONSTANTES DE SSFF
SSFF_USERNAME = "apiadmin@efmayasoci"
SSFF_PASSWORD = "Tj8B#M3_yKa85%N62F"
ODATA_BASE_URL = "https://api19.sapsf.com/odata/v2/"
PAGE_SIZE = 1000  # Tamaño de página
MAX_NUMBER_OF_PAGES = 1000
DATE_FILTER_CONDITIONS = "fromDate=1900-01-01"

#VARIABLES DE PROCESO (SSFF Y BIGQUERY)
endpoint_variable = "" #Tabla fuente en SSFF: valores distintos en cada flujo de tasks, argumentos definidos en las task como op_kwargs
table_id_variable = "" #Tabla destino en BQ: valores distintos en cada flujo de tasks, argumentos definidos en las task como op_kwargs

# Configuración DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['bigdata@pisa.com.mx'],  # Agrega los correos electrónicos aquí
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'email_subject_template': 'Error en la ejecución de {{ dag.dag_id }} en {{ ds }}',
    'email_html_content': """
    <h3>Error en la ejecución del DAG {{ dag.dag_id }}</h3>
    <p>Fecha: {{ ds }}</p>
    <p>Tarea: {{ task.task_id }}</p>
    <p>Log de errores:</p>
    <pre>{{ ti.log }}</pre>
    """
}

#Instanciación del DAG
  
with models.DAG(
        DAG_ID,
        schedule = ACTUALIZACION, 
        start_date = datetime(2023, 1, 1),
        description = DAG_DESCRIPTION,
        catchup = False,
        default_args = default_args,
) as dag:
       
    def get_api_data(odata_base_url, page_size, username, password, date_filter_conditions, max_number_of_pages, endpoint):
        #Lista vacía para llenarla con los registros encontrados
        all_data = {}
        #variable para iterar por las páginas
        current_page_number = 1
        
        # Función para cambiar el nombre de las columnas
        def transform_column_name(column_name):
            new_column_name = column_name.split('}')[-1]
            return new_column_name
        
        while current_page_number <= max_number_of_pages:
            url = f"{odata_base_url}{endpoint}?$top={page_size}&$skip={(current_page_number - 1) * page_size}&{date_filter_conditions}"
            
            response = requests.get(url, auth=(username, password))
            data = response.text
            # Creo siempre en estos casos va a estar en XML (asegurar)
            root = ET.fromstring(data)
            entries = root.findall('{http://www.w3.org/2005/Atom}entry')
            if not entries:
                break
            entry_data = []
            for entry in entries:
                entry_dict = {}
                for elem in entry.findall('{http://www.w3.org/2005/Atom}content/{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties/*'):
                    entry_dict[elem.tag.replace('{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}', '')] = elem.text
                entry_data.append(entry_dict)
            
            all_data = all_data.update(entry_data)
            
            current_page_number += 1

        table = pa.Table.from_pylist(all_data)
        table = table.rename_columns(transform_column_name(table.schema.names))
        
        return table
    