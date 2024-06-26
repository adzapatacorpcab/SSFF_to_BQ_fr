import requests
import json
import logging
import os
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
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud import bigquery

#CONSTANTES DAG
DAG_ID = "test_ssff_to_CS_to_BQ" 
DAG_DESCRIPTION = "Un test para pasar tablas de SSFF a Cloud Storage y luego a BigQuery"
ACTUALIZACION ="30 8,13,17 * * 1-5" # en formato cron Lunes a Viernes 8:30 am, 1:30 pm, 5:30pm
TIMEZONE = pytz.timezone('America/Mexico_City')

#CONSTANTES DE SSFF
SSFF_USERNAME = Variable.get("ssff_user") #variable idéntica en PRD
SSFF_PASSWORD = Variable.get("ssff_pass") #variable idéntica en PRD
ODATA_BASE_URL = "https://api19.sapsf.com/odata/v2/"
PAGE_SIZE = 1000  # Tamaño de página
MAX_NUMBER_OF_PAGES = 1000
DATE_FILTER_CONDITIONS = "fromDate=1900-01-01"

#CONSTANTES DE CLOUD STORAGE
BUCKET_NAME = 'raw_ssff_mx_qa'

#CONSTANTES DE BIGQUERY
PROJECT_ID = "psa-sga-dfn-qa" # CAMBIAR A PRD TRAS PRUEBAS: "psa-sga-dfn-pr"
DATASET_ID = "raw_ssff_mx"

#CONSTANTES DE CORREO
EMAIL_SENDER = Variable.get("sp_user")  #variable idéntica en PRD
EMAIL_PASSWORD = Variable.get("sp_pass") #variable idéntica en PRD 
DESTINATARIOS = ["adzapata@corpcab.com.mx"] # CAMBIAR A PRD TRAS PRUEBAS: "ingenieriadatos@pisa.com.mx"

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

    def table_to_CS(endpoint_variable, table_id_variable):

        def get_api_data(odata_base_url, page_size, username, password, date_filter_conditions, max_number_of_pages, endpoint):
            #Lista vacía para llenarla con los registros encontrados
            all_data = []
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
                
                all_data.extend(entry_data)
                
                current_page_number += 1

            table = pa.Table.from_pylist(all_data)
            new_column_names = [transform_column_name(name) for name in table.schema.names]
            table = table.rename_columns(new_column_names)
            
            return table
        
        # Obtener tabla de pyarrow a partir de SSFF

        table = get_api_data(ODATA_BASE_URL, PAGE_SIZE, SSFF_USERNAME, SSFF_PASSWORD, DATE_FILTER_CONDITIONS, MAX_NUMBER_OF_PAGES, endpoint=endpoint_variable)

        # Crear directorio temporal
        temp_path = f"/tmp/{table_id_variable}.parquet"

        # Crear blob
        path_blob = f"{table_id_variable}.parquet"
        
        # Crear archivo parquet en directorio temporal 
        pq.write_table(table, temp_path)

        # Crear cliente storage
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(path_blob)
        # Cargar parquet a CS
        blob.upload_from_filename(temp_path)

    def CS_to_BQ(table_id_variable):
        # Crear cliente
        client = bigquery.Client()
        # Crear table_id
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id_variable}"
        # Crear direccion de cloud storage
        gcs_uri = f"gs://{BUCKET_NAME}/{table_id_variable}.parquet"
        # Configuración dle trabajo
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        # Cargar de GC a BQ
        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        # Esperar al que trabajo se haga
        load_job.result()

    def correo(table_id_variable):
        # Configuración del servidor de correo
        smtp_server = "smtp.office365.com"
        smtp_port = 587
        smtp_username = EMAIL_SENDER 
        smtp_password = EMAIL_PASSWORD

        # Crear el objeto del mensaje
        mensaje = MIMEMultipart()
        mensaje["From"] = smtp_username
        mensaje["To"] = ", ".join(DESTINATARIOS)
        mensaje["Subject"] = "Carga de tabla "+table_id_variable+" a BigQuery exitosa"

        # Cuerpo del mensaje
        cuerpo_mensaje = "\n\n"+"Carga de tabla "+table_id_variable+" a BigQuery finalizada en dataset "+DATASET_ID+" en proyecto "+PROJECT_ID+"\n\n\n"
        mensaje.attach(MIMEText(cuerpo_mensaje, "plain"))


        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Habilitar conexión segura
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, DESTINATARIOS, mensaje.as_string())
        server.quit()

    """Tasks de DAG"""

    # Position a CS
    position_load_CS = PythonOperator(
        task_id='position_to_CS',
        python_callable=table_to_CS,
        op_kwargs={'endpoint_variable': 'Position', 'table_id_variable': 'position'},
        dag=dag,
        provide_context=True
    )

    # Position a BQ
    position_load_BQ = PythonOperator(
        task_id='position_to_BQ',
        python_callable=CS_to_BQ,
        op_kwargs={'table_id_variable': 'position'},
        dag=dag,
        provide_context=True
    )

    # Correo Position
    position_correo = PythonOperator(
        task_id='position_send_email',
        python_callable=correo,
        op_kwargs={'table_id_variable': 'position'},
        dag=dag,
        provide_context=True
    )

    # PositionMatrixRelationship a CS
    positionmatrixrelationship_load_CS = PythonOperator(
        task_id='positionmatrixrelationship_to_CS',
        python_callable=table_to_CS,
        op_kwargs={'endpoint_variable': 'PositionMatrixRelationship', 'table_id_variable': 'positionmatrixrelationship'},
        dag=dag,
        provide_context=True
    )

    # PositionMatrixRelationship a BQ
    positionmatrixrelationship_load_BQ = PythonOperator(
        task_id='positionmatrixrelationship_to_BQ',
        python_callable=CS_to_BQ,
        op_kwargs={'table_id_variable': 'positionmatrixrelationship'},
        dag=dag,
        provide_context=True
    )

    # Correo PositionMatrixRelationship
    positionmatrixrelationship_correo = PythonOperator(
        task_id='positionmatrixrelationship_send_email',
        python_callable=correo,
        op_kwargs={'table_id_variable': 'positionmatrixrelationship'},
        dag=dag,
        provide_context=True
    )

    # Proceso Position

    position_load_CS >> position_load_BQ >> position_correo

    # procesp PositionMatrixRelationship

    positionmatrixrelationship_load_CS >> positionmatrixrelationship_load_BQ >> positionmatrixrelationship_correo