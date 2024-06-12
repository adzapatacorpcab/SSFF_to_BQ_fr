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

#CONSTANTES DAG
DAG_ID = "DAG_LOAD_SSFF_TO_BQ_FREQ_REFRESH2" 
DAG_DESCRIPTION = "Carga las tablas PayScaleGroup, PerEmail, PerNationalId, PerPerson, PerPersonal, PerPersonRelarionship, PicklistLabel, PickListValueV2, Position, PositionMatrixRelationship de Success Factors a BQ"
ACTUALIZACION ="30 8,13,17 * * 1-5" # en formato cron Lunes a Viernes 8:30 am, 1:30 pm, 5:30pm
TIMEZONE = pytz.timezone('America/Mexico_City')

#CONSTANTES DE SSFF
SSFF_USERNAME = Variable.get("ssff_user") #variable idéntica en PRD
SSFF_PASSWORD = Variable.get("ssff_pass") #variable idéntica en PRD
ODATA_BASE_URL = "https://api19.sapsf.com/odata/v2/"
PAGE_SIZE = 1000  # Tamaño de página
MAX_NUMBER_OF_PAGES = 1000
DATE_FILTER_CONDITIONS = "fromDate=1900-01-01"

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
        start_date = datetime(2023, 1, 1, tzinfo=TIMEZONE),
        description = DAG_DESCRIPTION,
        catchup = False,
        default_args = default_args,
) as dag:
        
    def df_to_bq(endpoint_variable, table_id_variable):
        
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
                
                if data.startswith('<?xml'):
                    # Si la respuesta está en formato XML
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
                    df = pd.DataFrame(entry_data)
                else:
                    # Si la respuesta está en formato JSON
                    try:
                        json_data = json.loads(data)
                        df = pd.DataFrame(json_data['value'])
                    except json.JSONDecodeError as e:
                        print(f"An error occurred while parsing JSON: {e}")
                        df = pd.DataFrame()
                
                # Cambiar nombres de todas las columnas en el DataFrame df
                df = df.rename(columns=transform_column_name)
                
                all_data.append(df)
                
                current_page_number += 1
            
            result_df = pd.concat(all_data, ignore_index=True)
            
            return result_df
        
        
        # Obtener el DataFrame resultante
        df_result = get_api_data(ODATA_BASE_URL, PAGE_SIZE, SSFF_USERNAME, SSFF_PASSWORD, DATE_FILTER_CONDITIONS, MAX_NUMBER_OF_PAGES, endpoint= endpoint_variable)
        df = df_result

        # Se genera el id de tabla destino en BQ y se envía con un replace (equivalente en esta librería a TRUNCATE)
        destination_table = f"{PROJECT_ID}.{DATASET_ID}.{table_id_variable}"
        pandas_gbq.to_gbq(df, destination_table=destination_table, project_id=PROJECT_ID, if_exists='replace')

    def correo():
        # Configuración del servidor de correo
        smtp_server = "smtp.office365.com"
        smtp_port = 587
        smtp_username = EMAIL_SENDER 
        smtp_password = EMAIL_PASSWORD

        # Crear el objeto del mensaje
        mensaje = MIMEMultipart()
        mensaje["From"] = smtp_username
        mensaje["To"] = ", ".join(DESTINATARIOS)
        mensaje["Subject"] = "Carga de tablas a BigQuery exitosa"

        #Nombre de las tablas
        tablas = [
            "payscalegroup",
            "peremail",
            "pernationalid",
            "perperson",
            "perpersonal",
            "perpersonrelationship",
            "picklistlabel",
            "picklistvaluev2",
            "position",
            "positionmatrixrelationship"
        ]

        # Cuerpo del mensaje
        cuerpo_mensaje = (
            "La carga de las siguientes tablas a BigQuery ha finalizado exitosamente:\n\n" +
            "\n".join(f"- {tabla}" for tabla in tablas) +
            f"\n\nDataset: {DATASET_ID}\nProyecto: {PROJECT_ID}\n\n"
        )
        mensaje.attach(MIMEText(cuerpo_mensaje, "plain"))


        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Habilitar conexión segura
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, DESTINATARIOS, mensaje.as_string())
        server.quit()


    """Tasks de DAG"""
    payscalegroup_load = PythonOperator(
        task_id='payscalegroup_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'PayScaleGroup', 'table_id_variable': 'payscalegroup'},
        dag=dag,
        provide_context=True
    )

    peremail_load = PythonOperator(
        task_id='peremail_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'PerEmail', 'table_id_variable': 'peremail'},
        dag=dag,
        provide_context=True
    )


    pernationalid_load = PythonOperator(
        task_id='pernationalid_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'PerNationalId', 'table_id_variable': 'pernationalid'},
        dag=dag,
        provide_context=True
    )


    perperson_load = PythonOperator(
        task_id='perperson_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'PerPerson', 'table_id_variable': 'perperson'},
        dag=dag,
        provide_context=True
    )

    perpersonal_load = PythonOperator(
        task_id='perpersonal_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'PerPersonal', 'table_id_variable': 'perpersonal'},
        dag=dag,
        provide_context=True
    )


    perpersonrelationship_load = PythonOperator(
        task_id='perpersonrelationship_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'PerPersonRelationship', 'table_id_variable': 'perpersonrelationship'},
        dag=dag,
        provide_context=True
    )

    picklistlabel_load = PythonOperator(
        task_id='picklistlabel_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'PicklistLabel', 'table_id_variable': 'picklistlabel'},
        dag=dag,
        provide_context=True
    )

    picklistvaluev2_load = PythonOperator(
        task_id='picklistvaluev2_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'PickListValueV2', 'table_id_variable': 'picklistvaluev2'},
        dag=dag,
        provide_context=True
    )

    position_load = PythonOperator(
        task_id='position_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'Position', 'table_id_variable': 'position'},
        dag=dag,
        provide_context=True
    )

    positionmatrixrelationship_load = PythonOperator(
        task_id='positionmatrixrelationship_load_to_bq',
        python_callable=df_to_bq,
        op_kwargs={'endpoint_variable': 'PositionMatrixRelationship', 'table_id_variable': 'positionmatrixrelationship'},
        dag=dag,
        provide_context=True
    )
    
    send_email = PythonOperator(
        task_id='send_email',
        python_callable=correo,
        dag=dag,
        provide_context=True
    )


    [payscalegroup_load, peremail_load, pernationalid_load, perperson_load, perpersonal_load, perpersonrelationship_load, picklistlabel_load , picklistvaluev2_load, position_load, positionmatrixrelationship_load] >> send_email

    