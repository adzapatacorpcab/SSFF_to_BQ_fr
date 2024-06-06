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
############## ACTUALIZAR SIGUIENTES PARAMETROS SEGUN TABLA, HORARIO Y PROYECTO########
endpoint = "PerNationalId" #Tabla
project_id = "psa-sga-dfn-pr" #Proyecto
actualizacion ="0 12 * * *" #minuto dia mes dia de la semana si se quiere una vez es @once
##Formato de la actualizacion ### MINUTO - HORA - DIA - MES - DIA DE LA SEMANA 
########################################################################################
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "DAG_LOAD_SF_"+endpoint+"_BQ"


base_url = "https://api19.sapsf.com/odata/v2/"
page_size = 1000  # Tamaño de página
username = "apiadmin@efmayasoci"
password = "Tj8B#M3_yKa85%N62F"
filter_conditions = "fromDate=1900-01-01"
num_pages = 1000

dataset_id = "raw_ssff_mx"
table_id = endpoint+"_SF"
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
timezone = pytz.timezone('America/Mexico_City')

#credentials_path = "//pisabw/reportes/Cuentas de servicio big query/Control financiero/psa-fin-general-98b249fb5873.json"
#client = bigquery.Client.from_service_account_json(credentials_path)

with models.DAG(
        DAG_ID,
        schedule=actualizacion, #minuto dia mes dia de la semana si se quiere una vez es @once
        
        start_date=datetime(2023, 1, 1),
        description='Carga SF to BQ '+endpoint,
        catchup=False,
        default_args=default_args,
) as dag:
    def get_api_data(base_url, endpoint, page_size, username, password, filter_conditions, num_pages=1000):
        all_data = []
        
        page_number = 1
        
        # Función para cambiar el nombre de las columnas
        def transform_column_name(column_name):
            new_column_name = column_name.split('}')[-1]
            return new_column_name
        
        while page_number <= num_pages:
            url = f"{base_url}{endpoint}?$top={page_size}&$skip={(page_number - 1) * page_size}&{filter_conditions}"
            
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
            
            page_number += 1
        
        result_df = pd.concat(all_data, ignore_index=True)
        
        return result_df
        

    

    def df_to_bq():
        # Obtener el DataFrame resultante
        df_result = get_api_data(base_url, endpoint, page_size, username, password, filter_conditions, num_pages)
        df = df_result
        df = df.astype(str)
        df2 = df
        df2 = df2[0:0]

        # Agregar una fila en blanco al DataFrame
        df2.loc[0] = [None] * len(df2.columns)
        destination_table = f"{project_id}.{dataset_id}.{table_id}"
        pandas_gbq.to_gbq(df2, destination_table=destination_table, project_id=project_id, if_exists='replace')
        pandas_gbq.to_gbq(df, destination_table=destination_table, project_id=project_id, if_exists='replace')


    def correo():
        # Configuración del servidor de correo
        smtp_server = "smtp.office365.com"
        smtp_port = 587
        smtp_username = "powerbi@pisa.com.mx"
        smtp_password = "cgQv$#X5hLUi&YeM=FP"
        
        # Lista de destinatarios
        destinatarios = ["ingenieriadatos@pisa.com.mx"]

        # Crear el objeto del mensaje
        mensaje = MIMEMultipart()
        mensaje["From"] = smtp_username
        mensaje["To"] = ", ".join(destinatarios)
        mensaje["Subject"] = "Carga de tabla "+endpoint+" a BigQuery correcta"

        # Cuerpo del mensaje
        cuerpo_mensaje = "\n\n"+"Carga de tabla "+endpoint+" a BigQuery correcta en dataset "+dataset_id+" en proyecto "+project_id+"\n\n\n"
        mensaje.attach(MIMEText(cuerpo_mensaje, "plain"))


        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Habilitar conexión segura
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, destinatarios, mensaje.as_string())
        server.quit()
        

    


    

 # Ejecuta llamadas de Python convertir info y crear df
    py_op_load = PythonOperator(
        task_id='load_to_bq',
        python_callable=df_to_bq,
        dag=dag,
        provide_context=True
    )
    

    py_op_correo = PythonOperator(
        task_id='send_email',
        python_callable=correo,
        dag=dag,
        provide_context=True
    )

    
    #py_op_extract >> py_op_convert >>\
    py_op_load >> py_op_correo