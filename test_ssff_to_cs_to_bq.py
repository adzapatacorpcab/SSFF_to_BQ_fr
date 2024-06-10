import requests
import json
import logging
import os
import xml.etree.ElementTree as ET
logging.captureWarnings(True)
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import storage
from google.cloud import bigquery
import pytz
import xml.etree.ElementTree as ET
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed

#CONSTANTES DE SSFF
SSFF_USERNAME = "apiadmin@efmayasoci"
SSFF_PASSWORD = "Tj8B#M3_yKa85%N62F"
ODATA_BASE_URL = "https://api19.sapsf.com/odata/v2/"
PAGE_SIZE = 1000  # Tamaño de página
MAX_NUMBER_OF_PAGES = 1000
DATE_FILTER_CONDITIONS = "fromDate=1900-01-01"

#CONSTANTES DE CLOUD STORAGE
BUCKET_NAME = 'raw_ssff_mx_qa'

#CONSTANTES DE BIGQUERY
PROJECT_ID = "psa-sga-dfn-qa" # CAMBIAR A PRD TRAS PRUEBAS: "psa-sga-dfn-pr"
DATASET_ID = "raw_ssff_mx"

def get_api_data(odata_base_url, page_size, username, password, date_filter_conditions, max_number_of_pages, endpoint):
    #Lista vacía para llenarla con los registros encontrados
    all_data = []
    #variable para iterar por las páginas
    current_page_number = 1
    
    # Función para cambiar el nombre de las columnas
    def transform_column_name(column_name):
        new_column_name = column_name.split('}')[-1]
        return new_column_name
    
    # Función que obtiene datos por página
    def fetch_page(page_number):
        url = f"{odata_base_url}{endpoint}?$top={page_size}&$skip={(page_number - 1) * page_size}&{date_filter_conditions}"
        logging.info(f"Fetching data from URL: {url}")

        response = requests.get(url, auth=(username, password))
        response.raise_for_status()  # Levantar una excepción para respuestas con error

        data = response.text
        return data

    # Función para procesar datos de una página
    def process_page(data):
        root = ET.fromstring(data)
        entries = root.findall('{http://www.w3.org/2005/Atom}entry')

        entry_data = []
        for entry in entries:
            entry_dict = {}
            for elem in entry.findall('{http://www.w3.org/2005/Atom}content/{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties/*'):
                entry_dict[elem.tag.replace('{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}', '')] = elem.text
            entry_data.append(entry_dict)

        return entry_data
    
    # Usar ThreadPoolExecutor para realizar solicitudes paralelas, explcacion pendiente

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_page = {executor.submit(fetch_page, page): page for page in range(1, max_number_of_pages + 1)}
        
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                data = future.result()
                page_data = process_page(data)
                if not page_data:
                    logging.info(f"No more entries found at page {page}.")
                    break
                all_data.extend(page_data)
                logging.info(f"Page {page} processed, {len(page_data)} entries found.")
            except Exception as e:
                logging.error(f"Request failed for page {page}: {e}")
                break
    
    if all_data:
        table = pa.Table.from_pylist(all_data)
        new_column_names = [transform_column_name(name) for name in table.schema.names]
        table = table.rename_columns(new_column_names)
        return table
    else:
        logging.warning("No data fetched from API.")
        return None

def table_to_CS(table, table_id):
    temp_path = f"/tmp/{table_id}.parquet"
    path_blob = f"{table_id}.parquet"
    pq.write_table(table, temp_path)
    # Crear cliente storage
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(path_blob)
    # Cargar parquet a CS
    blob.upload_from_filename(temp_path)

def CS_to_BQ(table_id_name):
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id_name}"
    gcs_uri = f"gs://{BUCKET_NAME}/{table_id_name}.parquet"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    # Cargar de GC a BQ
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    # Esperar al que trabajo se haga
    load_job.result()


dic_result = get_api_data(ODATA_BASE_URL, PAGE_SIZE, SSFF_USERNAME, SSFF_PASSWORD, DATE_FILTER_CONDITIONS, MAX_NUMBER_OF_PAGES, 'PickListLabel')
table_to_CS(dic_result, 'picklistlabel')
CS_to_BQ('picklistlabel')



