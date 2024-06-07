import requests
import json
import logging
import os
import pandas as pd
import xml.etree.ElementTree as ET
logging.captureWarnings(True)
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pytz
import xml.etree.ElementTree as ET
import pyarrow as pa
import pyarrow.parquet as pq

#CONSTANTES DE SSFF
SSFF_USERNAME = "apiadmin@efmayasoci"
SSFF_PASSWORD = "Tj8B#M3_yKa85%N62F"
ODATA_BASE_URL = "https://api19.sapsf.com/odata/v2/"
PAGE_SIZE = 1000  # Tamaño de página
MAX_NUMBER_OF_PAGES = 1000
DATE_FILTER_CONDITIONS = "fromDate=1900-01-01"

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
        
        all_data = all_data.extend(entry_data)
        
        current_page_number += 1
    
    return all_data

dic_result = get_api_data(ODATA_BASE_URL, PAGE_SIZE, SSFF_USERNAME, SSFF_PASSWORD, DATE_FILTER_CONDITIONS, MAX_NUMBER_OF_PAGES, 'Position')