from nltk.corpus import stopwords
from nltk import word_tokenize
import nltk
import os
import base64
import pandas as pd
import psycopg2
from google.cloud import bigquery


def extract_data_from_db():

    host = "34.45.33.45"
    port = "5432"
    dbname = "heal-dev-database"
    user = "heal_dev_user"
    password = "dJc51|39rYt*"
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        print("Connection to PostgreSQL established successfully.")

        cursor = conn.cursor()

        query = "SELECT gender FROM patient_patient;"  

        cursor.execute(query)
        
        rows = cursor.fetchall()

        cursor.close()
        conn.close()
        print("PostgreSQL connection closed.")

    except Exception as e:
        print(f"Error: {e}")

def standardize_dob(dob):
    try:
       
        standardized_dob = pd.to_datetime(dob, errors='coerce', infer_datetime_format=True)
        
        if pd.isna(standardized_dob):
            return None
     
        return standardized_dob.strftime('%Y-%m-%d')
    
    except Exception as e:
        
        print(f"Error standardizing DOB: {e}")
        return None
    
def clean_textual_records(textual_records):
    if isinstance(textual_records, str):
        
        for symbol in ",.'?!()":
        
            textual_records=textual_records.replace(symbol," ")
        stopwords_list = stopwords.words('english')
        stopwords_list.append("occassional")
        tokens = word_tokenize(textual_records)
        stopped_tokens = [w for w in tokens if w not in stopwords_list] 
        lower_stopped = [w.lower() for w in stopped_tokens]   
        return lower_stopped   
    else:
        return None    
    
def generate_columns(data,column_name):
    unique_values = set()
    for sublist in data[column_name]:
        if isinstance(sublist, list):  
            unique_values.update(sublist)  
    
    print(unique_values)
    
    for word in unique_values:
        data[word] = data[column_name].apply(lambda x: 1 if isinstance(x, list) and word in x else 0)

    return data   

def push_dataframe_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str, if_exists='replace'):
    
    client = bigquery.Client(project=project_id)

    
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    
    if if_exists == 'append':
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif if_exists == 'fail':
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  

    print(f"Loaded {job.output_rows} rows into {table_ref}.")
    return table_ref    