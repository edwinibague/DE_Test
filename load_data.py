from prefect import task
from credentials import gcp_credentials_block
from prefect_gcp.cloud_storage import cloud_storage_create_bucket, GcsBucket
from prefect_gcp.bigquery import bigquery_create_table, bigquery_insert_stream
from google.cloud import bigquery


#creacion de datalake con data virgen
@task
def upload_to_datalake(Data, bucket_name: str, path: str):
    
    try:
        storage = cloud_storage_create_bucket(bucket_name, gcp_credentials_block)
        print(f"Se ha creado  el Datalake '{storage}' con exito.")
        blob = GcsBucket.upload_from_dataframe(df=Data,to_path=path)
        
    except:
        print("Error en creacion de data lake. Tarea fallida...")
    

# Creacion de data werehouse bigquery

@task
def create_table(dataset_id, table_id):
        
    schema = [
        bigquery.SchemaField("date", field_type="DATE"),
        bigquery.SchemaField("cases", field_type="INTEGER"),
        bigquery.SchemaField("deaths", field_type="INTEGER"),
        bigquery.SchemaField("new_cases", field_type="INTEGER"),
        bigquery.SchemaField("new_deaths", field_type="INTEGER")
    ]
    
    result = bigquery_create_table(
        dataset=dataset_id,
        table=table_id,
        schema=schema,
        gcp_credentials=gcp_credentials_block
    )
    
    return result
    
    

@task
def load_bq(data, table_ref, dataset_id):
    
    Data = data.to_dict()
    
    result = bigquery_insert_stream(
        dataset=dataset_id,
        table=table_ref,
        records=Data,
        gcp_credentials=gcp_credentials_block
    )
    
    return result
    