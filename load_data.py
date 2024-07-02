from prefect import task
from credentials import backend
from prefect_gcp.storage import CloudStorage 
from google.cloud import bigquery


#creacion de datalake con data virgen
@task("Creacion de Datalake")
def upload_to_datalake(Data, bucket_name: str, path: str):
    
    try:
        storage = CloudStorage(backend)
        storage.upload_string(Data, bucket_name, path)
        print("Datalake creado con exito.")
        
    except:
        print("Error en creacion de data lake. Tarea fallida...")
    

# Creacion de data werehouse bigquery

@task("creacion de tabla bigquery")
def create_table(data, Project_id, dataset_id, table_id):
    
    client = bigquery.Client(project= Project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    
    schema = bigquery.Schema.from_dataframe(data)
    table = bigquery.Table(table_ref, schema=schema)
    table.create_if_not_exists()
    
      
@task("creacion de dataset")
def create_dataset(dataset_id, project_id):
    
    client = bigquery.Client(project=project_id)
    dataset = client.dataset(dataset_id)
    dataset.create_if_not_exists()
    

@task("Cargar data en BigQuery")
def load_bq(data, table_ref, job_configure):
    client = bigquery.Client()
    load_job = client.load_table_from_dataframe(data, table_ref,job_config=job_configure)
    load_job.result()
    