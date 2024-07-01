from prefect import task
from credentials import backend
from prefect_gcp.storage import CloudStorage 


@task("Creacion de Datalake")
def upload_to_datalake(Data, bucket_name: str, path: str):
    
    try:
        storage = CloudStorage(backend)
        storage.upload_string(Data, bucket_name, path)
        print("Datalake creado con exito.")
        
    except:
        print("Error en creacion de data lake. Tarea fallida...")
    

