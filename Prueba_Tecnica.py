from prefect import task, flow
from prefect_gcp import BigQueryClient, CloudStorage
from prefect.tasks.github import load_repo
import Data_transform
from google.cloud import bigquery
import Load_Data
from credentials import project_id

#Proceso ETL 
#EXtraccin de datos 
@task("descarga de datos") 
def load_data(url_repo, filename):
    """
    Carga datos desde un repositorio y devuelve los datos.
    Args:
        url_repo (str): URL del repositorio.
        filename (str): Nombre del archivo a cargar.
    Returns:
        Data: Datos cargados.
    """
    
    Repo_data = load_repo(url_repo)
    Data = Repo_data.get_contents(filename)
    return Data


#Transformacion de datos 
@task("Transformacion de datos")
def data_transform(Data): 
    
    DataFrame = Data_transform.Create_data_frame(Data=Data)
    
    df, index = Data_transform.Date_validation(Data_frame=DataFrame)
    df = Data_transform.clean_rows(Data_frame=df, date_index = index)
    df = Data_transform.new_cases_deaths(Data_frame=df)
    df = Data_transform.Data_Quality(Data_frame=df)
    
    return df
    
    
#Carga de datos
@task("Carga de datos con bigquery y creacion de datalake")
def upload_data_to_cloud(df, data):
    
    Load_Data.upload_to_datalake(Data = data, bucket_name="DE_Bucket", path="data/Covid19.csv")
    
    dataset_id = "Dataset_DE"
    table_id = "Table_DE"
    
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.dataframe = df  

    Load_Data.create_table(data = df, Project_id = project_id , dataset_id= dataset_id, table_id = table_id)
    
    Load_Data.load_bq(data= df, table_ref=table_id, job_configure=job_config)
    



#Flujo principal
with flow("Proceso_ETL") as flow:
    Data = load_data(url_repo = "https://github.com/nytimes/covid-19-data/blob/master/", filename = "us.csv")
    df = data_transform(Data)
    upload_data_to_cloud(df, Data)
    
    

flow.run()