from prefect import task, flow
from prefect_gcp.bigquery import BigQueryWarehouse
#from prefect.tasks.github import load_repo
import Data_transform
import Load_Data
from credentials import project_id
import pandas as pd





#Proceso ETL 
#EXtraccin de datos 
@task
def extract_data(url_repo):
    """
    Carga datos desde un repositorio y devuelve los datos.
    Args:
        url_repo (str): URL del repositorio.
    Returns:
        Data: Datos cargados.
    """
    Data = pd.read_csv(url_repo, header=0)
   
    return Data


#Transformacion de datos 
@task
def data_transform(Data): 
    """
    Realiza la transformacion de los datos desde un dataframe.
    Args:
        Data (DataFrame): DataFrame con los datos a trasnformar.
    returns:
        df: DataFrame modificado con las trasnformaciones respectivas.
    """   
    
    df, index = Data_transform.Date_validation(Data_frame=Data)
    df = Data_transform.clean_rows(Data_frame=df, date_index = index)
    df = Data_transform.new_cases_deaths(Data_frame=df)
    df = Data_transform.Data_Quality(Data_frame=df)
    
    return df
    
    
#Carga de datos
@task
def upload_data_to_cloud(df, data):
    """
    Carga de datos a un datawerehouse por medio de bigQuery y creacionde dataLake.
    Args:
        df (DataFrame); DataFrame con trasnformaciones.
        data (DataFrame); Dataframe con la informacion original sin transformar.
    returns:
        sin return
    """
    #bigquery_warehouse_block = BigQueryWarehouse.load("dataload")
    
    Load_Data.upload_to_datalake(Data = data, bucket_name="DE_Bucket", path="data/Covid19.csv")
    
    dataset_id = "Dataset_DE"
    table_id = "Table_DE"

    Table = Load_Data.create_table(dataset_id= dataset_id, table_id = table_id)
    
    Load_Data.load_bq(data= df, table_ref=Table)
    
    BigQueryWarehouse.close()
    



#Flujo principal
@flow
def ETL_prosses():
    Data = extract_data(url_repo = "https://github.com/nytimes/covid-19-data/blob/master/us.csv?raw=true")
    df = data_transform(Data)
    upload_data_to_cloud(df, Data)
    
    

if __name__ == "__main__":
    ETL_prosses()