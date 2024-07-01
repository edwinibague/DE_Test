from prefect import task, flow
from prefect_gcp import BigQueryClient, CloudStorage
from prefect.tasks.github import load_repo
import pandas as pd
import Data_transform


#Proceso ETL 
#EXtraccin de datos 
@task("Carga de datos") 
def load_data(url_repo, filename):
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
@task 


#creancion de flujo de trabajo 
@flow("Data_Transformation")
def tranform_data():
    pass

#Flujo principal
with flow("Cargar_Data") as flow:
    Data = load_data(url_repo = "https://github.com/nytimes/covid-19-data/blob/master/", filename = "us.csv")
    