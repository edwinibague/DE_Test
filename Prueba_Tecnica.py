import prefect
from prefect import task, flow
from prefect.tasks.github import load_repo
import pandas as pd


#Proceso ETL 
#EXtraccin de datos 
@task("Carga de datos") 
def load_data(url_repo, filename):
    Repo_data = load_repo(url_repo)
    Data = Repo_data.get_contents(filename)
    return Data

@task("creacion Data frame")
def data_frame(Data):
    df = pd.read_csv(Data)
    return df


#Transformacion de datos 
@task 


#Carga de datos
@task 


#creancion de flujo de trabajo 
@flow("Data_Transformation")
def tranform_data():
    pass

#Flujo principal
with flow("Cargar_Data") as flow:
    Data = load_data(url_repo = "https://github.com/nytimes/covid-19-data/blob/master/", filename = "us.csv")
    