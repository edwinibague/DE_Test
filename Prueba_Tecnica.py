from prefect import task, flow
from prefect.tasks.github import load_repo
from prefect.task.pandas import DataFrame 


#Proceso ETL 
#EXtraccin de datos 
@task #Tarea de Carga de datos 
def load_data(url_repo, filename):
    Repo_data = load_repo(url_repo)
    Data = Repo_data.get_contents(filename)
    return Data

#Transformacion de datos 
@task 


#Carga de datos
@task 


#creancion de flujo de trabajo 
@flow("Data_Transformation")


#Flujo principal
with Flow("Cargar_Data") as flow:
    Data = load_data(url_repo = "https://github.com/nytimes/covid-19-data/blob/master/", filename = "us.csv")
    