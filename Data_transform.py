from prefect import task
import pandas as pd
import datetime
from dateutil import parser
import numpy as np

@task("Eliminacion de Datos faltantes o errones")
def clean_rows(df, date_index):
    
    df = df.drop(date_index, axis=0)
    
    mask = (df["cases"] >= 0) & (df["deaths"] >= 0)
    df =df[mask]
    
    df = df.dropna(axis=0, how= 'any')
    
    return df

@task("Validacion de fecha")
def validate_date(Date):
    try:
        perse_date = parser.parse(Date, format="%Y-%m-%d")
        return perse_date.strftime("%Y-%m-%d")
    except ValueError:
        return None

def Date_validation(df):
    
    df["date"] = df["date"].apply(validate_date)
    invalid_index = df[df["date"].isnull()].index.tolist()
    if not invalid_index:
        print("No se requieren correcciones de formato de fecha en el DataFrame")
    else:
        print("correcciones realizadas en  las filas: {invalid_index}")
    
    return df, invalid_index


@task("calculo de nuevos casos")
def new_cases_deaths(df):
    df['date'] = pd.to_datetime(df['date'])
    
    df = df.sort_values(by='date')
      
    df['new_cases'] = df['cases'].diff()
    df['new_deaths'] = df['deaths'].diff()
    
    df = df.fillna(0)
    
    return df


@task("Calidad de datos")
def Calidad_data(df):
    
    negative_cases = df[df['new_cases'] < 0].index
    negative_deaths = df[df['new_deaths']< 0].index
        
    negative_index = np.concatenate(negative_cases, negative_deaths)
    negative_index = np.unique(negative_index)
    
     
    df.drop(index=negative_index, axis=0)
    df.drop_duplicates(subset=['date'], inplace=True)
    
    return df