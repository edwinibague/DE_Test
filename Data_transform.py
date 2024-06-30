from prefect import task
import pandas as pd
import datetime
from dateutil import parser
import numpy as np

@task("Eliminacion de Datos faltantes o errones")
def clean_rows(df, date_index):
    
    df = df.drop(date_index, axis=0)
    to_drop = []
    for i in df.interrows():
        if not df[i[0], "cases"] >= 0 or not df[i[0], "deaths"] >= 0:
            to_drop.append(i[0])
    
    if to_drop:
        df = df.drop(to_drop, axis=0)
    
    df = df.dropna(axis=0, how= 'any')
    
    return df

@task("Validacion de fecha")
def Date_validation(df):
    
    index_invalid_format = []
    flag_cor = False
    for index, row in df.iterrows():
        date = row["date"]
        try:
            parse_date = parser.parse(date, format="%Y-%m-%d")
            df.loc[index,"date"] = parse_date.strftime("%Y-%m-%d")
            flag_cor = True
            
        except ValueError:
            print("Error: correccion de formato de fecha no realizado '{date}', fila: {index+1}")
            index_invalid_format.append(index)
        
    if not flag_cor:
        print("No han sido necesarias correcciones de formato de fecha en el DataFrame")
    
    return df, index_invalid_format


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