from prefect import task
import pandas as pd
import datetime
from dateutil import parser

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
