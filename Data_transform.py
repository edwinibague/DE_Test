from prefect import task
import pandas as pd
import datetime
from dateutil import parser

@task("Datos faltantes")
def clean_rows(df):
    
    index_null = df.isnull().any(axis=1)
    return df.drop(index_null.index, axis = 0)

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
