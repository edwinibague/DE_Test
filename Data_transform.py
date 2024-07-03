from prefect import task
import pandas as pd
from datetime import datetime
from dateutil import parser
import numpy as np



def validate_date(Date):
    try:
        perse_date = datetime.strptime(Date, "%Y-%m-%d")
        return perse_date
    except ValueError:
        return None



@task
def clean_rows(Data_frame, date_index):
    
    Data_frame = Data_frame.drop(date_index, axis=0)
    
    mask = (Data_frame["cases"] >= 0) & (Data_frame["deaths"] >= 0)
    Data_frame =Data_frame[mask]
    
    Data_frame = Data_frame.dropna(axis=0, how= 'any')
    
    return Data_frame

@task
def Date_validation(Data_frame):
    
    Data_frame["date"] = Data_frame["date"].apply(validate_date)

    invalid_index = Data_frame[Data_frame["date"].isnull()].index.tolist()
    if not invalid_index:
        print("No se requieren correcciones de formato de fecha en el DataFrame")
    else:
        print("correcciones realizadas en  las filas: {invalid_index}")
    
    return Data_frame, invalid_index



@task
def new_cases_deaths(Data_frame):
    Data_frame['date'] = pd.to_datetime(Data_frame['date'])
    
    Data_frame = Data_frame.sort_values(by='date')
      
    Data_frame['new_cases'] = Data_frame['cases'].diff()
    Data_frame['new_deaths'] = Data_frame['deaths'].diff()
    
    Data_frame = Data_frame.fillna(0)
    
    return Data_frame


@task
def Data_Quality(Data_frame):
    
    negative_cases = Data_frame[Data_frame['new_cases'] < 0].index
    negative_deaths = Data_frame[Data_frame['new_deaths']< 0].index
        
    negative_index = np.concatenate((negative_cases, negative_deaths))
    negative_index = np.unique(negative_index)
    
     
    Data_frame.drop(index=negative_index, axis=0)
    Data_frame.drop_duplicates(subset=['date'], inplace=True)
    
    return Data_frame


@task
def Create_data_frame(Data):
    df = pd.read_csv(Data)
    return df