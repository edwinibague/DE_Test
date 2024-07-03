from prefect import task
import pandas as pd
from datetime import datetime
from dateutil import parser
import numpy as np



def validate_date(Date):
    """
    Se valida que el formato de fecha sea correcto.
    Args: 
        Date (Date); fecha 
    returns:
        Perse_data (Date): fecha corregida.
        None 
    """
    try:
        perse_date = datetime.strptime(Date, "%Y-%m-%d")
        return perse_date
    except ValueError:
        return None



@task
def clean_rows(Data_frame, date_index):
    """
    Limpia cada una de las filas con errores o inecesarias.
    Args:
        Data_frame (DataFRame): dataframe con los datos para ser eliminados.
        date_index (index); indices de fechas que no fue posible modificar.
    returns:
        Data_frame (DAtaFrame): dataframe con data limpia.
    """
    Data_frame = Data_frame.drop(date_index, axis=0)
    
    mask = (Data_frame["cases"] >= 0) & (Data_frame["deaths"] >= 0)
    Data_frame =Data_frame[mask]
    
    Data_frame = Data_frame.dropna(axis=0, how= 'any')
    
    return Data_frame

@task
def Date_validation(Data_frame):
    """
    valida la fecha de la columna date para validar que se encuentre en formato correcto.
    Args:
        Data_frame (DataFrame): dataframe original
    retunrs:
        Data_frame (DataFrame): dataframe con fechas coregidas.
        invalid_index (index): indices que no se pudieron modificar.
    """
    Data_frame["date"] = Data_frame["date"].apply(validate_date)

    invalid_index = Data_frame[Data_frame["date"].isnull()].index.tolist()
    if not invalid_index:
        print("No se requieren correcciones de formato de fecha en el DataFrame")
    else:
        print("correcciones realizadas en  las filas: {invalid_index}")
    
    return Data_frame, invalid_index



@task
def new_cases_deaths(Data_frame):
    """
    almacena una nueva columna en donde se ven reflejados los nuevos casos y muertes por dia.
    Args:
        Data_frame (DataFrame): dataframe corregido para realizar comparaciones.
    returns: 
        Data_frame (DataFrame); data frame con nuevas columnas de nuevos cass y nuevas muertes.
    """
    Data_frame['date'] = pd.to_datetime(Data_frame['date'])
    
    Data_frame = Data_frame.sort_values(by='date')
      
    Data_frame['new_cases'] = Data_frame['cases'].diff()
    Data_frame['new_deaths'] = Data_frame['deaths'].diff()
    
    Data_frame = Data_frame.fillna(0)
    
    return Data_frame


@task
def Data_Quality(Data_frame):
    """
    verificacion de la intefridad de la informacion y eliminacion de datos inncesarios.
    Args: 
        Data_frame (DataFrame): dataframe
    returns:
        Data_frame (DataFrame): dataframe fianl corregido. 
    """
    negative_cases = Data_frame[Data_frame['new_cases'] < 0].index
    negative_deaths = Data_frame[Data_frame['new_deaths']< 0].index
        
    negative_index = np.concatenate((negative_cases, negative_deaths))
    negative_index = np.unique(negative_index)
    
     
    Data_frame.drop(index=negative_index, axis=0)
    Data_frame.drop_duplicates(subset=['date'], inplace=True)
    
    return Data_frame
