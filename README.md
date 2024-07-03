# DE_Test

En este proyecto se quiere realizar la solucion al test para Data engineer que se encuentra en el repositorio https://github.com/martomor/de_test_case?tab=readme-ov-file para lo cual se ha seguido el paso a paso del repositorio solo con la omicion de la tarea de secuencias de comandos SQL. 

El proceso ETL que se ha realizado mediante el orquestador prefect cuenta con las diferentes etapas las cuales se dividieron en 3. cada unsa se explica acontinuacion.

project_root/
├── ETL_workflow.py        # Main script with Prefect flow definition <br />
├── Data_transform.py      # Script for data transformation logic <br />
├── Load_Data.py           # Script for data upload functions <br />
├── credentials.py         # (Optional) Script for storing credentials <br />
├── requirements.txt      # File listing project dependencies <br />
└── README.md             # This documentation file <br />


## Extraccion 
El proceso de extraccion de la informacion se ha realizado de manera rapida, para ello se ha hecho uso de la libreria de pandas con la cual se ha obtenido el archivo desde el repositorio de origen: https://github.com/nytimes/covid-19-data/blob/master/us.csv, a este link se le ha agregado ?raw=true con lo cual se crea el formato raw del archivo de origen para que al leer desde el repositorio, este archivo solo contenga los datos que son necesarios.
Se ha definido la tarea extract_data, en la cual se llama la libreria pandas y se ha leido como csv teniendo encuenta de dejar la cabecera del formato original.

## Transformacion
El proceso de transformacion ha sido el proceso con mas aspectos por revisar, es por ello que se decidio manejar de manera que se tengan operaciones divididas de manera simple y legible, para ello se creo el archivo ./Data_transform.py el cual contiene las diferentes definiciones necesarias para la trasnformacon de la informacion.

las diferentes tareas son 4 principales que se han creado: 
### Date validation, 
Esta primera tarea se encarga de la limpiesa de las fechas y la correccion del formato, se ha creado con la capacidad para leer todo el dataframe y por medio de una nueva definicion se hace la validacion  de cada uno de los campos, para ello se ha hecho uso de la opcion aply de los dataframe con la cual se puede correr sobre toda la columna de datos un unico codigo para comprobar la estructura de la fecha de los datos, de esta manera se valida cada una de las fechas. Se realiza el retorno de el data frame modificado con las fechas en el formato correcto y de aquellas que no pudieron ser modificadas, se almacena el indice para su posterior uso.

### Clean Rows

Esta tarea limpia todos los campos invalidos del dataframe, dado que se ha realizado primero el cambio de las fechas, se hace uso de los datos de indexacion que se almacenaron en este proceso y de esta manera son los primeros en eliminarce, luego se hace un varido por todo el dataframe para validar que no se encuentren espacios vacios y en el caso de los datos de casos y muertes, que estos no se encuentren en negativo, de tal manera limpiando todo el dataframe para us siguiente uso.

### New cases/deaths 
Dado que la necesidad de los nuevos casos y muertes que se presentan a lo largo del tiempo en el dataset, se ha hecho un barrido de toda la informacion y se ha decidido hacer uso de la funcion diff de los dataframes, con esta se ha podido calcular la diferencia entre cada uno de las filas y obtener la diferenia. Los valores obtenidos se agregan a nuevas columnas las cuales se han nombrado new cases y new_deaths respectivamente.

### Data Quality

para que la informacion sea valida y tenga los formatos requeridos, se realiza una ultima validacion de todos los valores, esto implica validar que no se encuentren valores negativos en los campos numericos y que no se encuentren espacions nullos o en blanco. 


con este rpocesamiento se obtiene un data frame listo para ser usado de la manera que se requiera y ser almacenado.

## Carga 

Para lofrar la carga se ha hecho uso de las librerias de bigquery por medio de prefct_gcp con lo cual se hace el anclaje de las credenciales para poder hacer la carga, para ello se hacreado un nuevo archivo llamado .load_data.py en el cual se almacenan los datos hacia gcp.

## observaciones generales 