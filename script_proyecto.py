#%%
# carga de librerias

import pandas as pd
import numpy as np
import boto3
import psycopg2
import configparser

#%%
# cargar archivos de configuraciones

config = configparser.ConfigParser()
config.read('credenciales_proyecto.cfg')

#%%
# cargar codigo ddl para creacion de tablas

import sql_queries_proyecto

#%%
# crear base de datos

try:
    db_conn = psycopg2.connect(
        database=config.get('RDS', 'DB_NAME'), 
        user=config.get('RDS', 'DB_USER'),
        password=config.get('RDS', 'DB_PASSWORD'), 
        host=config.get('RDS', 'DB_HOST'),
        port=config.get('RDS', 'DB_PORT')
    )

    cursor = db_conn.cursor()
    cursor.execute(sql_queries_proyecto.ddl_query)
    db_conn.commit()
    print("Base de Datos Creada Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)

#%%



#%%


#%%