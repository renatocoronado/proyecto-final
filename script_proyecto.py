# carga de librerias

import pandas as pd
import numpy as np
import boto3
import psycopg2
import configparser
import io
from sqlalchemy import create_engine


# cargar archivos de configuraciones

config = configparser.ConfigParser()
config.read("credenciales_proyecto.cfg")


# cargar codigo ddl para creacion de tablas

import sql_queries_proyecto


# crear base de datos

try:
    db_conn = psycopg2.connect(
        database=config.get("RDS", "DB_NAME"),
        user=config.get("RDS", "DB_USER"),
        password=config.get("RDS", "DB_PASSWORD"),
        host=config.get("RDS", "DB_HOST"),
        port=config.get("RDS", "DB_PORT"),
    )

    cursor = db_conn.cursor()
    cursor.execute(sql_queries_proyecto.ddl_query)
    db_conn.commit()
    print("Base de Datos Creada Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)


# crear conexion con s3

s3 = boto3.resource(
    service_name="s3",
    region_name="us-east-2",
    aws_access_key_id=config.get("IAM", "ACCESS_KEY"),
    aws_secret_access_key=config.get("IAM", "SECRET_ACCESS_KEY"),
)


# se extraen elementos de cada carpeta del bucket

lista_archivos_ordenes = []
for objt in s3.Bucket("superstorefiles").objects.filter(Prefix="orders/"):
    lista_archivos_ordenes.append(objt.key)

lista_archivos_clientes = []
for objt in s3.Bucket("superstorefiles").objects.filter(Prefix="customer/customer"):
    lista_archivos_clientes.append(objt.key)

lista_archivos_segmentos = []
for objt in s3.Bucket("superstorefiles").objects.filter(Prefix="customer/segment"):
    lista_archivos_segmentos.append(objt.key)

lista_archivos_producto = []
for objt in s3.Bucket("superstorefiles").objects.filter(Prefix="product/product"):
    lista_archivos_producto.append(objt.key)

lista_archivos_sub_categoria = []
for objt in s3.Bucket("superstorefiles").objects.filter(Prefix="product/sub_category"):
    lista_archivos_sub_categoria.append(objt.key)

lista_archivos_codigo_postal = []
for objt in s3.Bucket("superstorefiles").objects.filter(Prefix="location/postal_code"):
    lista_archivos_codigo_postal.append(objt.key)

lista_archivos_estado = []
for objt in s3.Bucket("superstorefiles").objects.filter(Prefix="location/state"):
    lista_archivos_estado.append(objt.key)

lista_archivos_envio = []
for objt in s3.Bucket("superstorefiles").objects.filter(Prefix="shipping/"):
    lista_archivos_envio.append(objt.key)

lista_archivos_calendario = []
for objt in s3.Bucket("superstorefiles").objects.filter(Prefix="calendar/"):
    lista_archivos_calendario.append(objt.key)


# se leen archivos de las listas y se unen

df_ordenes = pd.DataFrame()

for archivo in lista_archivos_ordenes:
    try:
        file = s3.Bucket("superstorefiles").Object(archivo).get()
        data = file["Body"].read()
        ordenes = pd.read_csv(io.BytesIO(data))
        df_ordenes = df_ordenes.append(ordenes)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_clientes = pd.DataFrame()

for archivo in lista_archivos_clientes:
    try:
        file = s3.Bucket("superstorefiles").Object(archivo).get()
        data = file["Body"].read()
        ordenes = pd.read_csv(io.BytesIO(data))
        df_clientes = df_clientes.append(ordenes)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_segmentos = pd.DataFrame()

for archivo in lista_archivos_segmentos:
    try:
        file = s3.Bucket("superstorefiles").Object(archivo).get()
        data = file["Body"].read()
        ordenes = pd.read_csv(io.BytesIO(data))
        df_segmentos = df_segmentos.append(ordenes)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_producto = pd.DataFrame()

for archivo in lista_archivos_producto:
    try:
        file = s3.Bucket("superstorefiles").Object(archivo).get()
        data = file["Body"].read()
        ordenes = pd.read_csv(io.BytesIO(data))
        df_producto = df_producto.append(ordenes)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_sub_categoria = pd.DataFrame()

for archivo in lista_archivos_sub_categoria:
    try:
        file = s3.Bucket("superstorefiles").Object(archivo).get()
        data = file["Body"].read()
        ordenes = pd.read_csv(io.BytesIO(data))
        df_sub_categoria = df_sub_categoria.append(ordenes)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_codigo_postal = pd.DataFrame()

for archivo in lista_archivos_codigo_postal:
    try:
        file = s3.Bucket("superstorefiles").Object(archivo).get()
        data = file["Body"].read()
        ordenes = pd.read_csv(io.BytesIO(data))
        df_codigo_postal = df_codigo_postal.append(ordenes)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_estado = pd.DataFrame()

for archivo in lista_archivos_estado:
    try:
        file = s3.Bucket("superstorefiles").Object(archivo).get()
        data = file["Body"].read()
        ordenes = pd.read_csv(io.BytesIO(data))
        df_estado = df_estado.append(ordenes)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_envio = pd.DataFrame()

for archivo in lista_archivos_envio:
    try:
        file = s3.Bucket("superstorefiles").Object(archivo).get()
        data = file["Body"].read()
        ordenes = pd.read_csv(io.BytesIO(data))
        df_envio = df_envio.append(ordenes)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_calendario = pd.DataFrame()

for archivo in lista_archivos_calendario:
    try:
        file = s3.Bucket("superstorefiles").Object(archivo).get()
        data = file["Body"].read()
        ordenes = pd.read_csv(io.BytesIO(data))
        df_calendario = df_calendario.append(ordenes)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)


# crear sesion para luego insertar data en el rds

database_uri = f"""postgresql://{config.get('RDS', 'DB_USER')}:{config.get('RDS', 'DB_PASSWORD')}@{config.get('RDS', 'DB_HOST')}:{config.get('RDS', 'DB_PORT')}/{config.get('RDS', 'DB_NAME')}"""
engine = create_engine(database_uri)


# insertar dataframes en sus respectivas tablas de la base de datos

df_segmentos.to_sql("segment", engine, if_exists="append", index=False)

df_clientes.to_sql("customer", engine, if_exists="append", index=False)

df_sub_categoria.to_sql("sub_category", engine, if_exists="append", index=False)

df_producto.to_sql("product", engine, if_exists="append", index=False)

df_estado.to_sql("state", engine, if_exists="append", index=False)

df_codigo_postal.to_sql("postal_code", engine, if_exists="append", index=False)

df_envio.to_sql("ship_mode", engine, if_exists="append", index=False)

df_calendario.to_sql("calendar", engine, if_exists="append", index=False)

df_ordenes.to_sql("total_orders", engine, if_exists="append", index=False)
