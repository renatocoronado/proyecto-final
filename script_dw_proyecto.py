# carga de librerias

import pandas as pd
import numpy as np
import boto3
import psycopg2
import configparser
import io
from sqlalchemy import create_engine
import pymysql


# cargar archivos de configuraciones

config = configparser.ConfigParser()
config.read("credenciales_proyecto.cfg")


# crear driver

database_uri = f"""postgresql://{config.get('RDS', 'DB_USER')}:{config.get('RDS', 'DB_PASSWORD')}@{config.get('RDS', 'DB_HOST')}:{config.get('RDS', 'DB_PORT')}/{config.get('RDS', 'DB_NAME')}"""


# importar tabla de cliente

sql_query_customer = "SELECT * FROM customer;"
df_customer = pd.read_sql(sql_query_customer, database_uri)


# importar tabla de segmento

sql_query_segment = "SELECT * FROM segment;"
df_segment = pd.read_sql(sql_query_segment, database_uri)


# crear dimesion de cliente

df_customer_dim = df_customer.merge(df_segment, on="segment_key", how="inner").drop(
    ["segment_key"], axis=1
)


# importar tabla producto

sql_query_product = "SELECT * FROM product;"
df_product = pd.read_sql(sql_query_product, database_uri)


# importar tabla sub categoria

sql_query_sub_category = "SELECT * FROM sub_category;"
df_sub_category = pd.read_sql(sql_query_sub_category, database_uri)


# crear dimension producto

df_product_dim = df_product.merge(
    df_sub_category, on="sub_category_key", how="inner"
).drop(["sub_category_key"], axis=1)


# importar tabla codigo postal

sql_query_postal_code = "SELECT * FROM postal_code;"
df_postal_code = pd.read_sql(sql_query_postal_code, database_uri)


# importar tabla estado

sql_query_state = "SELECT * FROM state;"
df_state = pd.read_sql(sql_query_state, database_uri)


# crear dimension localidad

df_location_dim = df_postal_code.merge(df_state, on="state_key", how="inner").drop(
    ["state_key"], axis=1
)


# importar y crear dimension tipo de envio

sql_query_ship_mode = "SELECT * FROM ship_mode;"
df_ship_mode = pd.read_sql(sql_query_ship_mode, database_uri)


# importar y crear dimension de calendario

sql_query_calendar = "SELECT * FROM calendar;"
df_calendar = pd.read_sql(sql_query_calendar, database_uri)


# ajustar tipos de datos en tabla calendario

df_calendar["full_date"] = pd.to_datetime(df_calendar["full_date"])

df_calendar["week_begin_date"] = pd.to_datetime(df_calendar["week_begin_date"])

df_calendar["same_day_year_ago_date"] = pd.to_datetime(
    df_calendar["same_day_year_ago_date"]
)


# importar y crear tabla de hechos

sql_query_total_orders = "SELECT * FROM total_orders;"
df_total_orders = pd.read_sql(sql_query_total_orders, database_uri)


# agregar columna con llave de fecha

df_total_orders["date_key"] = (
    pd.to_datetime(df_total_orders["order_date"], format="%Y-%m-%d")
    .dt.strftime("%Y%m%d")
    .astype(int)
)


# quitar columnas innecesarias en df ordenes

df_total_orders = df_total_orders.drop(["order_date"], axis=1)


# crear conexion

aws_conn = boto3.client(
    "rds",
    aws_access_key_id=config.get("IAM", "ACCESS_KEY"),
    aws_secret_access_key=config.get("IAM", "SECRET_ACCESS_KEY"),
    region_name="us-east-2",
)


# crear instancia de base de datos

try:
    response = aws_conn.create_db_instance(
        AllocatedStorage=10,
        DBName=config.get("RDS_MYSQL", "DB_NAME"),
        DBInstanceIdentifier="dw-db",
        DBInstanceClass="db.t3.micro",
        Engine="mysql",
        MasterUsername=config.get("RDS_MYSQL", "DB_USER"),
        MasterUserPassword=config.get("RDS_MYSQL", "DB_PASSWORD"),
        Port=int(config.get("RDS_MYSQL", "DB_PORT")),
        VpcSecurityGroupIds=[config.get("VPC", "SECURITY_GROUP")],
        PubliclyAccessible=True,
    )
    print(response)
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")


# cargar codigo sql para creacion de tablas

import sql_queries_dw_proyecto


# crear data warehouse

import mysql.connector as mysqlC

try:
    myDw = mysqlC.connect(
        host=config.get("RDS_MYSQL", "DB_HOST"),
        user=config.get("RDS_MYSQL", "DB_USER"),
        password=config.get("RDS_MYSQL", "DB_PASSWORD"),
        database=config.get("RDS_MYSQL", "DB_NAME"),
    )

    mycursor = myDw.cursor()
    mycursor.execute(sql_queries_dw_proyecto.CREATE_DW, multi=True)
    myDw.commit()
    print("Data Warehouse Creado Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)


# crear driver

mysql_driver = f"""mysql+pymysql://{config.get('RDS_MYSQL', 'DB_USER')}:{config.get('RDS_MYSQL', 'DB_PASSWORD')}@{config.get('RDS_MYSQL', 'DB_HOST')}:{config.get('RDS_MYSQL', 'DB_PORT')}/{config.get('RDS_MYSQL', 'DB_NAME')}"""


# insertar informacion en tablas

df_customer_dim.to_sql(
    "customer_dimension", mysql_driver, index=False, if_exists="append"
)

df_product_dim.to_sql(
    "product_dimension", mysql_driver, index=False, if_exists="append"
)

df_location_dim.to_sql(
    "location_dimension", mysql_driver, index=False, if_exists="append"
)

df_ship_mode.to_sql("ship_mode", mysql_driver, index=False, if_exists="append")

df_calendar.to_sql("date_dimension", mysql_driver, index=False, if_exists="append")

df_total_orders.to_sql(
    "retail_sales_fact", mysql_driver, index=False, if_exists="append"
)
