-- Databricks notebook source
CREATE DATABASE PROYECTO LOCATION '/databases/PROYECTO';

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

CREATE TABLE PROYECTO.PERSONA(
    ID STRING,
    NOMBRE STRING,
    TELEFONO STRING,
    CORREO STRING,
    FECHA_INGRESO STRING,
    EDAD INT,
    SALARIO DOUBLE,
    ID_EMPRESA STRING
)
STORED AS PARQUET
LOCATION '/databases/PROYECTO/PERSONA'
TBLPROPERTIES(
    'parquet.compression'='SNAPPY',
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- [Hive] Creamos una tabla
CREATE TABLE PROYECTO.EMPRESA(
    ID STRING,
    NOMBRE STRING
)
STORED AS PARQUET
LOCATION '/databases/PROYECTO/PERSONA'
TBLPROPERTIES(
    'parquet.compression'='SNAPPY',
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

CREATE TABLE PROYECTO.TRANSACCION(
    ID_PERSONA STRING,
    ID_EMPRESA STRING,
    MONTO DOUBLE,
    FECHA STRING
)
STORED AS PARQUET
LOCATION '/databases/PROYECTO/TRANSACCION'
TBLPROPERTIES(
    'parquet.compression'='SNAPPY',
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);


-- COMMAND ----------

CREATE TABLE PROYECTO.TABLON(
    ID_PERSONA STRING,
    NOMBRE_PERSONA STRING,
    TELEFONO STRING,
    CORREO STRING,
    FECHA_INGRESO STRING,
    EDAD INT,
    SALARIO DOUBLE,
    ID_EMPRESA_TRABAJO STRING,
    NOMBRE_EMPRESA_TRABAJO STRING,
    ID_EMPRESA_TRANSACCION STRING,
    NOMBRE_EMPRESA_TRANSACCION STRING,
    MONTO DOUBLE,
    FECHA STRING
)
STORED AS PARQUET
LOCATION '/databases/PROYECTO/TABLON'
TBLPROPERTIES(
    'parquet.compression'='SNAPPY',
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);