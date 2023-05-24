# Databricks notebook source
#Estos objetos nos ayudarán a definir la metadata
from pyspark.sql.types import StructType, StructField

from pyspark.sql.types import StringType, IntegerType, DoubleType

from pyspark.sql.types import *

#Importamos la librería de funciones clasicas
import pyspark.sql.functions as f


# COMMAND ----------

#Lectura desde HIVE
dfPersona = spark.sql("SELECT * FROM PROYECTO.PERSONA")

#Mostramos los datos
dfPersona.show()

# COMMAND ----------

dfEmpresa = spark.sql("SELECT * FROM PROYECTO.EMPRESA")

#Mostramos los datos
dfEmpresa.show()

# COMMAND ----------

dfTransaccion = spark.sql("SELECT * FROM PROYECTO.TRANSACCION")

#Mostramos los datos
dfTransaccion.show()

# COMMAND ----------

#Procesamiento
#PASO 1: OBTENEMOS LOS DATOS DE LA PERSONA QUE HIZO LA TRANSACCIÓN
df1 = dfTransaccion.alias("T").join(
  dfPersona.alias("P"),
  f.col("T.ID_PERSONA") == f.col("P.ID")
).select(
  f.col("P.ID").alias("ID_PERSONA"),
  f.col("P.NOMBRE"),
  f.col("P.TELEFONO"),
  f.col("P.CORREO"),
  f.col("P.FECHA_INGRESO"),
  f.col("P.EDAD"),
  f.col("P.SALARIO"),
  f.col("P.ID_EMPRESA").alias("ID_EMPRESA_TRABAJO"),
  f.col("T.ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION"),
  f.col("T.MONTO"),
  f.col("T.FECHA")
)
#Mostramos los datos
df1.show()

# COMMAND ----------

#PASO 2: OBTENEMOS EL NOMBRE DE LA EMPRESA DONDE TRABAJA LA PERSONA
df2 = df1.alias("T").join(
  dfEmpresa.alias("E"),
  f.col("T.ID_EMPRESA_TRABAJO") == f.col("E.ID")
).select(
  f.col("T.ID_PERSONA"),
  f.col("T.NOMBRE").alias("NOMBRE_PERSONA"),
  f.col("T.TELEFONO"),
  f.col("T.CORREO"),
  f.col("T.FECHA_INGRESO"),
  f.col("T.EDAD"),
  f.col("T.SALARIO"),
  f.col("T.ID_EMPRESA_TRABAJO"),
  f.col("E.NOMBRE").alias("NOMBRE_EMPRESA_TRABAJO"),
  f.col("T.ID_EMPRESA_TRANSACCION"),
  f.col("T.MONTO"),
  f.col("T.FECHA")
)

#Mostramos los datos
df2.show()

# COMMAND ----------

#PASO 3: OBTENEMOS EL NOMBRE DE LA EMPRESA EN DONDE SE REALIZÓ LA TRANSACCIÓN
dfTablon = df2.alias("T").join(
  dfEmpresa.alias("E"),
  f.col("T.ID_EMPRESA_TRANSACCION") == f.col("E.ID")
).select(
  f.col("T.ID_PERSONA"),
  f.col("T.NOMBRE_PERSONA"),
  f.col("T.TELEFONO"),
  f.col("T.CORREO"),
  f.col("T.FECHA_INGRESO"),
  f.col("T.EDAD"),
  f.col("T.SALARIO"),
  f.col("T.ID_EMPRESA_TRABAJO"),
  f.col("T.NOMBRE_EMPRESA_TRABAJO"),
  f.col("T.ID_EMPRESA_TRANSACCION"),
  f.col("E.NOMBRE").alias("NOMBRE_EMPRESA_TRANSACCION"),
  f.col("T.MONTO"),
  f.col("T.FECHA")
)

#Mostramos los datos
dfTablon.show()

# COMMAND ----------

#Convertimos el dataframe en una vista temporal
dfTablon.createOrReplaceTempView("dfTablon")

#Lo guardamos en la tabla Hive con Spark SQL
spark.sql("""
INSERT INTO PROYECTO.TABLON
  SELECT
    *
  FROM
    dfTablon
""")
#Verificamos
spark.sql("SELECT * FROM PROYECTO.TABLON").show()