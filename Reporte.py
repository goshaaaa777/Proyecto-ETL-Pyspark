# Databricks notebook source
from pyspark.sql.types import StructType, StructField

from pyspark.sql.types import StringType, IntegerType, DoubleType

from pyspark.sql.types import *

import pyspark.sql.functions as f

# COMMAND ----------

#Lectura desde HIVE
dfTablon = spark.sql("SELECT * FROM PROYECTO.TABLON")

#Mostramos los datos
dfTablon.show()

# COMMAND ----------

#Calculamos las reglas comunes en un dataframe
dfTablon1 = dfTablon.filter(
  (dfTablon["MONTO"] > 500) &
  (dfTablon["NOMBRE_EMPRESA_TRANSACCION"] == "Amazon")
)
dfTablon1.show()

# COMMAND ----------

#REPORTE 1:
# - POR PERSONAS ENTRE 30 A 39 AÑOS
# - CON UN SALARIO DE 1000 A 5000 DOLARES

dfReporte1 = dfTablon1.filter(
  (dfTablon1["EDAD"] >= 30) &
  (dfTablon1["EDAD"] <= 39) &
  (dfTablon1["SALARIO"] >= 1000) &
  (dfTablon1["SALARIO"] <= 5000)
)
dfReporte1.show()

# COMMAND ----------

#REPORTE 2:
# - POR PERSONAS ENTRE 40 A 49 AÑOS
# - CON UN SALARIO DE 2500 A 7000 DOLARES
dfReporte2 = dfTablon1.filter(
  (dfTablon1["EDAD"] >= 40) &
  (dfTablon1["EDAD"] <= 49) &
  (dfTablon1["SALARIO"] >= 2500) &
  (dfTablon1["SALARIO"] <= 7000)
)

#Mostramos los datos
dfReporte2.show()

# COMMAND ----------

#REPORTE 3:
# - POR PERSONAS ENTRE 50 A 60 AÑOS
# - CON UN SALARIO DE 3500 A 10000 DOLARES
dfReporte3 = dfTablon1.filter(
  (dfTablon1["EDAD"] >= 50) &
  (dfTablon1["EDAD"] <= 60) &
  (dfTablon1["SALARIO"] >= 3500) &
  (dfTablon1["SALARIO"] <= 10000)
)
dfReporte3.show()