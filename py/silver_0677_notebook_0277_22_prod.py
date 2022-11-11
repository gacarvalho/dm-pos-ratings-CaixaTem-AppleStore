# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### 003 - MANTENDO LINHA HISTÓRICA DOS DADOS - SILVER ZONE  

# COMMAND ----------

# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Importando bibliotecas necessarias
# -------------------------------------------------------------------------------------------------------------------------------------------- #

from __future__ import print_function 
import re
import sys 
from pandas import DataFrame, concat
import requests
import xml.etree.ElementTree as ET
import logging
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import NullType
from pyspark.sql.functions import *
import pandas as pd


# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Realizando leitura dos arquivos + Interpretando como temp view
# -------------------------------------------------------------------------------------------------------------------------------------------- #
df004a  = spark.read.option("charset", "ISO-8859-1").parquet("/mnt/bronze_zone/tb_t_caixaTem/")
print('Leitura completa dos dados!')


# -------------------------------------------------------------------------------------------------------------------------------------------- #
# RReplicação dos dados com registro do current timestamp
# -------------------------------------------------------------------------------------------------------------------------------------------- #
df004b = df004a \
            .withColumn('ts_gravado_em', current_timestamp()) \
            .coalesce(1)


# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Realiza gravação dos dados em bronze zone no datalake
# PARTIÇÃO: ano / mes (a partir do timestamo do registro)
# MODO: overwrite 
# DIRETÓRIO DATALAKE: /mnt/bronze_zone/tb_e_caixaTem
# PROPRIETARIO: Gabriel Oliveira / Gabriel Carvlho
# -------------------------------------------------------------------------------------------------------------------------------------------- #
(
    df004b
    .write
    .partitionBy("pt_ano","pt_mes")
    .mode("overwrite")
    .parquet("/mnt/silver_zone/tb_t_caixaTem")
)
print('''
# ****************************************************************************
            Gravação dos dados - Parte 3 - Silver - já concluído!
# ****************************************************************************
''')

# COMMAND ----------

df004b.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Testes: Documentação dos testes da aplicação pyspark;
# MAGIC 
# MAGIC O objetivo dessa tarefa é realizar testes aplicados ao dataset obtido na ingestão dos dados, seguindo o padrão exigido pelo quadro abaixo.

# COMMAND ----------

df004b.createOrReplaceTempView("vw_caixaTem")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC with base as ( SELECT DISTINCT * FROM vw_caixaTem )
# MAGIC 
# MAGIC SELECT count(*) as total_registros, substr(dt_date,1,7) as data FROM base  GROUP BY substr(dt_date,1,7);
# MAGIC 
# MAGIC 
# MAGIC -- * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 
# MAGIC --  Exibindo o total de registros por ano-mes
# MAGIC -- * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) as total, nu_sum_votes_reviews, ds_count_votes_reviews  FROM vw_caixaTem GROUP BY nu_sum_votes_reviews, ds_count_votes_reviews ORDER BY nu_sum_votes_reviews, ds_count_votes_reviews DESC
# MAGIC 
# MAGIC -- * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 
# MAGIC --  Count total por número de soma de votos e count total de votos por pessoal
# MAGIC -- * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) as total, nu_rating_review  FROM vw_caixaTem GROUP BY nu_rating_review ORDER BY nu_rating_review DESC
# MAGIC 
# MAGIC -- * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 
# MAGIC --  Count total por avaliação
# MAGIC -- * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) as total, nu_version_app  FROM vw_caixaTem GROUP BY nu_version_app ORDER BY nu_version_app DESC
# MAGIC 
# MAGIC -- * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 
# MAGIC --  Count total por versão do app
# MAGIC -- * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC (SELECT ds_title, ds_reviews, '202206' as ano FROM vw_caixaTem WHERE substr(dt_date,1,7) = '2022-06' limit 3)
# MAGIC UNION ALL
# MAGIC (SELECT ds_title, ds_reviews, '202207' as ano FROM vw_caixaTem WHERE substr(dt_date,1,7) = '2022-07' limit 3)

# COMMAND ----------



# COMMAND ----------

df_evd = spark.sql(''' SELECT * FROM vw_caixaTem''')

df_evd.limit(5).toPandas()


# COMMAND ----------


