# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### 002 - REALIZANDO TRATAMENTO DE DADOS E CRIANDO LINHA HISTÓRICA DOS DADOS NO DATALAKE - BRONZE ZONE  

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

df003 = spark.read.option("charset", "ISO-8859-1").parquet("/mnt/landing_zone/tb_e_caixaTem/")
print('''
# ****************************************************************************
                          Leitura completa dos dados!
# ****************************************************************************
''')



# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Iniciando tratamento dos nome das colunas e ordenando por ano - mes - avaliação (nota) - versão do app
# -------------------------------------------------------------------------------------------------------------------------------------------- #
df003a = df003 \
    .withColumnRenamed("id","nu_id") \
    .withColumnRenamed("title","ds_title") \
    .withColumnRenamed("contextType","ds_type_review") \
    .withColumnRenamed("voteSum","nu_sum_votes_reviews") \
    .withColumnRenamed("voteCount","ds_count_votes_reviews") \
    .withColumnRenamed("rating","nu_rating_review") \
    .withColumnRenamed("updated","ts_review") \
    .withColumnRenamed("version","nu_version_app") \
    .withColumnRenamed("author","ds_desc_author") \
    .withColumnRenamed("link","ds_link") \
    .withColumnRenamed("ano","pt_ano") \
    .withColumnRenamed("mes","pt_mes") \
    .orderBy("ano","mes","rating","version")


# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Na criação de um novo dataframe, realizamos a remoção dos acentos e emojis
# -------------------------------------------------------------------------------------------------------------------------------------------- #
@F.pandas_udf('string')
def strip_accents(s: pd.Series) -> pd.Series:
    return s.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

df003b = df003a.withColumn('ds_title', strip_accents('ds_title')) \
            .withColumn('ds_reviews', strip_accents('content'))


# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Padronização do dataset
# -------------------------------------------------------------------------------------------------------------------------------------------- #
df003c = df003b.select("nu_id","ds_title","ds_reviews","contentType","nu_sum_votes_reviews","ds_count_votes_reviews", "nu_rating_review",	"ts_review",	"nu_version_app",	"ds_desc_author",	"ds_link",	"pt_ano", "pt_mes")	\
            .withColumn("ds_title", upper(col('ds_title')))\
            .withColumn("ds_reviews", upper(col('ds_reviews')))

# Verificando as colunas extras da extração: 
# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Essas colunas vieram extra na extração dos dados, apesar da Apple não disponibilizar um dicionario, essas colunas vieram todas nulas
# -------------------------------------------------------------------------------------------------------------------------------------------- #
# - [x] ```contentType```
# - [x] ```ds_desc_author```
# - [x] ```ds_link```

df003c.select('ds_link', 'ds_desc_author', 'contentType').distinct().show()


# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Padronização do dataset
# -------------------------------------------------------------------------------------------------------------------------------------------- #
df003d = df003c \
    .withColumn("ds_title", upper(col('ds_title')))\
    .withColumn("ds_reviews", upper(col('ds_reviews'))) \
    .withColumn('dt_date', to_date(col('ts_review'))) \
    .withColumn('hr_review', col('ts_review').substr(12,8)) \
    .distinct()
    

df003f = df003d \
        .select(
            "nu_id","ds_title","ds_reviews","nu_sum_votes_reviews","ds_count_votes_reviews", "nu_rating_review","dt_date","hr_review","ts_review","nu_version_app","pt_ano", "pt_mes"
        ) \
        .orderBy("ano","mes","dt_date","hr_review","nu_version_app","nu_rating_review")


#-------------------------------------------------------------------------- #
# Realiza gravação dos dados em bronze zone no datalake
# PARTIÇÃO: ano / mes (a partir do timestamo do registro)
# MODO: overwrite 
# DIRETÓRIO DATALAKE: /mnt/bronze_zone/tb_e_caixaTem
# PROPRIETARIO: Gabriel Oliveira / Gabriel Carvlho
#-------------------------------------------------------------------------- #
(
    df003f
    .write
    .partitionBy("pt_ano","pt_mes")
    .mode("overwrite")
    .parquet("/mnt/bronze_zone/tb_t_caixaTem")
)
print('''
# ****************************************************************************
                    Gravação dos dados - Parte 2 - Bronze - já concluído!
# ****************************************************************************
''')

# COMMAND ----------


