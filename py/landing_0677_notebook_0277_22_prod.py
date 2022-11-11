# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### 001 - REALIZANDO VERIFICAÇÃO DA ESTRUTURA DO DATALAKE
# MAGIC 
# MAGIC O objetivo do projeto se consiste em realizar a obtenção das avaliações dos usuários da loja App Store do aplicativo Caixa Tem.

# COMMAND ----------

# # -------------------------------------------------------------------------------------------------------------------------------------------- #
# # Montando estrutura do datalake
# # -------------------------------------------------------------------------------------------------------------------------------------------- #

# dbutils.fs.mount(
#     source = "wasbs://datalakeCompleteHere@completeHere.blob.core.windows.net/",
#     mount_point = "/mnt/",
#     extra_configs = {"fs.azure.account.key.completeHere":dbutils.secrets.get(scope = "completeHere", key = "completeHere")}
# )

# COMMAND ----------

# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Verificando diretórios do datalake
# -------------------------------------------------------------------------------------------------------------------------------------------- #
display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 002 - OBTENÇÃO DAS AVALIAÇÕES DO APLICATIVO CAIXA TEM - APP STORE - LANDING ZONE

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
from pyspark.sql.functions import col,lit
from pyspark.sql.types import NullType
import pandas as pd


# COMMAND ----------


url = "https://itunes.apple.com/br/rss/customerreviews/id=1485424267/sortBy=mostRecent/xml"

# -------------------------------------------------------------------------------------------------------------------------------------------- #
# tag_pattern armazena uma expressão Regex, que tem como objetivo remover todos os links {*}tag que vem dentro das tags 
# -------------------------------------------------------------------------------------------------------------------------------------------- #
tag_pattern = re.compile(r"\{.*\}")
# -------------------------------------------------------------------------------------------------------------------------------------------- #
# url_pattern armazena uma expressão Regex que tem como objetivo remover um complemento que vem antes da url, por exemplo: ?.link
# -------------------------------------------------------------------------------------------------------------------------------------------- #
url_pattern = re.compile(r"\?.*")

# -------------------------------------------------------------------------------------------------------------------------------------------- #
# def download_url tem como funcionalidade fazer a requisição do xml e forçando uma exceção em de erro
# -------------------------------------------------------------------------------------------------------------------------------------------- #
def download_url(url):    
    try:
        res = requests.get(url)
        res.raise_for_status()  
    except requests.exceptions.HTTPError as errh:
        raise RuntimeError('Problema ao fazer a solicitacao ao HTTP:',errh)
    except requests.exceptions.ConnectionError as errc:
        raise RuntimeError('Problema ao conectar:',errc)
    except requests.exceptions.RequestException as err:
        raise RuntimeError('Problema ao fazer a requisicao da URL:',err)
        
    res.encoding = 'utf-8'
    return res.text

#----------------------------------------------------------------------------------------------------------------------------------------------------- #
# def parse(xml_data) apresenta duas funcionalidades:
#   link_next: Essa funcionalidade tem como objetivo procurar em cada nó link, a URL da próxima página até chegar na última
#   nodes_entry: Essa funcionalidade vai procurar em cada tag <entry> no XML os elementos filhos e irá adicionar dentro de uma lista chamada entries
#----------------------------------------------------------------------------------------------------------------------------------------------------- #
def parse(xml_data):
    
    root = ET.fromstring(xml_data.encode('utf-8'))
    nodes_link = root.findall('{http://www.w3.org/2005/Atom}link')
    
    link_next = None
    for node in nodes_link:
        rel = node.attrib['rel']
        if rel == 'next':
            link_next = node.attrib['href']
        if rel == 'last':
            link_last = node.attrib['href']
    
    nodes_entry = root.findall('{http://www.w3.org/2005/Atom}entry')
    
    entries = []
    for node in nodes_entry:
        entry = {}
        for child in node:sssssssssssssssssssssssssssssssssssssss
            tag = tag_pattern.sub('', child.tag)
            if tag == 'content' and child.attrib['type'] != 'text':
                continue
            entry[tag] = child.text
            
        entries.append(entry)
        
    return link_next, link_last, entries

#-------------------------------------------------------------------------- #
# Função para carga dos dados do URL, concatenando para o dataframe
#-------------------------------------------------------------------------- #
def load_url(url, df):
    xml_data = download_url(url)
    link_next, link_last, entries = parse(xml_data)
    
    if df is None:
        df = DataFrame.from_records(entries)
    else:
        df = concat([df, DataFrame.from_records(entries)], ignore_index=True)
        
    if url_pattern.sub('', url) == url_pattern.sub('', link_last):
        return False, df
    
    return link_next, df


df = None
while url:
    url, df = load_url(url, df)

#-------------------------------------------------------------------------- #
# Atribuição do df pandas para dataframe spark       
#-------------------------------------------------------------------------- #
dfCT = spark.createDataFrame(df) 


#-------------------------------------------------------------------------- #
# Verifica se a coluna é null, caso positivo, converte em String
# Caso a coluna não seja null, mantem a coluna na mesma estrutura          
#-------------------------------------------------------------------------- #
df2 = dfCT.select([
    F.lit(None).cast('string').alias(i.name)
    if isinstance(i.dataType, NullType)
    else i.name
    for i in dfCT.schema
])

#-------------------------------------------------------------------------- #
# Cria colunas de partição chamado ANO e MES
#-------------------------------------------------------------------------- #
dfCaixaTem = df2 \
    .withColumn("ano", col('updated').substr(1,4)) \
    .withColumn("mes", col('updated').substr(6,2))


#-------------------------------------------------------------------------- #
# Realiza gravação dos dados em landing zone no datalake
# PARTIÇÃO: ano / mes (a partir do timestamo do registro)
# MODO: overwrite 
# DIRETÓRIO DATALAKE: /mnt/landing_zone/tb_e_caixaTem
# PROPRIETARIO: Gabriel Oliveira / Gabriel Carvlho
#-------------------------------------------------------------------------- #

(
    dfCaixaTem
    .write
    .partitionBy("ano","mes")
    .mode("overwrite")
    .parquet("/mnt/landing_zone/tb_e_caixaTem")
)

print('''
# ****************************************************************************
                    Gravação dos dados - Parte 1 - Landing - já concluído!
# ****************************************************************************
''')

# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Verificando qualidade primária da ingestão do arquivo
# -------------------------------------------------------------------------------------------------------------------------------------------- #
dfCaixaTem.select("title","ano","mes").groupBy("ano","mes").count().show()

# COMMAND ----------

# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Realizando check da migração dos dados no datalake
# -------------------------------------------------------------------------------------------------------------------------------------------- #
display(dbutils.fs.ls("/mnt/landing_zone"))

# COMMAND ----------


