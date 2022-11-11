# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### 005 - TABELA DE AGREGAÇÃO - GOLD ZONE  

# COMMAND ----------

# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Realizando leitura dos arquivos 
# -------------------------------------------------------------------------------------------------------------------------------------------- #

df005a  = spark.read.option("charset", "ISO-8859-1").parquet("/mnt/silver_zone/tb_t_caixaTem/")
print('''
# ****************************************************************************
                        Leitura completa dos dados!
# ****************************************************************************
''')

# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Criação da temp view do dataframe
# -------------------------------------------------------------------------------------------------------------------------------------------- #
df005a.createOrReplaceTempView("vw_caixaTem_extract_reviews")


# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Visão de quantidade / notas
# -------------------------------------------------------------------------------------------------------------------------------------------- #

df005a = spark.sql("""
SELECT
    CAST(avg(nu_rating_review) AS DECIMAL(18,1)) AS nu_media_reviews_app,
    nu_version_app as nu_versao_app,
    count(distinct(nu_id)) as nu_quantidade_usuarios,
    substr(dt_date,1,7) as pt_ano_mes
FROM vw_caixaTem_extract_reviews
GROUP BY
substr(dt_date,1,7),
nu_version_app
""")

df005b = df005a \
            .coalesce(1)

# -------------------------------------------------------------------------------------------------------------------------------------------- #
# Realiza gravação dos dados em bronze zone no datalake
# PARTIÇÃO: ano / mes (a partir do timestamo do registro)
# MODO: overwrite 
# DIRETÓRIO DATALAKE: /mnt/gold_zone/tb_t_caixaTem
# PROPRIETARIO: Gabriel Oliveira / Gabriel Carvlho
# -------------------------------------------------------------------------------------------------------------------------------------------- #
(
    df005b
    .write
    .partitionBy("pt_ano_mes")
    .mode("overwrite")
    .parquet("/mnt/gold_zone/tb_t_caixaTem")
)
print('''
# ****************************************************************************
            Gravação dos dados - Parte 4 - Gold - já concluído!
# ****************************************************************************
''')

# COMMAND ----------

df005b.sort('pt_ano_mes','nu_versao_app','nu_media_reviews_app','nu_quantidade_usuarios').show()

# COMMAND ----------


