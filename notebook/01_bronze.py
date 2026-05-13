# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Bronze Layer
# MAGIC ## Ingestão dos dados: LANDING → BRONZE
# MAGIC
# MAGIC Lê os arquivos CSV do schema **LANDING** e grava no formato **Delta Lake** no schema **BRONZE**.
# MAGIC
# MAGIC > Delta Lake garante transações ACID, versionamento e time travel.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze
# MAGIC COMMENT 'Schema Bronze - dados brutos armazenados em formato Delta Lake';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Bronze: PARTIDOS

# COMMAND ----------

df_partidos = spark.table("landing.partidos")
df_partidos.write.format("delta").mode("overwrite").saveAsTable("bronze.partidos")

print(f"✔ bronze.partidos: {df_partidos.count()} registros")
df_partidos.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Bronze: PARLAMENTARES

# COMMAND ----------

df_parlamentares = spark.table("landing.parlamentares")
df_parlamentares.write.format("delta").mode("overwrite").saveAsTable("bronze.parlamentares")

print(f"✔ bronze.parlamentares: {df_parlamentares.count()} registros")
df_parlamentares.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Bronze: CATEGORIAS_DESPESA

# COMMAND ----------

df_categorias = spark.table("landing.categorias_despesa")
df_categorias.write.format("delta").mode("overwrite").saveAsTable("bronze.categorias_despesa")

print(f"✔ bronze.categorias_despesa: {df_categorias.count()} registros")
df_categorias.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Bronze: FORNECEDORES

# COMMAND ----------

df_fornecedores = spark.table("landing.fornecedores")
df_fornecedores.write.format("delta").mode("overwrite").saveAsTable("bronze.fornecedores")

print(f"✔ bronze.fornecedores: {df_fornecedores.count()} registros")
df_fornecedores.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Bronze: DESPESAS

# COMMAND ----------

df_despesas = spark.table("landing.despesas")
df_despesas.write.format("delta").mode("overwrite").saveAsTable("bronze.despesas")

print(f"✔ bronze.despesas: {df_despesas.count()} registros")
df_despesas.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificação: histórico Delta Lake (time travel)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze.despesas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sumário da Camada Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'partidos'           AS tabela, COUNT(*) AS total_registros FROM bronze.partidos
# MAGIC UNION ALL
# MAGIC SELECT 'parlamentares',                COUNT(*) FROM bronze.parlamentares
# MAGIC UNION ALL
# MAGIC SELECT 'categorias_despesa',           COUNT(*) FROM bronze.categorias_despesa
# MAGIC UNION ALL
# MAGIC SELECT 'fornecedores',                 COUNT(*) FROM bronze.fornecedores
# MAGIC UNION ALL
# MAGIC SELECT 'despesas',                     COUNT(*) FROM bronze.despesas
# MAGIC ORDER BY tabela
