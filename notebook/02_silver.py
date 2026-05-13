# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - Silver Layer
# MAGIC ## Data Quality: BRONZE → SILVER
# MAGIC
# MAGIC Aplica regras de **Data Quality** nos dados do schema BRONZE e persiste os dados
# MAGIC tratados e confiáveis no schema **SILVER** em formato Delta Lake.
# MAGIC
# MAGIC ### Regras de Data Quality aplicadas
# MAGIC | Regra | Descrição |
# MAGIC |-------|-----------|
# MAGIC | Nulos obrigatórios | Remove linhas com PKs ou campos críticos nulos |
# MAGIC | Valores negativos | Valores monetários devem ser >= 0 |
# MAGIC | Consistência monetária | `vl_liquido = vl_documento - vl_glosa` (tolerância 0.01) |
# MAGIC | Mês válido | `nu_mes` entre 1 e 12 |
# MAGIC | Ano válido | `nu_ano` >= 2000 |
# MAGIC | UF válida | `sg_uf` deve ter exatamente 2 caracteres |
# MAGIC | Tipo fornecedor | Deve ser "Pessoa Juridica" ou "Pessoa Fisica" |
# MAGIC | Integridade referencial | Chaves estrangeiras devem existir na tabela pai |
# MAGIC | Padronização strings | trim + upper em campos de código |
# MAGIC | Coluna de controle | Adiciona `dt_carga` e `fl_ativo` |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver
# MAGIC COMMENT 'Schema Silver - dados tratados e confiáveis em formato Delta Lake';

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Silver: PARTIDOS

# COMMAND ----------

df_bronze = spark.table("bronze.partidos")

df_silver = (
    df_bronze
    .withColumn("cd_partido", F.col("cd_partido").cast(IntegerType()))
    .withColumn("sg_partido",  F.trim(F.upper(F.col("sg_partido"))))
    .withColumn("nm_partido",  F.trim(F.col("nm_partido")))
    .filter(F.col("cd_partido").isNotNull())
    .filter(F.col("sg_partido").isNotNull() & (F.col("sg_partido") != ""))
    .filter(F.col("nm_partido").isNotNull() & (F.col("nm_partido") != ""))
    .withColumn("dt_carga", F.current_timestamp())
    .withColumn("fl_ativo",  F.lit(True))
)

removidos = df_bronze.count() - df_silver.count()
print(f"✔ silver.partidos | Bronze: {df_bronze.count()} | Silver: {df_silver.count()} | Removidos (DQ): {removidos}")
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.partidos")
df_silver.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Silver: PARLAMENTARES

# COMMAND ----------

df_bronze = spark.table("bronze.parlamentares")

df_silver = (
    df_bronze
    .withColumn("cd_parlamentar", F.col("cd_parlamentar").cast(IntegerType()))
    .withColumn("cd_partido",      F.col("cd_partido").cast(IntegerType()))
    .withColumn("nu_legislatura",  F.col("nu_legislatura").cast(IntegerType()))
    .withColumn("nm_parlamentar",  F.trim(F.col("nm_parlamentar")))
    .withColumn("sg_uf",           F.trim(F.upper(F.col("sg_uf"))))
    .filter(F.col("cd_parlamentar").isNotNull())
    .filter(F.col("nm_parlamentar").isNotNull() & (F.col("nm_parlamentar") != ""))
    .filter(F.col("sg_uf").isNotNull() & (F.length(F.col("sg_uf")) == 2))
    .filter(F.col("cd_partido").isNotNull())
    .filter(F.col("nu_legislatura") > 0)
    .join(
        spark.table("silver.partidos").select("cd_partido").distinct(),
        "cd_partido", "inner"
    )
    .withColumn("dt_carga", F.current_timestamp())
    .withColumn("fl_ativo",  F.lit(True))
)

removidos = df_bronze.count() - df_silver.count()
print(f"✔ silver.parlamentares | Bronze: {df_bronze.count()} | Silver: {df_silver.count()} | Removidos (DQ): {removidos}")
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.parlamentares")
df_silver.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Silver: CATEGORIAS_DESPESA

# COMMAND ----------

df_bronze = spark.table("bronze.categorias_despesa")

df_silver = (
    df_bronze
    .withColumn("cd_categoria",     F.col("cd_categoria").cast(IntegerType()))
    .withColumn("ds_categoria",     F.trim(F.col("ds_categoria")))
    .withColumn("ds_especificacao", F.trim(F.col("ds_especificacao")))
    .filter(F.col("cd_categoria").isNotNull())
    .filter(F.col("ds_categoria").isNotNull() & (F.col("ds_categoria") != ""))
    .withColumn("ds_especificacao",
        F.when(
            F.col("ds_especificacao").isNull() | (F.col("ds_especificacao") == ""),
            F.lit("Não especificado")
        ).otherwise(F.col("ds_especificacao"))
    )
    .withColumn("dt_carga", F.current_timestamp())
    .withColumn("fl_ativo",  F.lit(True))
)

removidos = df_bronze.count() - df_silver.count()
print(f"✔ silver.categorias_despesa | Bronze: {df_bronze.count()} | Silver: {df_silver.count()} | Removidos (DQ): {removidos}")
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.categorias_despesa")
df_silver.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Silver: FORNECEDORES

# COMMAND ----------

df_bronze = spark.table("bronze.fornecedores")

df_silver = (
    df_bronze
    .withColumn("cd_fornecedor", F.col("cd_fornecedor").cast(IntegerType()))
    .withColumn("nm_fornecedor", F.trim(F.col("nm_fornecedor")))
    .withColumn("nr_cnpj_cpf",   F.trim(F.col("nr_cnpj_cpf")))
    .withColumn("ds_tipo",       F.trim(F.col("ds_tipo")))
    .filter(F.col("cd_fornecedor").isNotNull())
    .filter(F.col("nm_fornecedor").isNotNull() & (F.col("nm_fornecedor") != ""))
    .filter(F.col("ds_tipo").isin("Pessoa Juridica", "Pessoa Fisica"))
    .withColumn("dt_carga", F.current_timestamp())
    .withColumn("fl_ativo",  F.lit(True))
)

removidos = df_bronze.count() - df_silver.count()
print(f"✔ silver.fornecedores | Bronze: {df_bronze.count()} | Silver: {df_silver.count()} | Removidos (DQ): {removidos}")
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.fornecedores")
df_silver.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Silver: DESPESAS

# COMMAND ----------

df_bronze = spark.table("bronze.despesas")

df_silver = (
    df_bronze
    .withColumn("cd_despesa",         F.col("cd_despesa").cast(IntegerType()))
    .withColumn("cd_parlamentar",     F.col("cd_parlamentar").cast(IntegerType()))
    .withColumn("cd_categoria",       F.col("cd_categoria").cast(IntegerType()))
    .withColumn("cd_fornecedor",      F.col("cd_fornecedor").cast(IntegerType()))
    .withColumn("nu_mes",             F.col("nu_mes").cast(IntegerType()))
    .withColumn("nu_ano",             F.col("nu_ano").cast(IntegerType()))
    .withColumn("dt_emissao",         F.to_date(F.col("dt_emissao"), "yyyy-MM-dd"))
    .withColumn("vl_documento",       F.col("vl_documento").cast(DoubleType()))
    .withColumn("vl_glosa",           F.col("vl_glosa").cast(DoubleType()))
    .withColumn("vl_liquido",         F.col("vl_liquido").cast(DoubleType()))
    .withColumn("ind_tipo_documento", F.col("ind_tipo_documento").cast(IntegerType()))
    # Regras de DQ
    .filter(F.col("cd_despesa").isNotNull())
    .filter(F.col("cd_parlamentar").isNotNull())
    .filter(F.col("cd_categoria").isNotNull())
    .filter(F.col("cd_fornecedor").isNotNull())
    .filter(F.col("vl_documento") >= 0)
    .filter(F.col("vl_glosa") >= 0)
    .filter(F.col("vl_liquido") >= 0)
    .filter(F.abs(F.col("vl_liquido") - (F.col("vl_documento") - F.col("vl_glosa"))) <= 0.01)
    .filter((F.col("nu_mes") >= 1) & (F.col("nu_mes") <= 12))
    .filter(F.col("nu_ano") >= 2000)
    .filter(F.col("dt_emissao").isNotNull())
    # Integridade referencial
    .join(spark.table("silver.parlamentares").select("cd_parlamentar").distinct(),    "cd_parlamentar", "inner")
    .join(spark.table("silver.categorias_despesa").select("cd_categoria").distinct(), "cd_categoria",   "inner")
    .join(spark.table("silver.fornecedores").select("cd_fornecedor").distinct(),      "cd_fornecedor",  "inner")
    .withColumn("dt_carga", F.current_timestamp())
    .withColumn("fl_ativo",  F.lit(True))
)

removidos = df_bronze.count() - df_silver.count()
print(f"✔ silver.despesas | Bronze: {df_bronze.count()} | Silver: {df_silver.count()} | Removidos (DQ): {removidos}")
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.despesas")
df_silver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Relatório Final de Data Quality

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'partidos'           AS tabela, COUNT(*) AS total_registros FROM silver.partidos
# MAGIC UNION ALL
# MAGIC SELECT 'parlamentares',                COUNT(*) FROM silver.parlamentares
# MAGIC UNION ALL
# MAGIC SELECT 'categorias_despesa',           COUNT(*) FROM silver.categorias_despesa
# MAGIC UNION ALL
# MAGIC SELECT 'fornecedores',                 COUNT(*) FROM silver.fornecedores
# MAGIC UNION ALL
# MAGIC SELECT 'despesas',                     COUNT(*) FROM silver.despesas
# MAGIC ORDER BY tabela
