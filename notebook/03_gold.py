# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Gold Layer
# MAGIC ## Modelagem Dimensional Ralph Kimball: SILVER → GOLD
# MAGIC
# MAGIC Lê os dados tratados do schema **SILVER** e alimenta o **Star Schema** (Ralph Kimball)
# MAGIC no schema **GOLD**.
# MAGIC
# MAGIC ### Modelo Dimensional (Star Schema)
# MAGIC
# MAGIC ```
# MAGIC                    ┌─────────────────┐
# MAGIC                    │   dim_tempo     │
# MAGIC                    └────────┬────────┘
# MAGIC                             │ sk_tempo
# MAGIC ┌───────────────┐           │           ┌──────────────────────┐
# MAGIC │ dim_parlamentar│──sk_parlamentar──│                      │
# MAGIC └───────────────┘           │           │                      │
# MAGIC                             │           │   fato_despesas      │
# MAGIC ┌───────────────┐     sk_categoria──│                      │
# MAGIC │ dim_categoria │───────────┘           │                      │
# MAGIC └───────────────┘                       │  vl_documento        │
# MAGIC                                         │  vl_glosa            │
# MAGIC ┌───────────────┐     sk_fornecedor──│  vl_liquido          │
# MAGIC │ dim_fornecedor│───────────────────────└──────────────────────┘
# MAGIC └───────────────┘
# MAGIC
# MAGIC ┌───────────────┐
# MAGIC │  dim_partido  │  (referenciada por dim_parlamentar)
# MAGIC └───────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold
# MAGIC COMMENT 'Schema Gold - dados modelados para análise (Star Schema - Ralph Kimball)';

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. DIM_TEMPO
# MAGIC Dimensão de tempo gerada a partir das datas de emissão presentes nas despesas.

# COMMAND ----------

df_datas = (
    spark.table("silver.despesas")
    .select("dt_emissao")
    .distinct()
    .filter(F.col("dt_emissao").isNotNull())
)

df_dim_tempo = (
    df_datas
    .withColumn("sk_tempo",       F.date_format(F.col("dt_emissao"), "yyyyMMdd").cast(IntegerType()))
    .withColumn("dt_data",        F.col("dt_emissao"))
    .withColumn("nu_ano",         F.year(F.col("dt_emissao")))
    .withColumn("nu_trimestre",   F.quarter(F.col("dt_emissao")))
    .withColumn("ds_trimestre",   F.concat(F.lit("Q"), F.quarter(F.col("dt_emissao")), F.lit("/"), F.year(F.col("dt_emissao"))))
    .withColumn("nu_mes",         F.month(F.col("dt_emissao")))
    .withColumn("nm_mes",         F.date_format(F.col("dt_emissao"), "MMMM"))
    .withColumn("nu_semana_ano",  F.weekofyear(F.col("dt_emissao")))
    .withColumn("nu_dia",         F.dayofmonth(F.col("dt_emissao")))
    .withColumn("nu_dia_semana",  F.dayofweek(F.col("dt_emissao")))
    .withColumn("nm_dia_semana",  F.date_format(F.col("dt_emissao"), "EEEE"))
    .withColumn("fl_fim_semana",  F.dayofweek(F.col("dt_emissao")).isin([1, 7]))
    .select(
        "sk_tempo", "dt_data", "nu_ano", "nu_trimestre", "ds_trimestre",
        "nu_mes", "nm_mes", "nu_semana_ano", "nu_dia",
        "nu_dia_semana", "nm_dia_semana", "fl_fim_semana"
    )
    .orderBy("sk_tempo")
)

df_dim_tempo.write.format("delta").mode("overwrite").saveAsTable("gold.dim_tempo")
print(f"✔ gold.dim_tempo: {df_dim_tempo.count()} registros")
df_dim_tempo.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. DIM_PARTIDO

# COMMAND ----------

df_dim_partido = (
    spark.table("silver.partidos")
    .select(
        F.col("cd_partido").alias("sk_partido"),
        F.col("cd_partido").alias("nk_cd_partido"),
        F.col("sg_partido"),
        F.col("nm_partido"),
    )
    .withColumn("dt_carga", F.current_timestamp())
)

df_dim_partido.write.format("delta").mode("overwrite").saveAsTable("gold.dim_partido")
print(f"✔ gold.dim_partido: {df_dim_partido.count()} registros")
df_dim_partido.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. DIM_PARLAMENTAR
# MAGIC Partido desnormalizado para facilitar análises (evita joins adicionais).

# COMMAND ----------

df_dim_parlamentar = (
    spark.table("silver.parlamentares")
    .join(
        spark.table("silver.partidos").select("cd_partido", "sg_partido", "nm_partido"),
        "cd_partido", "left"
    )
    .select(
        F.col("cd_parlamentar").alias("sk_parlamentar"),
        F.col("cd_parlamentar").alias("nk_cd_parlamentar"),
        F.col("nm_parlamentar"),
        F.col("sg_uf"),
        F.col("cd_partido"),
        F.col("sg_partido"),
        F.col("nm_partido"),
        F.col("nu_legislatura"),
    )
    .withColumn("dt_carga", F.current_timestamp())
)

df_dim_parlamentar.write.format("delta").mode("overwrite").saveAsTable("gold.dim_parlamentar")
print(f"✔ gold.dim_parlamentar: {df_dim_parlamentar.count()} registros")
df_dim_parlamentar.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. DIM_CATEGORIA_DESPESA

# COMMAND ----------

df_dim_categoria = (
    spark.table("silver.categorias_despesa")
    .select(
        F.col("cd_categoria").alias("sk_categoria"),
        F.col("cd_categoria").alias("nk_cd_categoria"),
        F.col("ds_categoria"),
        F.col("ds_especificacao"),
    )
    .withColumn("dt_carga", F.current_timestamp())
)

df_dim_categoria.write.format("delta").mode("overwrite").saveAsTable("gold.dim_categoria_despesa")
print(f"✔ gold.dim_categoria_despesa: {df_dim_categoria.count()} registros")
df_dim_categoria.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. DIM_FORNECEDOR

# COMMAND ----------

df_dim_fornecedor = (
    spark.table("silver.fornecedores")
    .select(
        F.col("cd_fornecedor").alias("sk_fornecedor"),
        F.col("cd_fornecedor").alias("nk_cd_fornecedor"),
        F.col("nm_fornecedor"),
        F.col("nr_cnpj_cpf"),
        F.col("ds_tipo"),
    )
    .withColumn("dt_carga", F.current_timestamp())
)

df_dim_fornecedor.write.format("delta").mode("overwrite").saveAsTable("gold.dim_fornecedor")
print(f"✔ gold.dim_fornecedor: {df_dim_fornecedor.count()} registros")
df_dim_fornecedor.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. FATO_DESPESAS
# MAGIC Tabela fato central do Star Schema com todas as chaves estrangeiras e métricas.

# COMMAND ----------

df_despesas_sv  = spark.table("silver.despesas")
df_dim_tempo_ref = spark.table("gold.dim_tempo").select("sk_tempo", "dt_data")

df_fato_despesas = (
    df_despesas_sv
    .join(
        df_dim_tempo_ref,
        df_despesas_sv["dt_emissao"] == df_dim_tempo_ref["dt_data"],
        "left"
    )
    .select(
        F.monotonically_increasing_id().alias("sk_despesa"),
        F.col("cd_despesa").alias("nk_cd_despesa"),
        # Chaves estrangeiras (surrogate keys)
        F.col("sk_tempo"),
        F.col("cd_parlamentar").alias("sk_parlamentar"),
        F.col("cd_categoria").alias("sk_categoria"),
        F.col("cd_fornecedor").alias("sk_fornecedor"),
        # Dimensões degeneradas (atributos que ficam na fato)
        F.col("nr_documento"),
        F.col("ind_tipo_documento"),
        F.col("nu_mes"),
        F.col("nu_ano"),
        # Métricas (fatos aditivos)
        F.col("vl_documento"),
        F.col("vl_glosa"),
        F.col("vl_liquido"),
        F.current_timestamp().alias("dt_carga"),
    )
)

df_fato_despesas.write.format("delta").mode("overwrite").saveAsTable("gold.fato_despesas")
print(f"✔ gold.fato_despesas: {df_fato_despesas.count()} registros")
df_fato_despesas.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análises Analíticas – Validação do Modelo

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 10 Parlamentares por Total de Despesas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     p.nm_parlamentar,
# MAGIC     p.sg_uf,
# MAGIC     p.sg_partido,
# MAGIC     COUNT(f.sk_despesa)   AS qtd_despesas,
# MAGIC     SUM(f.vl_liquido)     AS total_despesas,
# MAGIC     AVG(f.vl_liquido)     AS media_despesa,
# MAGIC     SUM(f.vl_glosa)       AS total_glosa
# MAGIC FROM gold.fato_despesas f
# MAGIC JOIN gold.dim_parlamentar p ON f.sk_parlamentar = p.sk_parlamentar
# MAGIC GROUP BY p.nm_parlamentar, p.sg_uf, p.sg_partido
# MAGIC ORDER BY total_despesas DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC #### Despesas por Categoria

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.ds_categoria,
# MAGIC     COUNT(f.sk_despesa) AS qtd_despesas,
# MAGIC     SUM(f.vl_liquido)   AS total_despesas,
# MAGIC     SUM(f.vl_glosa)     AS total_glosa
# MAGIC FROM gold.fato_despesas f
# MAGIC JOIN gold.dim_categoria_despesa c ON f.sk_categoria = c.sk_categoria
# MAGIC GROUP BY c.ds_categoria
# MAGIC ORDER BY total_despesas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evolução Mensal das Despesas (2024)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.nu_ano,
# MAGIC     t.nu_mes,
# MAGIC     t.nm_mes,
# MAGIC     COUNT(f.sk_despesa) AS qtd_despesas,
# MAGIC     SUM(f.vl_liquido)   AS total_despesas
# MAGIC FROM gold.fato_despesas f
# MAGIC JOIN gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
# MAGIC GROUP BY t.nu_ano, t.nu_mes, t.nm_mes
# MAGIC ORDER BY t.nu_ano, t.nu_mes

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sumário do Schema GOLD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_tempo'              AS tabela, COUNT(*) AS total_registros FROM gold.dim_tempo
# MAGIC UNION ALL
# MAGIC SELECT 'dim_partido',                      COUNT(*) FROM gold.dim_partido
# MAGIC UNION ALL
# MAGIC SELECT 'dim_parlamentar',                  COUNT(*) FROM gold.dim_parlamentar
# MAGIC UNION ALL
# MAGIC SELECT 'dim_categoria_despesa',            COUNT(*) FROM gold.dim_categoria_despesa
# MAGIC UNION ALL
# MAGIC SELECT 'dim_fornecedor',                   COUNT(*) FROM gold.dim_fornecedor
# MAGIC UNION ALL
# MAGIC SELECT 'fato_despesas',                    COUNT(*) FROM gold.fato_despesas
# MAGIC ORDER BY tabela
