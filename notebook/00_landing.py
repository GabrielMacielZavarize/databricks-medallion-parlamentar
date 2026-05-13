# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 - Landing Zone
# MAGIC ## Extração dos dados → LANDING
# MAGIC
# MAGIC **Origem:** Banco de dados relacional (Despesas Parlamentares - CEAP/Câmara Federal)
# MAGIC
# MAGIC Este notebook simula a extração de todas as tabelas do banco de dados relacional
# MAGIC e armazena no schema **LANDING** como tabelas Delta gerenciadas pelo Unity Catalog.
# MAGIC
# MAGIC | Tabela | Descrição |
# MAGIC |--------|-----------|
# MAGIC | partidos | Partidos políticos |
# MAGIC | parlamentares | Deputados federais |
# MAGIC | categorias_despesa | Tipos de despesa (CEAP) |
# MAGIC | fornecedores | Empresas e pessoas que prestaram serviços |
# MAGIC | despesas | Registros de reembolso (tabela fato) |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação do Schema LANDING

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS landing
# MAGIC COMMENT 'Schema de Landing Zone - dados brutos extraídos da origem';

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Tabela: PARTIDOS

# COMMAND ----------

partidos_data = [
    Row(cd_partido=1,  sg_partido="PT",           nm_partido="Partido dos Trabalhadores"),
    Row(cd_partido=2,  sg_partido="PL",           nm_partido="Partido Liberal"),
    Row(cd_partido=3,  sg_partido="UNIAO",        nm_partido="Uniao Brasil"),
    Row(cd_partido=4,  sg_partido="PP",           nm_partido="Progressistas"),
    Row(cd_partido=5,  sg_partido="PSD",          nm_partido="Partido Social Democratico"),
    Row(cd_partido=6,  sg_partido="REPUBLICANOS", nm_partido="Republicanos"),
    Row(cd_partido=7,  sg_partido="MDB",          nm_partido="Movimento Democratico Brasileiro"),
    Row(cd_partido=8,  sg_partido="PDT",          nm_partido="Partido Democratico Trabalhista"),
    Row(cd_partido=9,  sg_partido="PSDB",         nm_partido="Partido da Social Democracia Brasileira"),
    Row(cd_partido=10, sg_partido="PODE",         nm_partido="Podemos"),
    Row(cd_partido=11, sg_partido="PSOL",         nm_partido="Partido Socialismo e Liberdade"),
    Row(cd_partido=12, sg_partido="AVANTE",       nm_partido="Avante"),
]

schema_partidos = StructType([
    StructField("cd_partido", IntegerType(), False),
    StructField("sg_partido", StringType(),  False),
    StructField("nm_partido", StringType(),  False),
])

df_partidos = spark.createDataFrame(partidos_data, schema_partidos)
df_partidos.write.format("delta").mode("overwrite").saveAsTable("landing.partidos")

print(f"OK - landing.partidos: {df_partidos.count()} registros")
df_partidos.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Tabela: PARLAMENTARES

# COMMAND ----------

parlamentares_data = [
    Row(cd_parlamentar=101, nm_parlamentar="LIDICE DA MATA",         sg_uf="BA", cd_partido=1,  nu_legislatura=57),
    Row(cd_parlamentar=102, nm_parlamentar="GLEISI HOFFMANN",         sg_uf="PR", cd_partido=1,  nu_legislatura=57),
    Row(cd_parlamentar=103, nm_parlamentar="REGINALDO LOPES",         sg_uf="MG", cd_partido=1,  nu_legislatura=57),
    Row(cd_parlamentar=104, nm_parlamentar="CARLOS JORDY",            sg_uf="RJ", cd_partido=2,  nu_legislatura=57),
    Row(cd_parlamentar=105, nm_parlamentar="CORONEL TADEU",           sg_uf="SP", cd_partido=2,  nu_legislatura=57),
    Row(cd_parlamentar=106, nm_parlamentar="NIKOLAS FERREIRA",        sg_uf="MG", cd_partido=2,  nu_legislatura=57),
    Row(cd_parlamentar=107, nm_parlamentar="ELMAR NASCIMENTO",        sg_uf="BA", cd_partido=3,  nu_legislatura=57),
    Row(cd_parlamentar=108, nm_parlamentar="ACM NETO",                sg_uf="BA", cd_partido=3,  nu_legislatura=57),
    Row(cd_parlamentar=109, nm_parlamentar="ARTHUR LIRA",             sg_uf="AL", cd_partido=4,  nu_legislatura=57),
    Row(cd_parlamentar=110, nm_parlamentar="MARCUS PESTANA",          sg_uf="MG", cd_partido=5,  nu_legislatura=57),
    Row(cd_parlamentar=111, nm_parlamentar="FAUSTO PINATO",           sg_uf="SP", cd_partido=6,  nu_legislatura=57),
    Row(cd_parlamentar=112, nm_parlamentar="BALEIA ROSSI",            sg_uf="SP", cd_partido=7,  nu_legislatura=57),
    Row(cd_parlamentar=113, nm_parlamentar="CIRO GOMES",              sg_uf="CE", cd_partido=8,  nu_legislatura=57),
    Row(cd_parlamentar=114, nm_parlamentar="PAULO MAGALHAES",         sg_uf="BA", cd_partido=5,  nu_legislatura=57),
    Row(cd_parlamentar=115, nm_parlamentar="SAMUEL MOREIRA",          sg_uf="SP", cd_partido=9,  nu_legislatura=57),
    Row(cd_parlamentar=116, nm_parlamentar="RENATA ABREU",            sg_uf="SP", cd_partido=10, nu_legislatura=57),
    Row(cd_parlamentar=117, nm_parlamentar="GLAUBER BRAGA",           sg_uf="RJ", cd_partido=11, nu_legislatura=57),
    Row(cd_parlamentar=118, nm_parlamentar="FERNANDA MELCHIONNA",     sg_uf="RS", cd_partido=11, nu_legislatura=57),
    Row(cd_parlamentar=119, nm_parlamentar="LUIZ LIMA",               sg_uf="RJ", cd_partido=2,  nu_legislatura=57),
    Row(cd_parlamentar=120, nm_parlamentar="MAURICIO DZIEKANIAK",     sg_uf="RS", cd_partido=2,  nu_legislatura=57),
    Row(cd_parlamentar=121, nm_parlamentar="PEDRO CAMPOS",            sg_uf="PE", cd_partido=7,  nu_legislatura=57),
    Row(cd_parlamentar=122, nm_parlamentar="TABATA AMARAL",           sg_uf="SP", cd_partido=8,  nu_legislatura=57),
    Row(cd_parlamentar=123, nm_parlamentar="ALEXIS FONTEYNE",         sg_uf="SP", cd_partido=10, nu_legislatura=57),
    Row(cd_parlamentar=124, nm_parlamentar="PROFESSOR ISRAEL",        sg_uf="DF", cd_partido=2,  nu_legislatura=57),
    Row(cd_parlamentar=125, nm_parlamentar="DELEGADO RAMAGEM",        sg_uf="RJ", cd_partido=2,  nu_legislatura=57),
]

schema_parlamentares = StructType([
    StructField("cd_parlamentar", IntegerType(), False),
    StructField("nm_parlamentar", StringType(),  False),
    StructField("sg_uf",          StringType(),  False),
    StructField("cd_partido",     IntegerType(), False),
    StructField("nu_legislatura", IntegerType(), False),
])

df_parlamentares = spark.createDataFrame(parlamentares_data, schema_parlamentares)
df_parlamentares.write.format("delta").mode("overwrite").saveAsTable("landing.parlamentares")

print(f"OK - landing.parlamentares: {df_parlamentares.count()} registros")
df_parlamentares.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Tabela: CATEGORIAS_DESPESA

# COMMAND ----------

categorias_data = [
    Row(cd_categoria=1, ds_categoria="Alimentacao",                        ds_especificacao="Despesas com alimentacao do parlamentar"),
    Row(cd_categoria=2, ds_categoria="Combustivel e lubrificantes",         ds_especificacao="Abastecimento de veiculos"),
    Row(cd_categoria=3, ds_categoria="Divulgacao da Atividade Parlamentar", ds_especificacao="Publicidade e divulgacao institucional"),
    Row(cd_categoria=4, ds_categoria="Locacao ou fretamento de veiculos",   ds_especificacao="Aluguel de veiculos automotores"),
    Row(cd_categoria=5, ds_categoria="Manutencao de escritorio",            ds_especificacao="Material de escritorio e manutencao predial"),
    Row(cd_categoria=6, ds_categoria="Passagens aereas",                    ds_especificacao="Passagens aereas nacionais e internacionais"),
    Row(cd_categoria=7, ds_categoria="Servicos postais",                    ds_especificacao="Correios e servicos de encomenda"),
    Row(cd_categoria=8, ds_categoria="Telefonia",                           ds_especificacao="Servicos de telefonia fixa e movel"),
]

schema_categorias = StructType([
    StructField("cd_categoria",     IntegerType(), False),
    StructField("ds_categoria",     StringType(),  False),
    StructField("ds_especificacao", StringType(),  True),
])

df_categorias = spark.createDataFrame(categorias_data, schema_categorias)
df_categorias.write.format("delta").mode("overwrite").saveAsTable("landing.categorias_despesa")

print(f"OK - landing.categorias_despesa: {df_categorias.count()} registros")
df_categorias.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Tabela: FORNECEDORES

# COMMAND ----------

fornecedores_data = [
    Row(cd_fornecedor=1001, nm_fornecedor="LATAM AIRLINES DO BRASIL S/A",               nr_cnpj_cpf="02.012.862/0001-60", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1002, nm_fornecedor="GOL LINHAS AEREAS S/A",                      nr_cnpj_cpf="07.575.651/0001-59", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1003, nm_fornecedor="PETROBRAS DISTRIBUIDORA S/A",                nr_cnpj_cpf="34.274.233/0001-02", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1004, nm_fornecedor="EMPRESA BRASILEIRA DE CORREIOS E TELEGRAFOS", nr_cnpj_cpf="34.028.316/0001-03", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1005, nm_fornecedor="CLARO S/A",                                  nr_cnpj_cpf="40.432.544/0001-47", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1006, nm_fornecedor="TIM S/A",                                    nr_cnpj_cpf="04.206.050/0001-80", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1007, nm_fornecedor="RESTAURANTE PANORAMA LTDA",                  nr_cnpj_cpf="12.345.678/0001-90", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1008, nm_fornecedor="PAPELARIA CONGRESSO LTDA",                   nr_cnpj_cpf="23.456.789/0001-01", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1009, nm_fornecedor="LOCADORA DE VEICULOS BRASILIA S/A",          nr_cnpj_cpf="34.567.890/0001-12", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1010, nm_fornecedor="GRAFICA PARLAMENTO LTDA",                    nr_cnpj_cpf="45.678.901/0001-23", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1011, nm_fornecedor="POSTO COMBUSTIVEL EIXO RODOVIARIO LTDA",     nr_cnpj_cpf="56.789.012/0001-34", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1012, nm_fornecedor="AZUL LINHAS AEREAS BRASILEIRAS S/A",         nr_cnpj_cpf="09.296.295/0001-60", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1013, nm_fornecedor="UBER DO BRASIL TECNOLOGIA LTDA",             nr_cnpj_cpf="17.895.646/0001-79", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1014, nm_fornecedor="CHURRASCARIA PLATAFORMA LTDA",               nr_cnpj_cpf="67.890.123/0001-45", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1015, nm_fornecedor="RAUL FERREIRA DA SILVA",                     nr_cnpj_cpf="123.456.789-01",    ds_tipo="Pessoa Fisica"),
    Row(cd_fornecedor=1016, nm_fornecedor="MARIA APARECIDA SOUZA CONSULTORIA ME",       nr_cnpj_cpf="78.901.234/0001-56", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1017, nm_fornecedor="HOTEL NACIONAL DE BRASILIA S/A",             nr_cnpj_cpf="89.012.345/0001-67", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1018, nm_fornecedor="SINTEGRA TECNOLOGIA LTDA",                   nr_cnpj_cpf="90.123.456/0001-78", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1019, nm_fornecedor="POSTO BR COMBUSTIVEIS LTDA",                 nr_cnpj_cpf="01.234.567/0001-89", ds_tipo="Pessoa Juridica"),
    Row(cd_fornecedor=1020, nm_fornecedor="GRAFICA E EDITORA VERDE S/A",                nr_cnpj_cpf="12.345.670/0001-90", ds_tipo="Pessoa Juridica"),
]

schema_fornecedores = StructType([
    StructField("cd_fornecedor", IntegerType(), False),
    StructField("nm_fornecedor", StringType(),  False),
    StructField("nr_cnpj_cpf",   StringType(),  True),
    StructField("ds_tipo",       StringType(),  True),
])

df_fornecedores = spark.createDataFrame(fornecedores_data, schema_fornecedores)
df_fornecedores.write.format("delta").mode("overwrite").saveAsTable("landing.fornecedores")

print(f"OK - landing.fornecedores: {df_fornecedores.count()} registros")
df_fornecedores.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Tabela: DESPESAS (Tabela Fato)

# COMMAND ----------

despesas_data = [
    Row(cd_despesa=1,  cd_parlamentar=101, cd_categoria=6, cd_fornecedor=1001, nu_mes=1, nu_ano=2024, dt_emissao="2024-01-10", vl_documento=1850.50, vl_glosa=0.00,    vl_liquido=1850.50, nr_documento="NF-001234",    ind_tipo_documento=0),
    Row(cd_despesa=2,  cd_parlamentar=101, cd_categoria=1, cd_fornecedor=1007, nu_mes=1, nu_ano=2024, dt_emissao="2024-01-15", vl_documento=320.00,  vl_glosa=0.00,    vl_liquido=320.00,  nr_documento="NF-002345",    ind_tipo_documento=0),
    Row(cd_despesa=3,  cd_parlamentar=102, cd_categoria=6, cd_fornecedor=1002, nu_mes=1, nu_ano=2024, dt_emissao="2024-01-08", vl_documento=2100.00, vl_glosa=0.00,    vl_liquido=2100.00, nr_documento="TICKET-00123", ind_tipo_documento=2),
    Row(cd_despesa=4,  cd_parlamentar=103, cd_categoria=3, cd_fornecedor=1010, nu_mes=1, nu_ano=2024, dt_emissao="2024-01-20", vl_documento=4500.00, vl_glosa=0.00,    vl_liquido=4500.00, nr_documento="NF-003456",    ind_tipo_documento=0),
    Row(cd_despesa=5,  cd_parlamentar=104, cd_categoria=2, cd_fornecedor=1003, nu_mes=1, nu_ano=2024, dt_emissao="2024-01-05", vl_documento=250.00,  vl_glosa=0.00,    vl_liquido=250.00,  nr_documento="NF-004567",    ind_tipo_documento=0),
    Row(cd_despesa=6,  cd_parlamentar=105, cd_categoria=8, cd_fornecedor=1005, nu_mes=2, nu_ano=2024, dt_emissao="2024-02-01", vl_documento=189.90,  vl_glosa=0.00,    vl_liquido=189.90,  nr_documento="FAT-001234",   ind_tipo_documento=0),
    Row(cd_despesa=7,  cd_parlamentar=106, cd_categoria=6, cd_fornecedor=1001, nu_mes=2, nu_ano=2024, dt_emissao="2024-02-14", vl_documento=3200.00, vl_glosa=0.00,    vl_liquido=3200.00, nr_documento="NF-005678",    ind_tipo_documento=0),
    Row(cd_despesa=8,  cd_parlamentar=107, cd_categoria=4, cd_fornecedor=1009, nu_mes=2, nu_ano=2024, dt_emissao="2024-02-20", vl_documento=1500.00, vl_glosa=0.00,    vl_liquido=1500.00, nr_documento="NF-006789",    ind_tipo_documento=0),
    Row(cd_despesa=9,  cd_parlamentar=108, cd_categoria=7, cd_fornecedor=1004, nu_mes=2, nu_ano=2024, dt_emissao="2024-02-25", vl_documento=75.30,   vl_glosa=0.00,    vl_liquido=75.30,   nr_documento="NF-007890",    ind_tipo_documento=0),
    Row(cd_despesa=10, cd_parlamentar=109, cd_categoria=5, cd_fornecedor=1008, nu_mes=3, nu_ano=2024, dt_emissao="2024-03-10", vl_documento=890.00,  vl_glosa=0.00,    vl_liquido=890.00,  nr_documento="NF-008901",    ind_tipo_documento=0),
    Row(cd_despesa=11, cd_parlamentar=110, cd_categoria=6, cd_fornecedor=1012, nu_mes=3, nu_ano=2024, dt_emissao="2024-03-15", vl_documento=1750.00, vl_glosa=0.00,    vl_liquido=1750.00, nr_documento="NF-009012",    ind_tipo_documento=0),
    Row(cd_despesa=12, cd_parlamentar=111, cd_categoria=2, cd_fornecedor=1011, nu_mes=3, nu_ano=2024, dt_emissao="2024-03-20", vl_documento=180.50,  vl_glosa=0.00,    vl_liquido=180.50,  nr_documento="NF-010123",    ind_tipo_documento=0),
    Row(cd_despesa=13, cd_parlamentar=112, cd_categoria=3, cd_fornecedor=1016, nu_mes=3, nu_ano=2024, dt_emissao="2024-03-25", vl_documento=6000.00, vl_glosa=500.00,  vl_liquido=5500.00, nr_documento="NF-011234",    ind_tipo_documento=0),
    Row(cd_despesa=14, cd_parlamentar=113, cd_categoria=1, cd_fornecedor=1014, nu_mes=4, nu_ano=2024, dt_emissao="2024-04-05", vl_documento=450.00,  vl_glosa=0.00,    vl_liquido=450.00,  nr_documento="NF-012345",    ind_tipo_documento=0),
    Row(cd_despesa=15, cd_parlamentar=114, cd_categoria=8, cd_fornecedor=1006, nu_mes=4, nu_ano=2024, dt_emissao="2024-04-01", vl_documento=210.00,  vl_glosa=0.00,    vl_liquido=210.00,  nr_documento="FAT-002345",   ind_tipo_documento=0),
    Row(cd_despesa=16, cd_parlamentar=115, cd_categoria=6, cd_fornecedor=1002, nu_mes=4, nu_ano=2024, dt_emissao="2024-04-18", vl_documento=2800.00, vl_glosa=0.00,    vl_liquido=2800.00, nr_documento="TICKET-00456", ind_tipo_documento=2),
    Row(cd_despesa=17, cd_parlamentar=116, cd_categoria=4, cd_fornecedor=1013, nu_mes=4, nu_ano=2024, dt_emissao="2024-04-22", vl_documento=350.00,  vl_glosa=0.00,    vl_liquido=350.00,  nr_documento="NF-013456",    ind_tipo_documento=0),
    Row(cd_despesa=18, cd_parlamentar=117, cd_categoria=3, cd_fornecedor=1010, nu_mes=5, nu_ano=2024, dt_emissao="2024-05-10", vl_documento=3800.00, vl_glosa=0.00,    vl_liquido=3800.00, nr_documento="NF-014567",    ind_tipo_documento=0),
    Row(cd_despesa=19, cd_parlamentar=118, cd_categoria=7, cd_fornecedor=1004, nu_mes=5, nu_ano=2024, dt_emissao="2024-05-15", vl_documento=120.00,  vl_glosa=0.00,    vl_liquido=120.00,  nr_documento="NF-015678",    ind_tipo_documento=0),
    Row(cd_despesa=20, cd_parlamentar=119, cd_categoria=6, cd_fornecedor=1001, nu_mes=5, nu_ano=2024, dt_emissao="2024-05-20", vl_documento=2950.00, vl_glosa=0.00,    vl_liquido=2950.00, nr_documento="NF-016789",    ind_tipo_documento=0),
    Row(cd_despesa=21, cd_parlamentar=120, cd_categoria=2, cd_fornecedor=1019, nu_mes=5, nu_ano=2024, dt_emissao="2024-05-25", vl_documento=310.00,  vl_glosa=0.00,    vl_liquido=310.00,  nr_documento="NF-017890",    ind_tipo_documento=0),
    Row(cd_despesa=22, cd_parlamentar=121, cd_categoria=5, cd_fornecedor=1018, nu_mes=6, nu_ano=2024, dt_emissao="2024-06-05", vl_documento=1200.00, vl_glosa=0.00,    vl_liquido=1200.00, nr_documento="NF-018901",    ind_tipo_documento=0),
    Row(cd_despesa=23, cd_parlamentar=122, cd_categoria=6, cd_fornecedor=1012, nu_mes=6, nu_ano=2024, dt_emissao="2024-06-12", vl_documento=1650.00, vl_glosa=0.00,    vl_liquido=1650.00, nr_documento="NF-019012",    ind_tipo_documento=0),
    Row(cd_despesa=24, cd_parlamentar=123, cd_categoria=1, cd_fornecedor=1007, nu_mes=6, nu_ano=2024, dt_emissao="2024-06-18", vl_documento=280.00,  vl_glosa=0.00,    vl_liquido=280.00,  nr_documento="NF-020123",    ind_tipo_documento=0),
    Row(cd_despesa=25, cd_parlamentar=124, cd_categoria=8, cd_fornecedor=1005, nu_mes=6, nu_ano=2024, dt_emissao="2024-06-01", vl_documento=195.00,  vl_glosa=0.00,    vl_liquido=195.00,  nr_documento="FAT-003456",   ind_tipo_documento=0),
    Row(cd_despesa=26, cd_parlamentar=125, cd_categoria=4, cd_fornecedor=1009, nu_mes=7, nu_ano=2024, dt_emissao="2024-07-08", vl_documento=1800.00, vl_glosa=0.00,    vl_liquido=1800.00, nr_documento="NF-021234",    ind_tipo_documento=0),
    Row(cd_despesa=27, cd_parlamentar=101, cd_categoria=6, cd_fornecedor=1002, nu_mes=7, nu_ano=2024, dt_emissao="2024-07-15", vl_documento=2200.00, vl_glosa=0.00,    vl_liquido=2200.00, nr_documento="TICKET-00789", ind_tipo_documento=2),
    Row(cd_despesa=28, cd_parlamentar=102, cd_categoria=3, cd_fornecedor=1020, nu_mes=7, nu_ano=2024, dt_emissao="2024-07-22", vl_documento=5500.00, vl_glosa=1000.00, vl_liquido=4500.00, nr_documento="NF-022345",    ind_tipo_documento=0),
    Row(cd_despesa=29, cd_parlamentar=103, cd_categoria=2, cd_fornecedor=1003, nu_mes=8, nu_ano=2024, dt_emissao="2024-08-10", vl_documento=420.00,  vl_glosa=0.00,    vl_liquido=420.00,  nr_documento="NF-023456",    ind_tipo_documento=0),
    Row(cd_despesa=30, cd_parlamentar=104, cd_categoria=7, cd_fornecedor=1004, nu_mes=8, nu_ano=2024, dt_emissao="2024-08-20", vl_documento=95.00,   vl_glosa=0.00,    vl_liquido=95.00,   nr_documento="NF-024567",    ind_tipo_documento=0),
]

schema_despesas = StructType([
    StructField("cd_despesa",         IntegerType(), False),
    StructField("cd_parlamentar",     IntegerType(), False),
    StructField("cd_categoria",       IntegerType(), False),
    StructField("cd_fornecedor",      IntegerType(), False),
    StructField("nu_mes",             IntegerType(), False),
    StructField("nu_ano",             IntegerType(), False),
    StructField("dt_emissao",         StringType(),  True),
    StructField("vl_documento",       DoubleType(),  True),
    StructField("vl_glosa",           DoubleType(),  True),
    StructField("vl_liquido",         DoubleType(),  True),
    StructField("nr_documento",       StringType(),  True),
    StructField("ind_tipo_documento", IntegerType(), True),
])

df_despesas = spark.createDataFrame(despesas_data, schema_despesas)
df_despesas.write.format("delta").mode("overwrite").saveAsTable("landing.despesas")

print(f"OK - landing.despesas: {df_despesas.count()} registros")
df_despesas.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sumário da Landing Zone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'partidos'           AS tabela, COUNT(*) AS total_registros FROM landing.partidos
# MAGIC UNION ALL
# MAGIC SELECT 'parlamentares',                COUNT(*) FROM landing.parlamentares
# MAGIC UNION ALL
# MAGIC SELECT 'categorias_despesa',           COUNT(*) FROM landing.categorias_despesa
# MAGIC UNION ALL
# MAGIC SELECT 'fornecedores',                 COUNT(*) FROM landing.fornecedores
# MAGIC UNION ALL
# MAGIC SELECT 'despesas',                     COUNT(*) FROM landing.despesas
# MAGIC ORDER BY tabela
