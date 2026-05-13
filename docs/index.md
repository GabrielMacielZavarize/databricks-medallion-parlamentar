# Lakehouse com Databricks – Arquitetura Medalhão

Pipeline de dados acadêmico que implementa a **Arquitetura Medalhão** no **Databricks Free Edition**
usando **Delta Lake** e **Jobs & Pipelines**.

## Objetivo

Construir um lakehouse completo partindo de dados relacionais (Despesas Parlamentares CEAP),
passando pelas camadas Landing → Bronze → Silver → Gold, com orquestração automatizada via Jobs.

## Tecnologias

| Tecnologia | Uso |
|------------|-----|
| Databricks Free Edition | Plataforma de execução |
| Apache Spark / PySpark | Processamento distribuído |
| Delta Lake | Armazenamento ACID com versionamento |
| Python | Linguagem dos notebooks |
| Jobs & Pipelines | Orquestração sequencial |

## Pré-requisitos

Conta gratuita no Databricks Community Edition: [community.cloud.databricks.com](https://community.cloud.databricks.com)

## Como executar

1. Faça login no Databricks
2. Vá em **Workspace** → clique nos três pontos → **Import**
3. Importe os 4 arquivos `.py` da pasta `notebook/`
4. Crie um cluster **Single Node** com DBR 13.x ou superior
5. Crie um **Job** com 4 tasks sequenciais (veja [Jobs & Pipelines](camadas.md#jobs--pipelines))
6. Execute o Job e acompanhe o progresso
