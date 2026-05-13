# Lakehouse com Databricks – Arquitetura Medalhão

**Trabalho 3 – Engenharia de Dados**  
Pipeline de dados implementando a **Arquitetura Medalhão** (Landing → Bronze → Silver → Gold) no Databricks Free Edition com Delta Lake e Jobs & Pipelines.

---

## Integrantes

| Nome | GitHub |
|------|--------|
| Gabriel Maciel Zavarize | [GabrielMacielZavarize](https://github.com/GabrielMacielZavarize) |
| Pedro Henrique Harter Marques | [PedroHarter](https://github.com/PedroHarter) |
| Wilian Vieira Fernandes | [WilianVieiraF](https://github.com/WilianVieiraF) |

---

## Domínio dos Dados

Despesas Parlamentares do CEAP (Cota para o Exercício da Atividade Parlamentar) da Câmara dos Deputados Federal do Brasil. O dataset contém 5 tabelas relacionais:

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| partidos | 12 | Partidos políticos |
| parlamentares | 25 | Deputados federais (57ª Legislatura) |
| categorias_despesa | 8 | Tipos de despesa CEAP |
| fornecedores | 20 | Empresas/pessoas que prestaram serviços |
| despesas | 30 | Registros de reembolso (tabela fato) |

---

## Arquitetura Medalhão

![Arquitetura Medalhão](assets/imagem_github.jpeg)

---

## Modelo Dimensional (Gold – Star Schema)

```
         dim_tempo
              │
dim_parlamentar ──── fato_despesas ──── dim_categoria_despesa
                           │
                    dim_fornecedor
```

**Dimensões:**
- `dim_tempo` – data de emissão decomposta (ano, trimestre, mês, dia, semana)
- `dim_parlamentar` – deputado com partido desnormalizado
- `dim_partido` – partido político
- `dim_categoria_despesa` – tipo de despesa CEAP
- `dim_fornecedor` – empresa ou pessoa física que prestou o serviço

**Fato:**
- `fato_despesas` – `vl_documento`, `vl_glosa`, `vl_liquido`

---

## Estrutura do Repositório

```
databricks-medallion-parlamentar/
├── notebook/
│   ├── 00_landing.py    # Extração → Landing Zone (CSV)
│   ├── 01_bronze.py     # Landing → Bronze (Delta Lake)
│   ├── 02_silver.py     # Bronze → Silver (Data Quality)
│   └── 03_gold.py       # Silver → Gold (Star Schema)
├── docs/
│   ├── index.md
│   ├── arquitetura.md
│   └── camadas.md
├── mkdocs.yml
└── README.md
```

---

## Tecnologias

- **Databricks Free Edition** – plataforma de processamento
- **Apache Spark** – motor de processamento distribuído
- **Delta Lake** – formato de armazenamento ACID
- **Python / PySpark** – linguagem dos notebooks
- **Jobs & Pipelines** – orquestração sequencial dos notebooks

---

## Como Executar no Databricks

1. Importe os notebooks em `Workspace > Import`
2. Crie um cluster (Single Node, DBR 13.x+)
3. Crie um Job com 4 tasks na ordem: `00 → 01 → 02 → 03`
4. Execute o Job

Consulte a [documentação completa](https://GabrielMacielZavarize.github.io/databricks-medallion-parlamentar/) para o passo a passo detalhado.

---

## Referências

- [Databricks Free Edition](https://www.databricks.com/learn/training/databricks-free-edition) – Plataforma utilizada para execução dos notebooks e Jobs
- [Delta Lake](https://delta.io/) – Formato de armazenamento ACID com suporte a time travel e versionamento
- [Apache Spark](https://spark.apache.org/) – Motor de processamento distribuído utilizado via PySpark
- [Arquitetura Medalhão](https://www.databricks.com/glossary/medallion-architecture) – Padrão de design Landing → Bronze → Silver → Gold
- [Ralph Kimball – Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) – Metodologia utilizada na camada Gold (Star Schema)
- [CEAP – Câmara dos Deputados](https://www2.camara.leg.br/transparencia/acesso-a-informacao/copy_of_glossario/cota-para-o-exercicio-da-atividade-parlamentar) – Origem dos dados de despesas parlamentares
- [MkDocs](https://www.mkdocs.org/) – Gerador de documentação estática
- [MkDocs Material Theme](https://squidfunk.github.io/mkdocs-material/) – Tema utilizado na documentação
- [GitHub Pages](https://pages.github.com/) – Hospedagem da documentação
