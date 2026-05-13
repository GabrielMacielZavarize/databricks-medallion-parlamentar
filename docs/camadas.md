# Camadas do Pipeline

## Landing (00_landing.py)

Simula a extração do banco de dados relacional. Cria os dados programaticamente
e os grava como **CSV** no DBFS (Databricks File System) sob `dbfs:/landing/dados/`.

Em seguida, cria tabelas externas no schema `landing` apontando para esses CSVs.

**Tabelas criadas:** `landing.partidos`, `landing.parlamentares`, `landing.categorias_despesa`,
`landing.fornecedores`, `landing.despesas`

---

## Bronze (01_bronze.py)

Lê cada tabela do schema `landing` e converte para **Delta Lake** no schema `bronze`.

Nenhuma transformação é aplicada – os dados são preservados como vieram da origem.

**Benefícios do Delta Lake:** transações ACID, time travel, schema enforcement.

---

## Silver (02_silver.py)

Aplica regras de **Data Quality** e persiste os dados limpos no schema `silver`.

### Regras aplicadas

| Regra | Tabelas afetadas |
|-------|-----------------|
| Remove PKs nulas | Todas |
| Trim + Upper em strings de código | sg_partido, sg_uf, ds_tipo |
| Valores monetários >= 0 | despesas |
| vl_liquido = vl_documento - vl_glosa (tolerância 0.01) | despesas |
| nu_mes entre 1 e 12 | despesas |
| nu_ano >= 2000 | despesas |
| sg_uf com exatamente 2 caracteres | parlamentares |
| ds_tipo deve ser "Pessoa Juridica" ou "Pessoa Fisica" | fornecedores |
| Integridade referencial FK → PK | despesas |
| Adiciona dt_carga e fl_ativo | Todas |

---

## Gold (03_gold.py)

Implementa o **Star Schema de Ralph Kimball** no schema `gold`.

### Dimensões

- **dim_tempo** – gerada a partir das datas de emissão, com ano, trimestre, mês, semana, dia e flag de fim de semana
- **dim_partido** – partidos políticos com chave surrogate
- **dim_parlamentar** – deputados com partido desnormalizado para facilitar queries
- **dim_categoria_despesa** – categorias de despesa CEAP
- **dim_fornecedor** – fornecedores com CNPJ/CPF e tipo

### Fato

- **fato_despesas** – métricas: `vl_documento`, `vl_glosa`, `vl_liquido`; chaves FK para todas as dimensões

---

## Jobs & Pipelines

Para criar o Job no Databricks:

1. No menu lateral, clique em **Workflows** → **Jobs**
2. Clique em **Create Job**
3. Adicione a **Task 1**: notebook `00_landing`, cluster criado
4. Clique em **Add Task** → **Task 2**: notebook `01_bronze`, dependência: Task 1
5. **Task 3**: notebook `02_silver`, dependência: Task 2
6. **Task 4**: notebook `03_gold`, dependência: Task 3
7. Clique em **Run Now**

O Job executa os notebooks **sequencialmente**, garantindo que cada camada
só inicie após a camada anterior ser concluída com sucesso.
