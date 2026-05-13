# Arquitetura

## Visão Geral

```
Banco Relacional (SQL Server)
          │
          │  Extração (CSV)
          ▼
   ┌─────────────┐
   │   LANDING   │  5 tabelas em CSV no DBFS
   └──────┬──────┘
          │  Ingestão Delta
          ▼
   ┌─────────────┐
   │   BRONZE    │  5 tabelas Delta Lake (raw)
   └──────┬──────┘
          │  Data Quality
          ▼
   ┌─────────────┐
   │   SILVER    │  5 tabelas Delta Lake (clean)
   └──────┬──────┘
          │  Modelagem Dimensional
          ▼
   ┌─────────────┐
   │    GOLD     │  6 tabelas: 5 dims + 1 fato
   └─────────────┘
```

## Star Schema (Gold)

```
              ┌──────────────────┐
              │    dim_tempo     │
              │  sk_tempo (PK)   │
              │  dt_data         │
              │  nu_ano          │
              │  nu_trimestre    │
              │  nu_mes          │
              │  nm_mes          │
              │  nu_dia          │
              │  fl_fim_semana   │
              └────────┬─────────┘
                       │ sk_tempo
                       │
┌──────────────────┐   │   ┌───────────────────────┐
│  dim_parlamentar │   │   │     fato_despesas      │
│ sk_parlamentar(PK)├──┼───┤ sk_despesa (PK)        │
│ nm_parlamentar   │   │   │ sk_tempo (FK)          │
│ sg_uf            │   │   │ sk_parlamentar (FK)    │
│ sg_partido       │   │   │ sk_categoria (FK)      │
│ nm_partido       │   │   │ sk_fornecedor (FK)     │
│ nu_legislatura   │   │   │ vl_documento           │
└──────────────────┘   │   │ vl_glosa               │
                       │   │ vl_liquido             │
┌──────────────────┐   │   │ nr_documento           │
│dim_categoria_    │   │   └───────────────────────┘
│despesa           ├───┘
│ sk_categoria (PK)|       ┌──────────────────┐
│ ds_categoria     │       │  dim_fornecedor  │
│ ds_especificacao │       │ sk_fornecedor(PK)│
└──────────────────┘       │ nm_fornecedor    │
                           │ nr_cnpj_cpf      │
                           │ ds_tipo          │
                           └──────────────────┘
```
