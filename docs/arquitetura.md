# Arquitetura

## Visão Geral

![Arquitetura Medalhão](assets/imagem_github.jpeg)

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
