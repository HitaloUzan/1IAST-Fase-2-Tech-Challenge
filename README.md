# Pipeline Híbrido para Análise da Alfabetização no Brasil

**Tech Challenge — Fase 2 | PosTech FIAP | IA Science**

---

## Contexto do Problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do Brasil. O **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados, Distrito Federal e municípios com a meta de garantir que **todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do Ensino Fundamental até 2030**.

Para medir esse avanço, o INEP criou o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem **743 pontos na escala SAEB** — ponto de corte definido pela Pesquisa Alfabetiza Brasil (2023). Compreender os fatores que influenciam esse indicador exige integrar múltiplas fontes: metas nacionais, estaduais e municipais, microdados educacionais e dados territoriais.

---

## Arquitetura da Solução

```
┌─────────────────────────────────────────────────────────────┐
│           FONTE: Base dos Dados (BigQuery público)          │
│   basedosdados.br_inep_avaliacao_alfabetizacao              │
│   ├── uf                  ├── meta_alfabetizacao_brasil     │
│   ├── municipio           ├── meta_alfabetizacao_uf         │
│   ├── alunos              └── meta_alfabetizacao_municipio  │
└───────────────────┬─────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │ INGESTÃO HÍBRIDA      │
        ├───────────────────────┤
        │  BATCH (periódico)    │  STREAMING (Pub/Sub)
        │  ingest_bronze.py     │  producer.py → consumer.py
        │  6 tabelas completas  │  eventos simulados em RT
        └───────────┬───────────┘
                    │
    ╔═══════════════▼═════════════════════╗
    ║  BRONZE — Dados Brutos (BigQuery)   ║
    ║  bronze.alfabetizacao_uf            ║
    ║  bronze.meta_brasil                 ║
    ║  bronze.meta_uf                     ║
    ║  bronze.meta_municipio              ║
    ║  bronze.alfabetizacao_municipio     ║
    ║  bronze.alunos                      ║
    ║  bronze.streaming_eventos           ║
    ╚═══════════════╦═════════════════════╝
                    ║ transform_silver.py
                    ║ dedup · filtro · normalização · integração
    ╔═══════════════▼═════════════════════╗
    ║  SILVER — Dados Tratados            ║
    ║  silver.alfabetizacao_uf_clean      ║
    ║  silver.metas_consolidadas          ║
    ║  silver.alfabetizacao_municipio_clean║
    ║  silver.alunos_clean                ║
    ║  silver.streaming_eventos_clean     ║
    ╚═══════════════╦═════════════════════╝
                    ║ build_gold.py
                    ║ agregação · ranking · JOIN metas
    ╔═══════════════▼═════════════════════╗
    ║  GOLD — Camada Analítica            ║
    ║  gold.indicador_por_uf_ano          ║
    ║  gold.evolucao_temporal_brasil      ║
    ║  gold.ranking_estados               ║
    ║  gold.perfil_desempenho_uf          ║
    ║  gold.painel_municipios             ║
    ╚═════════════════════════════════════╝
                    │
            quality/validate.py
            checks bronze · silver · gold
```

### Fluxo de Dados

1. **Ingestão batch** (`ingest_bronze.py`): 6 consultas ao dataset público `basedosdados.br_inep_avaliacao_alfabetizacao` no BigQuery, enriquecidas com os diretórios de UF/município e o dicionário de códigos, gravadas em `bronze.*` por *full refresh* — as tabelas grandes já nascem particionadas por `ano` e clusterizadas por chave de consulta. Os códigos crus da fonte são preservados ao lado das descrições decodificadas (`serie_codigo`/`serie`, `rede_codigo`/`rede`, etc.), mantendo o bronze fiel ao raw data: o enriquecimento é aditivo, nunca substitui o dado original.
2. **Ingestão streaming** (`producer.py` → Pub/Sub → `consumer.py`): eventos simulados de medição de desempenho, atualização de meta e revisão de indicador são publicados no tópico `alfabetizacao-streaming`; o consumidor cria a infraestrutura (tópico, subscription e tabela) se não existir, consome as mensagens e as grava em `bronze.streaming_eventos`. Roda automaticamente a cada execução do pipeline — sem intervenção manual — logo após a ingestão batch (ver `run_pipeline.py` e `.github/workflows/pipeline.yml`).
3. **Transformação silver** (`transform_silver.py`): deduplicação por chave via `ROW_NUMBER()`, filtro de linhas com enriquecimento incompleto, normalização de tipos e chaves, consolidação das 3 tabelas de metas em `silver.metas_consolidadas` e normalização dos eventos de streaming para formato longo (uma linha por métrica, sem NULL).
4. **Camada gold** (`build_gold.py`): agregações por UF/ano, ranking de estados, evolução temporal, perfil de desempenho por níveis e painel municipal — todas com `INNER JOIN` contra as metas, prontas para dashboard e ML.
5. **Qualidade** (`validate.py`): checks de existência/volume, duplicidade, domínio (UFs válidas, taxa em [0,100]), NULLs em colunas críticas e integridade referencial entre gold e silver; `exit 1` interrompe o CI em caso de falha.

---

## Fontes de Dados

| Tabela Bronze | Fonte Base dos Dados | Descrição |
|---|---|---|
| `alfabetizacao_uf` | `uf` + `dicionario` + diretório UF | Taxa por estado/série/rede |
| `meta_brasil` | `meta_alfabetizacao_brasil` | Metas nacionais 2024–2030 |
| `meta_uf` | `meta_alfabetizacao_uf` | Metas por estado 2024–2030 |
| `meta_municipio` | `meta_alfabetizacao_municipio` | Metas municipais 2024–2030 |
| `alfabetizacao_municipio` | `municipio` + `dicionario` | Taxa por município/série/rede |
| `alunos` | `alunos` + `dicionario` | Microdados individuais de alunos |

---

## Tecnologias Utilizadas

| Componente | Tecnologia | Justificativa |
|---|---|---|
| Cloud | **GCP** | Free tier generoso; BigQuery e Pub/Sub nativos e integrados |
| Data Warehouse | **BigQuery** | SQL serverless, 1 TB/mês grátis, sem VMs para gerenciar |
| Streaming | **Google Pub/Sub** | Integração nativa com BigQuery; 10 GB/mês no free tier |
| Linguagem | **Python 3.11** | Bibliotecas maduras para GCP; padrão na engenharia de dados |
| CI/CD | **GitHub Actions** | Orquestração gratuita com autenticação via Workload Identity |
| Qualidade | **Validações nativas BigQuery** | Sem dependência de frameworks externos |

---

## Como Executar Localmente

### Pré-requisitos

1. Conta GCP com projeto `pipeline-alfabetizacao`
2. APIs habilitadas: BigQuery API, Pub/Sub API
3. Service Account com roles:
   - `BigQuery Data Editor`
   - `BigQuery Job User`
   - `Pub/Sub Editor`
4. Arquivo JSON da service account salvo em `credentials/service-account.json`

### Setup

```bash
pip install -r requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS="credentials/service-account.json"
```

### Executar pipeline completo

```bash
python run_pipeline.py
```

### Executar etapas individualmente

```bash
# Bronze
python ingestion/batch/ingest_bronze.py

# Silver
python silver/transform_silver.py

# Gold
python gold/build_gold.py

# Qualidade
python quality/validate.py --camada all
python quality/validate.py --camada bronze
python quality/validate.py --camada silver
python quality/validate.py --camada gold
```

### Streaming

`python run_pipeline.py` já orquestra o streaming automaticamente: sobe o consumidor em
background, aguarda a subscription conectar, dispara o produtor e espera o consumidor
finalizar antes de seguir para a camada silver — sem necessidade de terminais separados
ou intervenção manual. Use `--skip-streaming` para pular essa etapa.

Para rodar o streaming isoladamente (debug), ainda é possível disparar os dois scripts
à mão em terminais separados:

```bash
# Terminal 1 — consumidor (deve estar rodando antes do produtor)
python streaming/consumer.py --max-mensagens 20 --timeout 60

# Terminal 2 — produtor
python streaming/producer.py --eventos 20 --intervalo 1.0
```

O consumidor e o produtor criam automaticamente o tópico, a subscription e a tabela
`bronze.streaming_eventos` na primeira execução — para isso a service account precisa
da role `Pub/Sub Editor` (que inclui `pubsub.topics.create`). Após consumir eventos,
rode `python silver/transform_silver.py` para materializar `silver.streaming_eventos_clean`
(isso já é feito automaticamente pelo `run_pipeline.py` e pelo GitHub Actions).

---

## GitHub Actions — Configuração

O workflow `.github/workflows/pipeline.yml` executa o pipeline completo — batch, streaming, silver, gold e qualidade — automaticamente todo dia às 6h UTC e pode ser disparado manualmente.

### Criando o secret `GCP_SA_KEY`

1. No GCP Console, baixe o JSON da service account
2. No repositório GitHub: **Settings → Secrets and variables → Actions → New repository secret**
3. Nome: `GCP_SA_KEY`
4. Valor: conteúdo completo do JSON

### Permissões da service account

```
roles/bigquery.dataEditor
roles/bigquery.jobUser
roles/pubsub.editor
```

---

## Decisões Arquiteturais

### Batch vs Streaming

O volume de dados do INEP é atualizado anualmente, tornando **batch** a abordagem principal. O componente **streaming via Pub/Sub** simula a ingestão de atualizações em tempo quase real (novas medições, revisões de metas), preparando a arquitetura para quando o INEP publicar dados em fluxo contínuo.

### Data Lake vs Data Warehouse

Optamos por **BigQuery como único destino** (sem Cloud Storage separado), eliminando camadas intermediárias de armazenamento e reduzindo custo. O BigQuery opera como data lake para bronze (schema-on-write flexível) e data warehouse para gold (tabelas analíticas tipadas).

### WRITE_TRUNCATE vs Append

Todas as tabelas usam `WRITE_TRUNCATE` para garantir idempotência: reexecuções não acumulam duplicatas e o custo de armazenamento permanece estável. O histórico completo é mantido pelo campo `ingestao_ts`.

### Custo vs Performance

Queries usam `SELECT` explícito (sem `SELECT *`) para minimizar bytes escaneados. Os datasets bronze/silver/gold ficam no mesmo projeto GCP, eliminando cobranças de transferência entre projetos.

### Particionamento e Clustering

As tabelas grandes (`alunos` com ~3,9M linhas e `alfabetizacao_municipio`, no bronze e no silver) são **particionadas por `ano`** (range partitioning) e **clusterizadas** pelas chaves mais filtradas (`id_municipio`, `serie`); `metas_consolidadas` é clusterizada por `escopo`, usado como filtro em todas as queries gold. Tabelas pequenas (UF, metas nacionais, gold agregadas) ficam sem partição de propósito: particionar tabelas de poucos KB adiciona overhead de metadados sem nenhum ganho de *pruning* — uma decisão FinOps documentada, não um esquecimento.

---

## Governança e Qualidade de Dados

### Política de valores ausentes por camada

| Camada | Tolerância a NULL | Justificativa |
|---|---|---|
| **Bronze** | Permitido | Raw layer — preserva os dados exatamente como vieram da fonte, sem transformação. Um `LEFT JOIN` de enriquecimento (nome de UF/município, descrição de série/rede) que não encontra correspondência gera NULL aqui, e isso é esperado: o histórico completo precisa ser preservado mesmo quando o enriquecimento falha. |
| **Silver** | Não permitido em colunas-chave · **permitido, deliberado e reportado** em `proporcao_aluno_nivel_0..8`/`media_portugues` | `transform_silver.py` filtra qualquer linha cujo enriquecimento do bronze tenha falhado (`nome_uf`, `nome_municipio`, `serie`, `rede`, `taxa_alfabetizacao` NULL). As colunas de distribuição por nível de proficiência **não** são forçadas para 0 quando ausentes: ~48% dos municípios/UFs não têm esse detalhamento divulgado pelo INEP na fonte (sigilo estatístico para amostras pequenas), e fabricar `COALESCE(..., 0)` ali violaria a própria consistência da linha (9 colunas de proporção somando 0% junto de uma `taxa_alfabetizacao` real e diferente de zero). O NULL genuíno é preservado e **detectado explicitamente** por `quality/validate.py` (ver Detecção de valores ausentes), em vez de mascarado. `metas_consolidadas` exige que todas as colunas de meta (`meta_alfabetizacao_2024..2030`) estejam preenchidas antes de consolidar a linha. Os eventos de streaming, cujo payload é esparso por tipo de evento, são **despivotados para formato longo** (`streaming_eventos_clean`: uma linha por métrica preenchida), sem inventar valores. |
| **Gold** | Não permitido em colunas-chave · herda o NULL deliberado de `proporcao_topo` quando a silver não tem distribuição | As tabelas gold fazem `INNER JOIN` (em vez de `LEFT JOIN`) contra `silver.metas_consolidadas`. Estados/municípios sem meta completa na fonte (ex.: Acre não tem `meta_alfabetizacao_2024` em nenhum ano de `bronze.meta_uf`) são **excluídos**, em vez de gerar `meta_2030`/`gap_meta` NULL. `perfil_desempenho_uf.proporcao_topo` fica NULL (não zero) quando a UF não tem distribuição por nível na silver — `AVG`/`SUM` em dashboards ignoram NULL nativamente, evitando que uma ausência de dado puxe a média para baixo como um zero fabricado faria.

### Verificação de duplicidade

`quality/validate.py` verifica ausência de chaves duplicadas em `bronze.alfabetizacao_uf` (`ano`, `sigla_uf`, `serie`, `rede`) e de eventos duplicados em `silver.streaming_eventos_clean` (`pubsub_message_id`, `metrica`); o `dedup` via `ROW_NUMBER()` no silver garante que apenas a ingestão mais recente (`ingestao_ts DESC`) sobrevive por chave.

### Validação de chaves e consistência entre tabelas

- `bronze.alfabetizacao_uf` e `silver.alfabetizacao_uf_clean`: valida que todas as UFs pertencem ao conjunto das 27 UFs válidas.
- `silver.alfabetizacao_uf_clean`/`alfabetizacao_municipio_clean`: taxa de alfabetização restrita a `[0, 100]`.
- **Integridade referencial gold → silver**: toda `sigla_uf` de `gold.indicador_por_uf_ano` e todo `id_municipio` de `gold.painel_municipios` devem existir em `silver.metas_consolidadas`, e toda UF de `gold.ranking_estados` deve existir em `silver.alfabetizacao_uf_clean` — chaves órfãs reprovam a validação.
- `gold.ranking_estados`: cobertura comparada contra `silver.metas_consolidadas` (não contra o total de UFs), já que o universo de estados elegíveis no gold é definido por quem tem meta completa, não por quem tem taxa de alfabetização.
- **Consistência da distribuição por nível**: quando `proporcao_aluno_nivel_0..8` está preenchida, a soma das 9 colunas deve ficar em `[99, 101]` (tolerância de arredondamento) — garante que o dado da fonte é internamente coerente antes de ser usado em agregações.

### Detecção de valores ausentes

`quality/validate.py --camada all` roda um check de NULL em colunas críticas nas 4 tabelas silver e nas 5 tabelas gold, retornando `exit 1` se qualquer NULL for encontrado nelas — o mesmo mecanismo que interrompe o GitHub Actions em caso de falha (ver seção Monitoramento).

Para as colunas de distribuição por nível (`proporcao_aluno_nivel_0..8` na silver, `proporcao_topo` na gold), o NULL não é tratado como falha de pipeline: `validate.py` mede e **reporta** o percentual de linhas sem essa distribuição (hoje ~48%, por sigilo estatístico do INEP para municípios/UFs com amostra pequena) como uma métrica informativa, sem interromper o build. Tratar essa ausência estrutural como erro — ou pior, mascará-la com `COALESCE(..., 0)` — seria inconsistente com o próprio requisito de "detecção de valores ausentes": um zero fabricado não é detectável como ausência por ninguém a jusante.

---

## Monitoramento e FinOps

### Monitoramento

- Cada script registra timestamps, contagem de linhas e erros via `logging`
- `quality/validate.py` retorna exit code 1 em falha, interrompendo o GitHub Actions e gerando alerta
- O GitHub Actions notifica falhas por e-mail automaticamente

### FinOps — Práticas Aplicadas

- **Armazenamento colunar**: o BigQuery armazena tudo em formato colunar comprimido (Capacitor, equivalente gerenciado do Parquet) — leituras tocam apenas as colunas selecionadas.
- **Particionamento por `ano` + clustering** nas tabelas grandes: queries analíticas que filtram por ano/município escaneiam só as partições e blocos relevantes (ver Decisões Arquiteturais).
- **Full refresh idempotente**: reexecuções substituem as tabelas em vez de acumular versões, mantendo o storage estável.
- **Micro-lote em vez de streaming insert**: o consumidor grava os eventos consumidos via *load job* (gratuito e ilimitado no BigQuery) em vez do `insertAll` de streaming, que é cobrado por volume — mesma latência prática para o caso de uso e custo zero.
- **Queries com projeção explícita** (sem `SELECT *`) e agregações feitas uma única vez na gold, não a cada dashboard.

### FinOps — Estimativa de Custo

| Recurso | Uso estimado | Custo |
|---|---|---|
| BigQuery Storage | < 1 GB (tabelas do INEP) | $0/mês (free tier: 10 GB) |
| BigQuery Queries | < 50 MB/execução | $0/mês (free tier: 1 TB) |
| Pub/Sub | < 1 MB/dia | $0/mês (free tier: 10 GB) |
| GitHub Actions | < 30 min/dia (~900 min/mês) | $0/mês (free tier: 2.000 min) |
| **Total estimado** | | **$0/mês** |

---

## Aplicação em IA

A camada Gold fornece datasets prontos para treinar modelos preditivos:

**`gold.perfil_desempenho_uf`** — vetor de proporções por nível (0–8) por UF/ano/série:
- **Clustering de vulnerabilidade**: K-means para agrupar estados por perfil de evolução e identificar regiões que precisam de intervenção prioritária
- **Regressão temporal**: ARIMA/Prophet para projetar a taxa de alfabetização por estado até 2030 e calcular a probabilidade de atingir a meta

**`gold.indicador_por_uf_ano` + `gold.ranking_estados`**:
- **Modelos de gap de meta**: prever quais estados não atingirão 100% até 2030 com base na trajetória histórica
- **Análise de desigualdade**: identificar disparidades entre redes (municipal vs. privada) e entre regiões geográficas

**`gold.painel_municipios`**:
- **Predição municipal**: modelo XGBoost com features socioeconômicas (IBGE) + taxa de alfabetização para prever municípios de risco
- **Políticas públicas baseadas em dados**: priorização de recursos do FUNDEB para municípios com maior gap de meta e menor IDH

---

## Estrutura do Repositório

```
1IAST-Fase-2-Tech-Challenge/
├── .github/
│   └── workflows/
│       └── pipeline.yml          # CI/CD — execução semanal automática
├── ingestion/
│   └── batch/
│       └── ingest_bronze.py      # Ingestão de 6 tabelas → bronze
├── silver/
│   └── transform_silver.py       # 5 tabelas silver (limpeza + integração + streaming)
├── gold/
│   └── build_gold.py             # 5 tabelas analíticas gold
├── quality/
│   └── validate.py               # Validação das 3 camadas (exit 1 em falha)
├── streaming/
│   ├── producer.py               # Publica eventos no Pub/Sub
│   └── consumer.py               # Consome Pub/Sub → bronze.streaming_eventos
├── config.py                     # IDs do projeto GCP e datasets
├── run_pipeline.py               # Orquestrador local (batch sequential)
├── requirements.txt
└── README.md
```
