import sys
import logging
from datetime import datetime
from google.cloud import bigquery
import google.auth

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, ".")
from config import GCP_PROJECT_ID, BQ_DATASET_BRONZE, BDD_PROJECT, BDD_DATASET, BDD_DIR_DATASET

BDD_SRC = f"{BDD_PROJECT}.{BDD_DATASET}"
BDD_DIR = f"{BDD_PROJECT}.{BDD_DIR_DATASET}"

# FinOps: partition/cluster only tables large enough to benefit —
# partitioning tiny tables adds metadata overhead without pruning gains.
PARTITION_BY_ANO = {"alunos", "alfabetizacao_municipio"}
CLUSTER_FIELDS = {
    "alunos": ["id_municipio", "serie"],
    "alfabetizacao_municipio": ["id_municipio"],
    "meta_municipio": ["id_municipio"],
}

TABLES = {
    "alfabetizacao_uf": f"""
        WITH
        dicionario_serie AS (
            SELECT chave AS chave_serie, valor AS descricao_serie
            FROM `{BDD_SRC}.dicionario`
            WHERE nome_coluna = 'serie' AND id_tabela = 'uf'
        ),
        dicionario_rede AS (
            SELECT chave AS chave_rede, valor AS descricao_rede
            FROM `{BDD_SRC}.dicionario`
            WHERE nome_coluna = 'rede' AND id_tabela = 'uf'
        )
        SELECT
            dados.ano,
            dados.sigla_uf,
            diretorio_sigla_uf.nome AS nome_uf,
            descricao_serie AS serie,
            descricao_rede AS rede,
            dados.taxa_alfabetizacao,
            dados.media_portugues,
            dados.proporcao_aluno_nivel_0,
            dados.proporcao_aluno_nivel_1,
            dados.proporcao_aluno_nivel_2,
            dados.proporcao_aluno_nivel_3,
            dados.proporcao_aluno_nivel_4,
            dados.proporcao_aluno_nivel_5,
            dados.proporcao_aluno_nivel_6,
            dados.proporcao_aluno_nivel_7,
            dados.proporcao_aluno_nivel_8,
            CURRENT_TIMESTAMP() AS ingestao_ts
        FROM `{BDD_SRC}.uf` AS dados
        LEFT JOIN (SELECT DISTINCT sigla, nome FROM `{BDD_DIR}.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        LEFT JOIN dicionario_serie ON CAST(dados.serie AS STRING) = chave_serie
        LEFT JOIN dicionario_rede  ON CAST(dados.rede  AS STRING) = chave_rede
    """,

    "meta_brasil": f"""
        SELECT
            dados.ano,
            dados.rede,
            dados.taxa_alfabetizacao,
            dados.meta_alfabetizacao_2024,
            dados.meta_alfabetizacao_2025,
            dados.meta_alfabetizacao_2026,
            dados.meta_alfabetizacao_2027,
            dados.meta_alfabetizacao_2028,
            dados.meta_alfabetizacao_2029,
            dados.meta_alfabetizacao_2030,
            dados.percentual_participacao,
            CURRENT_TIMESTAMP() AS ingestao_ts
        FROM `{BDD_SRC}.meta_alfabetizacao_brasil` AS dados
    """,

    "meta_uf": f"""
        SELECT
            dados.ano,
            dados.sigla_uf,
            diretorio_sigla_uf.nome AS nome_uf,
            dados.rede,
            dados.taxa_alfabetizacao,
            dados.meta_alfabetizacao_2024,
            dados.meta_alfabetizacao_2025,
            dados.meta_alfabetizacao_2026,
            dados.meta_alfabetizacao_2027,
            dados.meta_alfabetizacao_2028,
            dados.meta_alfabetizacao_2029,
            dados.meta_alfabetizacao_2030,
            dados.percentual_participacao,
            CURRENT_TIMESTAMP() AS ingestao_ts
        FROM `{BDD_SRC}.meta_alfabetizacao_uf` AS dados
        LEFT JOIN (SELECT DISTINCT sigla, nome FROM `{BDD_DIR}.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
    """,

    "meta_municipio": f"""
        SELECT
            dados.ano,
            dados.id_municipio,
            diretorio_id_municipio.nome AS nome_municipio,
            dados.rede,
            dados.taxa_alfabetizacao,
            dados.meta_alfabetizacao_2024,
            dados.meta_alfabetizacao_2025,
            dados.meta_alfabetizacao_2026,
            dados.meta_alfabetizacao_2027,
            dados.meta_alfabetizacao_2028,
            dados.meta_alfabetizacao_2029,
            dados.meta_alfabetizacao_2030,
            dados.nivel_alfabetizacao,
            dados.percentual_participacao,
            CURRENT_TIMESTAMP() AS ingestao_ts
        FROM `{BDD_SRC}.meta_alfabetizacao_municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio, nome FROM `{BDD_DIR}.municipio`) AS diretorio_id_municipio
            ON CAST(dados.id_municipio AS STRING) = CAST(diretorio_id_municipio.id_municipio AS STRING)
    """,

    "alfabetizacao_municipio": f"""
        WITH
        dicionario_serie AS (
            SELECT chave AS chave_serie, valor AS descricao_serie
            FROM `{BDD_SRC}.dicionario`
            WHERE nome_coluna = 'serie' AND id_tabela = 'municipio'
        ),
        dicionario_rede AS (
            SELECT chave AS chave_rede, valor AS descricao_rede
            FROM `{BDD_SRC}.dicionario`
            WHERE nome_coluna = 'rede' AND id_tabela = 'municipio'
        )
        SELECT
            dados.ano,
            dados.id_municipio,
            diretorio_id_municipio.nome AS nome_municipio,
            descricao_serie AS serie,
            descricao_rede AS rede,
            dados.taxa_alfabetizacao,
            dados.media_portugues,
            dados.proporcao_aluno_nivel_0,
            dados.proporcao_aluno_nivel_1,
            dados.proporcao_aluno_nivel_2,
            dados.proporcao_aluno_nivel_3,
            dados.proporcao_aluno_nivel_4,
            dados.proporcao_aluno_nivel_5,
            dados.proporcao_aluno_nivel_6,
            dados.proporcao_aluno_nivel_7,
            dados.proporcao_aluno_nivel_8,
            CURRENT_TIMESTAMP() AS ingestao_ts
        FROM `{BDD_SRC}.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio, nome FROM `{BDD_DIR}.municipio`) AS diretorio_id_municipio
            ON CAST(dados.id_municipio AS STRING) = CAST(diretorio_id_municipio.id_municipio AS STRING)
        LEFT JOIN dicionario_serie ON CAST(dados.serie AS STRING) = chave_serie
        LEFT JOIN dicionario_rede  ON CAST(dados.rede  AS STRING) = chave_rede
    """,

    "alunos": f"""
        WITH
        dicionario_serie AS (
            SELECT chave AS chave_serie, valor AS descricao_serie
            FROM `{BDD_SRC}.dicionario`
            WHERE nome_coluna = 'serie' AND id_tabela = 'alunos'
        ),
        dicionario_rede AS (
            SELECT chave AS chave_rede, valor AS descricao_rede
            FROM `{BDD_SRC}.dicionario`
            WHERE nome_coluna = 'rede' AND id_tabela = 'alunos'
        ),
        dicionario_presenca AS (
            SELECT chave AS chave_presenca, valor AS descricao_presenca
            FROM `{BDD_SRC}.dicionario`
            WHERE nome_coluna = 'presenca' AND id_tabela = 'alunos'
        ),
        dicionario_preenchimento_caderno AS (
            SELECT chave AS chave_preenchimento_caderno, valor AS descricao_preenchimento_caderno
            FROM `{BDD_SRC}.dicionario`
            WHERE nome_coluna = 'preenchimento_caderno' AND id_tabela = 'alunos'
        ),
        dicionario_alfabetizado AS (
            SELECT chave AS chave_alfabetizado, valor AS descricao_alfabetizado
            FROM `{BDD_SRC}.dicionario`
            WHERE nome_coluna = 'alfabetizado' AND id_tabela = 'alunos'
        )
        SELECT
            dados.ano,
            dados.id_municipio,
            diretorio_id_municipio.nome AS nome_municipio,
            dados.id_escola,
            dados.id_aluno,
            dados.caderno,
            descricao_serie AS serie,
            descricao_rede AS rede,
            descricao_presenca AS presenca,
            descricao_preenchimento_caderno AS preenchimento_caderno,
            descricao_alfabetizado AS alfabetizado,
            dados.proficiencia,
            dados.peso_aluno,
            CURRENT_TIMESTAMP() AS ingestao_ts
        FROM `{BDD_SRC}.alunos` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio, nome FROM `{BDD_DIR}.municipio`) AS diretorio_id_municipio
            ON CAST(dados.id_municipio AS STRING) = CAST(diretorio_id_municipio.id_municipio AS STRING)
        LEFT JOIN dicionario_serie
            ON CAST(dados.serie AS STRING) = chave_serie
        LEFT JOIN dicionario_rede
            ON CAST(dados.rede AS STRING) = chave_rede
        LEFT JOIN dicionario_presenca
            ON CAST(dados.presenca AS STRING) = chave_presenca
        LEFT JOIN dicionario_preenchimento_caderno
            ON CAST(dados.preenchimento_caderno AS STRING) = chave_preenchimento_caderno
        LEFT JOIN dicionario_alfabetizado
            ON CAST(dados.alfabetizado AS STRING) = chave_alfabetizado
    """,
}


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{dataset_id}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)


def ingest_table(client: bigquery.Client, table_name: str, query: str) -> int:
    destination = f"{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{table_name}"
    job_config = bigquery.QueryJobConfig(
        destination=destination,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    if table_name in PARTITION_BY_ANO:
        job_config.range_partitioning = bigquery.RangePartitioning(
            field="ano",
            range_=bigquery.PartitionRange(start=2019, end=2051, interval=1),
        )
    if table_name in CLUSTER_FIELDS:
        job_config.clustering_fields = CLUSTER_FIELDS[table_name]
    log.info(f"Ingesting bronze.{table_name} ...")
    job = client.query(query, job_config=job_config)
    job.result()
    table = client.get_table(destination)
    log.info(f"bronze.{table_name} -> {table.num_rows:,} rows")
    return table.num_rows


def main() -> None:
    credentials, _ = google.auth.default()
    client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

    ensure_dataset(client, BQ_DATASET_BRONZE)

    start = datetime.now()
    results = {}
    errors = []

    for table_name, query in TABLES.items():
        try:
            rows = ingest_table(client, table_name, query)
            results[table_name] = rows
        except Exception as exc:
            log.error(f"Failed to ingest bronze.{table_name}: {exc}")
            errors.append(table_name)

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"Bronze ingestion complete in {elapsed:.1f}s | success={len(results)} error={len(errors)}")

    if errors:
        log.error(f"Tables with errors: {errors}")
        sys.exit(1)


if __name__ == "__main__":
    main()
