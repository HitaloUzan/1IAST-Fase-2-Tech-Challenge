import sys
import logging
from datetime import datetime
from google.cloud import bigquery
import google.auth

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, ".")
from config import GCP_PROJECT_ID, BQ_DATASET_BRONZE, BQ_DATASET_SILVER

TRANSFORMS = {
    "alfabetizacao_uf_clean": f"""
        WITH dedup AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY ano, sigla_uf, serie, rede
                    ORDER BY ingestao_ts DESC
                ) AS rn
            FROM `{GCP_PROJECT_ID}.bronze.alfabetizacao_uf`
            WHERE taxa_alfabetizacao IS NOT NULL
              AND taxa_alfabetizacao BETWEEN 0 AND 100
        )
        SELECT
            ano,
            sigla_uf,
            UPPER(TRIM(sigla_uf)) AS sigla_uf_norm,
            nome_uf,
            serie,
            rede,
            taxa_alfabetizacao,
            media_portugues,
            proporcao_aluno_nivel_0,
            proporcao_aluno_nivel_1,
            proporcao_aluno_nivel_2,
            proporcao_aluno_nivel_3,
            proporcao_aluno_nivel_4,
            proporcao_aluno_nivel_5,
            proporcao_aluno_nivel_6,
            proporcao_aluno_nivel_7,
            proporcao_aluno_nivel_8,
            COALESCE(proporcao_aluno_nivel_0, 0) + COALESCE(proporcao_aluno_nivel_1, 0)
                AS proporcao_abaixo_basico,
            COALESCE(proporcao_aluno_nivel_2, 0) + COALESCE(proporcao_aluno_nivel_3, 0)
                AS proporcao_basico,
            COALESCE(proporcao_aluno_nivel_4, 0) + COALESCE(proporcao_aluno_nivel_5, 0)
            + COALESCE(proporcao_aluno_nivel_6, 0) + COALESCE(proporcao_aluno_nivel_7, 0)
            + COALESCE(proporcao_aluno_nivel_8, 0)
                AS proporcao_adequado_avancado,
            ingestao_ts,
            CURRENT_TIMESTAMP() AS silver_processado_ts
        FROM dedup
        WHERE rn = 1
    """,

    "metas_consolidadas": f"""
        SELECT
            ano,
            CAST(NULL AS STRING) AS sigla_uf,
            CAST(NULL AS STRING) AS nome_uf,
            CAST(NULL AS STRING) AS id_municipio,
            CAST(NULL AS STRING) AS nome_municipio,
            rede,
            taxa_alfabetizacao,
            meta_alfabetizacao_2024,
            meta_alfabetizacao_2025,
            meta_alfabetizacao_2026,
            meta_alfabetizacao_2027,
            meta_alfabetizacao_2028,
            meta_alfabetizacao_2029,
            meta_alfabetizacao_2030,
            percentual_participacao,
            'brasil' AS escopo,
            CURRENT_TIMESTAMP() AS silver_processado_ts
        FROM `{GCP_PROJECT_ID}.bronze.meta_brasil`

        UNION ALL

        SELECT
            ano,
            sigla_uf,
            nome_uf,
            CAST(NULL AS STRING) AS id_municipio,
            CAST(NULL AS STRING) AS nome_municipio,
            rede,
            taxa_alfabetizacao,
            meta_alfabetizacao_2024,
            meta_alfabetizacao_2025,
            meta_alfabetizacao_2026,
            meta_alfabetizacao_2027,
            meta_alfabetizacao_2028,
            meta_alfabetizacao_2029,
            meta_alfabetizacao_2030,
            percentual_participacao,
            'uf' AS escopo,
            CURRENT_TIMESTAMP() AS silver_processado_ts
        FROM `{GCP_PROJECT_ID}.bronze.meta_uf`

        UNION ALL

        SELECT
            ano,
            CAST(NULL AS STRING) AS sigla_uf,
            CAST(NULL AS STRING) AS nome_uf,
            CAST(id_municipio AS STRING) AS id_municipio,
            nome_municipio,
            rede,
            taxa_alfabetizacao,
            meta_alfabetizacao_2024,
            meta_alfabetizacao_2025,
            meta_alfabetizacao_2026,
            meta_alfabetizacao_2027,
            meta_alfabetizacao_2028,
            meta_alfabetizacao_2029,
            meta_alfabetizacao_2030,
            percentual_participacao,
            'municipio' AS escopo,
            CURRENT_TIMESTAMP() AS silver_processado_ts
        FROM `{GCP_PROJECT_ID}.bronze.meta_municipio`
    """,

    "alfabetizacao_municipio_clean": f"""
        WITH dedup AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY ano, id_municipio, serie, rede
                    ORDER BY ingestao_ts DESC
                ) AS rn
            FROM `{GCP_PROJECT_ID}.bronze.alfabetizacao_municipio`
            WHERE taxa_alfabetizacao IS NOT NULL
              AND taxa_alfabetizacao BETWEEN 0 AND 100
        )
        SELECT
            ano,
            id_municipio,
            nome_municipio,
            serie,
            rede,
            taxa_alfabetizacao,
            media_portugues,
            proporcao_aluno_nivel_0,
            proporcao_aluno_nivel_1,
            proporcao_aluno_nivel_2,
            proporcao_aluno_nivel_3,
            proporcao_aluno_nivel_4,
            proporcao_aluno_nivel_5,
            proporcao_aluno_nivel_6,
            proporcao_aluno_nivel_7,
            proporcao_aluno_nivel_8,
            COALESCE(proporcao_aluno_nivel_0, 0) + COALESCE(proporcao_aluno_nivel_1, 0)
                AS proporcao_abaixo_basico,
            COALESCE(proporcao_aluno_nivel_2, 0) + COALESCE(proporcao_aluno_nivel_3, 0)
                AS proporcao_basico,
            COALESCE(proporcao_aluno_nivel_4, 0) + COALESCE(proporcao_aluno_nivel_5, 0)
            + COALESCE(proporcao_aluno_nivel_6, 0) + COALESCE(proporcao_aluno_nivel_7, 0)
            + COALESCE(proporcao_aluno_nivel_8, 0)
                AS proporcao_adequado_avancado,
            ingestao_ts,
            CURRENT_TIMESTAMP() AS silver_processado_ts
        FROM dedup
        WHERE rn = 1
    """,

    "alunos_clean": f"""
        SELECT
            ano,
            id_municipio,
            nome_municipio,
            id_escola,
            id_aluno,
            caderno,
            serie,
            rede,
            presenca,
            preenchimento_caderno,
            alfabetizado,
            proficiencia,
            peso_aluno,
            ingestao_ts,
            CURRENT_TIMESTAMP() AS silver_processado_ts
        FROM `{GCP_PROJECT_ID}.bronze.alunos`
        WHERE presenca = 'Presente'
          AND proficiencia IS NOT NULL
    """,
}


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{dataset_id}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)


def transform_table(client: bigquery.Client, table_name: str, query: str) -> int:
    destination = f"{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.{table_name}"
    job_config = bigquery.QueryJobConfig(
        destination=destination,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    log.info(f"Transforming silver.{table_name} ...")
    job = client.query(query, job_config=job_config)
    job.result()
    table = client.get_table(destination)
    log.info(f"silver.{table_name} -> {table.num_rows:,} rows")
    return table.num_rows


def main() -> None:
    credentials, _ = google.auth.default()
    client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

    ensure_dataset(client, BQ_DATASET_SILVER)

    start = datetime.now()
    results = {}
    errors = []

    for table_name, query in TRANSFORMS.items():
        try:
            rows = transform_table(client, table_name, query)
            results[table_name] = rows
        except Exception as exc:
            log.error(f"Failed to transform silver.{table_name}: {exc}")
            errors.append(table_name)

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"Silver transforms complete in {elapsed:.1f}s | success={len(results)} error={len(errors)}")

    if errors:
        log.error(f"Tables with errors: {errors}")
        sys.exit(1)


if __name__ == "__main__":
    main()
