import sys
import logging
from datetime import datetime
from google.cloud import bigquery
import google.auth

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, ".")
from config import GCP_PROJECT_ID, BQ_DATASET_BRONZE, BQ_DATASET_SILVER

# FinOps: partition/cluster only tables large enough to benefit —
# partitioning tiny tables adds metadata overhead without pruning gains.
PARTITION_BY_ANO = {"alunos_clean", "alfabetizacao_municipio_clean"}
CLUSTER_FIELDS = {
    "alunos_clean": ["id_municipio", "serie"],
    "alfabetizacao_municipio_clean": ["id_municipio"],
    "metas_consolidadas": ["escopo"],
}

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
              AND nome_uf IS NOT NULL
              AND serie IS NOT NULL
              AND rede IS NOT NULL
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
            proporcao_aluno_nivel_0 + proporcao_aluno_nivel_1
                AS proporcao_abaixo_basico,
            proporcao_aluno_nivel_2 + proporcao_aluno_nivel_3
                AS proporcao_basico,
            proporcao_aluno_nivel_4 + proporcao_aluno_nivel_5
            + proporcao_aluno_nivel_6 + proporcao_aluno_nivel_7
            + proporcao_aluno_nivel_8
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
            CAST(NULL AS STRING) AS nivel_alfabetizacao,
            'brasil' AS escopo,
            CURRENT_TIMESTAMP() AS silver_processado_ts
        FROM `{GCP_PROJECT_ID}.bronze.meta_brasil`
        WHERE rede IS NOT NULL
          AND taxa_alfabetizacao IS NOT NULL
          AND meta_alfabetizacao_2024 IS NOT NULL
          AND meta_alfabetizacao_2025 IS NOT NULL
          AND meta_alfabetizacao_2026 IS NOT NULL
          AND meta_alfabetizacao_2027 IS NOT NULL
          AND meta_alfabetizacao_2028 IS NOT NULL
          AND meta_alfabetizacao_2029 IS NOT NULL
          AND meta_alfabetizacao_2030 IS NOT NULL
          AND percentual_participacao IS NOT NULL

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
            CAST(NULL AS STRING) AS nivel_alfabetizacao,
            'uf' AS escopo,
            CURRENT_TIMESTAMP() AS silver_processado_ts
        FROM `{GCP_PROJECT_ID}.bronze.meta_uf`
        WHERE sigla_uf IS NOT NULL
          AND nome_uf IS NOT NULL
          AND rede IS NOT NULL
          AND taxa_alfabetizacao IS NOT NULL
          AND meta_alfabetizacao_2024 IS NOT NULL
          AND meta_alfabetizacao_2025 IS NOT NULL
          AND meta_alfabetizacao_2026 IS NOT NULL
          AND meta_alfabetizacao_2027 IS NOT NULL
          AND meta_alfabetizacao_2028 IS NOT NULL
          AND meta_alfabetizacao_2029 IS NOT NULL
          AND meta_alfabetizacao_2030 IS NOT NULL
          AND percentual_participacao IS NOT NULL

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
            CAST(nivel_alfabetizacao AS STRING) AS nivel_alfabetizacao,
            'municipio' AS escopo,
            CURRENT_TIMESTAMP() AS silver_processado_ts
        FROM `{GCP_PROJECT_ID}.bronze.meta_municipio`
        WHERE id_municipio IS NOT NULL
          AND nome_municipio IS NOT NULL
          AND rede IS NOT NULL
          AND taxa_alfabetizacao IS NOT NULL
          AND meta_alfabetizacao_2024 IS NOT NULL
          AND meta_alfabetizacao_2025 IS NOT NULL
          AND meta_alfabetizacao_2026 IS NOT NULL
          AND meta_alfabetizacao_2027 IS NOT NULL
          AND meta_alfabetizacao_2028 IS NOT NULL
          AND meta_alfabetizacao_2029 IS NOT NULL
          AND meta_alfabetizacao_2030 IS NOT NULL
          AND percentual_participacao IS NOT NULL
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
              AND nome_municipio IS NOT NULL
              AND serie IS NOT NULL
              AND rede IS NOT NULL
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
            proporcao_aluno_nivel_0 + proporcao_aluno_nivel_1
                AS proporcao_abaixo_basico,
            proporcao_aluno_nivel_2 + proporcao_aluno_nivel_3
                AS proporcao_basico,
            proporcao_aluno_nivel_4 + proporcao_aluno_nivel_5
            + proporcao_aluno_nivel_6 + proporcao_aluno_nivel_7
            + proporcao_aluno_nivel_8
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
          AND nome_municipio IS NOT NULL
          AND serie IS NOT NULL
          AND rede IS NOT NULL
          AND preenchimento_caderno IS NOT NULL
          AND alfabetizado IS NOT NULL
          AND peso_aluno IS NOT NULL
          AND caderno IS NOT NULL
    """,
}

# Streaming events carry a sparse payload (each event type fills a different
# subset of metric columns). To keep the silver zero-NULL policy, events are
# unpivoted to long format: one row per (event, metric), NULL metrics dropped.
STREAMING_TRANSFORM = f"""
    WITH dedup AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY pubsub_message_id
                ORDER BY ingestao_ts DESC
            ) AS rn
        FROM `{GCP_PROJECT_ID}.bronze.streaming_eventos`
        WHERE tipo IS NOT NULL
          AND timestamp IS NOT NULL
          AND sigla_uf IS NOT NULL
          AND pubsub_message_id IS NOT NULL
          AND (taxa_alfabetizacao IS NULL OR taxa_alfabetizacao BETWEEN 0 AND 100)
    )
    SELECT
        tipo,
        timestamp AS evento_ts,
        ano,
        sigla_uf,
        serie,
        rede,
        source,
        metrica,
        valor,
        pubsub_message_id,
        ingestao_ts,
        CURRENT_TIMESTAMP() AS silver_processado_ts
    FROM dedup,
    UNNEST([
        STRUCT('taxa_alfabetizacao'  AS metrica, CAST(taxa_alfabetizacao  AS STRING) AS valor),
        STRUCT('media_portugues'     AS metrica, CAST(media_portugues     AS STRING) AS valor),
        STRUCT('meta_2030'           AS metrica, CAST(meta_2030           AS STRING) AS valor),
        STRUCT('percentual_atingido' AS metrica, CAST(percentual_atingido AS STRING) AS valor),
        STRUCT('taxa_anterior'       AS metrica, CAST(taxa_anterior       AS STRING) AS valor),
        STRUCT('taxa_revisada'       AS metrica, CAST(taxa_revisada       AS STRING) AS valor),
        STRUCT('motivo_revisao'      AS metrica, motivo_revisao           AS valor)
    ])
    WHERE rn = 1 AND valor IS NOT NULL
"""


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{dataset_id}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)


def bronze_table_exists(client: bigquery.Client, table_name: str) -> bool:
    try:
        client.get_table(f"{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{table_name}")
        return True
    except Exception:
        return False


def transform_table(client: bigquery.Client, table_name: str, query: str) -> int:
    destination = f"{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.{table_name}"
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

    transforms = dict(TRANSFORMS)
    if bronze_table_exists(client, "streaming_eventos"):
        transforms["streaming_eventos_clean"] = STREAMING_TRANSFORM
    else:
        log.info("bronze.streaming_eventos not found — skipping streaming transform")

    for table_name, query in transforms.items():
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
