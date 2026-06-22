import sys
import logging
from datetime import datetime
from google.cloud import bigquery
import google.auth

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, ".")
from config import GCP_PROJECT_ID, BQ_DATASET_SILVER, BQ_DATASET_GOLD

GOLD_TABLES = {
    "indicador_por_uf_ano": f"""
        WITH base AS (
            SELECT
                uf.ano,
                uf.sigla_uf,
                uf.nome_uf,
                uf.serie,
                AVG(uf.taxa_alfabetizacao)  AS taxa_media,
                MIN(uf.taxa_alfabetizacao)  AS taxa_min_rede,
                MAX(uf.taxa_alfabetizacao)  AS taxa_max_rede,
                AVG(uf.media_portugues)     AS media_portugues,
                COUNT(DISTINCT uf.rede)     AS qtd_redes
            FROM `{GCP_PROJECT_ID}.silver.alfabetizacao_uf_clean` AS uf
            GROUP BY uf.ano, uf.sigla_uf, uf.nome_uf, uf.serie
        ),
        ranked AS (
            SELECT *,
                RANK() OVER (PARTITION BY ano, serie ORDER BY taxa_media DESC) AS ranking_nacional
            FROM base
        ),
        metas AS (
            SELECT sigla_uf, MAX(meta_alfabetizacao_2030) AS meta_2030
            FROM `{GCP_PROJECT_ID}.silver.metas_consolidadas`
            WHERE escopo = 'uf' AND sigla_uf IS NOT NULL
            GROUP BY sigla_uf
        )
        SELECT
            r.*,
            m.meta_2030,
            r.taxa_media - m.meta_2030 AS gap_meta
        FROM ranked r
        LEFT JOIN metas m ON r.sigla_uf = m.sigla_uf
    """,

    "evolucao_temporal_brasil": f"""
        SELECT
            ano,
            serie,
            AVG(taxa_alfabetizacao)    AS taxa_media_brasil,
            MIN(taxa_alfabetizacao)    AS taxa_min_estado,
            MAX(taxa_alfabetizacao)    AS taxa_max_estado,
            STDDEV(taxa_alfabetizacao) AS desvio_padrao,
            COUNT(DISTINCT sigla_uf)   AS qtd_estados
        FROM `{GCP_PROJECT_ID}.silver.alfabetizacao_uf_clean`
        GROUP BY ano, serie
        ORDER BY ano, serie
    """,

    "ranking_estados": f"""
        WITH ano_recente AS (
            SELECT MAX(ano) AS max_ano FROM `{GCP_PROJECT_ID}.silver.alfabetizacao_uf_clean`
        ),
        base AS (
            SELECT
                uf.sigla_uf,
                uf.nome_uf,
                AVG(uf.taxa_alfabetizacao) AS taxa_media
            FROM `{GCP_PROJECT_ID}.silver.alfabetizacao_uf_clean` AS uf
            JOIN ano_recente ON uf.ano = ano_recente.max_ano
            GROUP BY uf.sigla_uf, uf.nome_uf
        ),
        metas AS (
            SELECT sigla_uf, MAX(meta_alfabetizacao_2030) AS meta_2030
            FROM `{GCP_PROJECT_ID}.silver.metas_consolidadas`
            WHERE escopo = 'uf' AND sigla_uf IS NOT NULL
            GROUP BY sigla_uf
        )
        SELECT
            RANK() OVER (ORDER BY b.taxa_media DESC) AS posicao,
            b.sigla_uf,
            b.nome_uf,
            b.taxa_media,
            m.meta_2030,
            b.taxa_media - m.meta_2030 AS gap_meta,
            CASE
                WHEN b.taxa_media >= 90 THEN 'Alto'
                WHEN b.taxa_media >= 75 THEN 'Médio'
                ELSE 'Requer atenção'
            END AS classificacao
        FROM base b
        LEFT JOIN metas m ON b.sigla_uf = m.sigla_uf
        ORDER BY posicao
    """,

    "perfil_desempenho_uf": f"""
        SELECT
            ano,
            sigla_uf,
            nome_uf,
            serie,
            rede,
            taxa_alfabetizacao,
            proporcao_aluno_nivel_0,
            proporcao_aluno_nivel_1,
            proporcao_aluno_nivel_2,
            proporcao_aluno_nivel_3,
            proporcao_aluno_nivel_4,
            proporcao_aluno_nivel_5,
            proporcao_aluno_nivel_6,
            proporcao_aluno_nivel_7,
            proporcao_aluno_nivel_8,
            proporcao_abaixo_basico,
            proporcao_basico,
            proporcao_adequado_avancado,
            COALESCE(proporcao_aluno_nivel_6, 0)
            + COALESCE(proporcao_aluno_nivel_7, 0)
            + COALESCE(proporcao_aluno_nivel_8, 0) AS proporcao_topo
        FROM `{GCP_PROJECT_ID}.silver.alfabetizacao_uf_clean`
    """,

    "painel_municipios": f"""
        WITH base AS (
            SELECT
                mun.id_municipio,
                mun.nome_municipio,
                mun.ano,
                AVG(mun.taxa_alfabetizacao) AS taxa_media
            FROM `{GCP_PROJECT_ID}.silver.alfabetizacao_municipio_clean` AS mun
            GROUP BY mun.id_municipio, mun.nome_municipio, mun.ano
        ),
        metas AS (
            SELECT
                id_municipio,
                MAX(meta_alfabetizacao_2030) AS meta_2030
            FROM `{GCP_PROJECT_ID}.silver.metas_consolidadas`
            WHERE escopo = 'municipio' AND id_municipio IS NOT NULL
            GROUP BY id_municipio
        )
        SELECT
            b.id_municipio,
            b.nome_municipio,
            b.ano,
            b.taxa_media,
            m.meta_2030,
            b.taxa_media - m.meta_2030                AS gap_meta,
            b.taxa_media >= COALESCE(m.meta_2030, 100) AS atingiu_meta
        FROM base b
        LEFT JOIN metas m ON CAST(b.id_municipio AS STRING) = m.id_municipio
    """,
}


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{dataset_id}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)


def build_table(client: bigquery.Client, table_name: str, query: str) -> int:
    destination = f"{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.{table_name}"
    job_config = bigquery.QueryJobConfig(
        destination=destination,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    log.info(f"Building gold.{table_name} ...")
    job = client.query(query, job_config=job_config)
    job.result()
    table = client.get_table(destination)
    log.info(f"gold.{table_name} -> {table.num_rows:,} rows")
    return table.num_rows


def main() -> None:
    credentials, _ = google.auth.default()
    client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

    ensure_dataset(client, BQ_DATASET_GOLD)

    start = datetime.now()
    results = {}
    errors = []

    for table_name, query in GOLD_TABLES.items():
        try:
            rows = build_table(client, table_name, query)
            results[table_name] = rows
        except Exception as exc:
            log.error(f"Failed to build gold.{table_name}: {exc}")
            errors.append(table_name)

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"Gold build complete in {elapsed:.1f}s | success={len(results)} error={len(errors)}")

    if errors:
        log.error(f"Tables with errors: {errors}")
        sys.exit(1)


if __name__ == "__main__":
    main()
