import sys
import argparse
import logging
from google.cloud import bigquery
import google.auth

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, ".")
from config import GCP_PROJECT_ID

VALID_UFS = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}

BRONZE_TABLES = [
    "alfabetizacao_uf",
    "meta_brasil",
    "meta_uf",
    "meta_municipio",
    "alfabetizacao_municipio",
    "alunos",
]

SILVER_TABLES = [
    "alfabetizacao_uf_clean",
    "metas_consolidadas",
    "alfabetizacao_municipio_clean",
    "alunos_clean",
]

GOLD_TABLES = [
    "indicador_por_uf_ano",
    "evolucao_temporal_brasil",
    "ranking_estados",
    "perfil_desempenho_uf",
    "painel_municipios",
]


def query_scalar(client: bigquery.Client, sql: str):
    rows = list(client.query(sql).result())
    return rows[0][0] if rows else None


def check(condition: bool, msg: str, errors: list) -> None:
    if condition:
        log.info(f"  PASS  {msg}")
    else:
        log.error(f"  FAIL  {msg}")
        errors.append(msg)


def table_exists_and_has_rows(client: bigquery.Client, dataset: str, table: str) -> tuple[bool, int]:
    try:
        t = client.get_table(f"{GCP_PROJECT_ID}.{dataset}.{table}")
        return True, t.num_rows or 0
    except Exception:
        return False, 0


def validate_bronze(client: bigquery.Client) -> list:
    log.info("=== BRONZE ===")
    errors = []

    for tbl in BRONZE_TABLES:
        exists, rows = table_exists_and_has_rows(client, "bronze", tbl)
        check(exists and rows > 0, f"bronze.{tbl} exists and has {rows:,} rows", errors)

    exists, _ = table_exists_and_has_rows(client, "bronze", "alfabetizacao_uf")
    if exists:
        dupes = query_scalar(client, f"""
            SELECT COUNT(*) FROM (
                SELECT ano, sigla_uf, serie, rede, COUNT(*) AS cnt
                FROM `{GCP_PROJECT_ID}.bronze.alfabetizacao_uf`
                GROUP BY ano, sigla_uf, serie, rede
                HAVING cnt > 1
            )
        """)
        check(dupes == 0, f"bronze.alfabetizacao_uf has no duplicate keys (found {dupes})", errors)

        bad_taxa = query_scalar(client, f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.bronze.alfabetizacao_uf`
            WHERE taxa_alfabetizacao IS NOT NULL
              AND (taxa_alfabetizacao < 0 OR taxa_alfabetizacao > 100)
        """)
        check(bad_taxa == 0, f"bronze.alfabetizacao_uf taxa in [0,100] (bad rows: {bad_taxa})", errors)

        invalid_ufs = query_scalar(client, f"""
            SELECT COUNT(DISTINCT sigla_uf) FROM `{GCP_PROJECT_ID}.bronze.alfabetizacao_uf`
            WHERE sigla_uf NOT IN ({','.join(f"'{u}'" for u in VALID_UFS)})
        """)
        check(invalid_ufs == 0, f"bronze.alfabetizacao_uf all UFs valid (invalid: {invalid_ufs})", errors)

    return errors


def validate_silver(client: bigquery.Client) -> list:
    log.info("=== SILVER ===")
    errors = []

    for tbl in SILVER_TABLES:
        exists, rows = table_exists_and_has_rows(client, "silver", tbl)
        check(exists and rows > 0, f"silver.{tbl} exists and has {rows:,} rows", errors)

    _, silver_uf_rows = table_exists_and_has_rows(client, "silver", "alfabetizacao_uf_clean")
    _, bronze_uf_rows = table_exists_and_has_rows(client, "bronze", "alfabetizacao_uf")
    if silver_uf_rows and bronze_uf_rows:
        check(
            silver_uf_rows <= bronze_uf_rows,
            f"silver.alfabetizacao_uf_clean ({silver_uf_rows:,}) <= bronze ({bronze_uf_rows:,})",
            errors,
        )

    exists, _ = table_exists_and_has_rows(client, "silver", "alfabetizacao_uf_clean")
    if exists:
        nulls = query_scalar(client, f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.silver.alfabetizacao_uf_clean`
            WHERE ano IS NULL OR sigla_uf IS NULL OR taxa_alfabetizacao IS NULL
        """)
        check(nulls == 0, f"silver.alfabetizacao_uf_clean no nulls in critical cols (found {nulls})", errors)

    return errors


def validate_gold(client: bigquery.Client) -> list:
    log.info("=== GOLD ===")
    errors = []

    for tbl in GOLD_TABLES:
        exists, rows = table_exists_and_has_rows(client, "gold", tbl)
        check(exists and rows > 0, f"gold.{tbl} exists and has {rows:,} rows", errors)

    exists, _ = table_exists_and_has_rows(client, "gold", "ranking_estados")
    if exists:
        state_count = query_scalar(client, f"""
            SELECT COUNT(DISTINCT sigla_uf) FROM `{GCP_PROJECT_ID}.gold.ranking_estados`
        """)
        check(state_count >= 27, f"gold.ranking_estados covers 27 states (found {state_count})", errors)

    return errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--camada", choices=["bronze", "silver", "gold", "all"], default="all")
    args = parser.parse_args()

    credentials, _ = google.auth.default()
    client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

    all_errors = []

    if args.camada in ("bronze", "all"):
        all_errors.extend(validate_bronze(client))
    if args.camada in ("silver", "all"):
        all_errors.extend(validate_silver(client))
    if args.camada in ("gold", "all"):
        all_errors.extend(validate_gold(client))

    if all_errors:
        log.error(f"Validation FAILED — {len(all_errors)} check(s) failed")
        for e in all_errors:
            log.error(f"  - {e}")
        sys.exit(1)
    else:
        log.info(f"Validation PASSED — all checks OK")


if __name__ == "__main__":
    main()
