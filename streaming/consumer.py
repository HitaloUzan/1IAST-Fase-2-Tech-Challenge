import sys
import json
import argparse
import logging
from datetime import datetime, timezone
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1, bigquery
import google.auth

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, ".")
from config import GCP_PROJECT_ID, BQ_DATASET_BRONZE, PUBSUB_SUBSCRIPTION_ID

TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.streaming_eventos"

SCHEMA = [
    bigquery.SchemaField("tipo",                 "STRING"),
    bigquery.SchemaField("timestamp",            "TIMESTAMP"),
    bigquery.SchemaField("ano",                  "INTEGER"),
    bigquery.SchemaField("sigla_uf",             "STRING"),
    bigquery.SchemaField("serie",                "STRING"),
    bigquery.SchemaField("rede",                 "STRING"),
    bigquery.SchemaField("source",               "STRING"),
    bigquery.SchemaField("taxa_alfabetizacao",   "FLOAT"),
    bigquery.SchemaField("meta_2030",            "FLOAT"),
    bigquery.SchemaField("pubsub_message_id",    "STRING"),
    bigquery.SchemaField("ingestao_ts",          "TIMESTAMP"),
]


def ensure_table(bq_client: bigquery.Client) -> None:
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}")
    dataset_ref.location = "US"
    bq_client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = bigquery.Table(TABLE_ID, schema=SCHEMA)
    bq_client.create_table(table_ref, exists_ok=True)


def parse_message(message: pubsub_v1.subscriber.message.Message) -> dict:
    data = json.loads(message.data.decode("utf-8"))
    return {
        "tipo":               data.get("tipo"),
        "timestamp":          data.get("timestamp"),
        "ano":                data.get("ano"),
        "sigla_uf":           data.get("sigla_uf"),
        "serie":              data.get("serie"),
        "rede":               data.get("rede"),
        "source":             data.get("source"),
        "taxa_alfabetizacao": data.get("taxa_alfabetizacao"),
        "meta_2030":          data.get("meta_2030"),
        "pubsub_message_id":  message.message_id,
        "ingestao_ts":        datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-mensagens", type=int, default=10, help="Max messages to consume")
    parser.add_argument("--timeout",       type=int, default=30,  help="Timeout in seconds")
    args = parser.parse_args()

    credentials, _ = google.auth.default()
    bq_client  = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
    sub_client = pubsub_v1.SubscriberClient(credentials=credentials)

    ensure_table(bq_client)

    subscription_path = sub_client.subscription_path(GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION_ID)
    log.info(f"Listening on {subscription_path} (max={args.max_mensagens}, timeout={args.timeout}s)")

    received = []

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        try:
            row = parse_message(message)
            received.append(row)
            message.ack()
            log.info(f"Received [{len(received)}] tipo={row['tipo']} uf={row['sigla_uf']}")
        except Exception as exc:
            log.error(f"Error processing message: {exc}")
            message.nack()

    streaming_pull = sub_client.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull.result(timeout=args.timeout)
    except TimeoutError:
        streaming_pull.cancel()
        streaming_pull.result()

    if received:
        errors = bq_client.insert_rows_json(TABLE_ID, received)
        if errors:
            log.error(f"BigQuery insert errors: {errors}")
            sys.exit(1)
        log.info(f"Inserted {len(received)} rows into bronze.streaming_eventos")
    else:
        log.info("No messages received")


if __name__ == "__main__":
    main()
