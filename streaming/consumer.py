import sys
import json
import argparse
import logging
import threading
from datetime import datetime, timezone

from google.cloud import pubsub_v1, bigquery
from google.api_core.exceptions import NotFound
import google.auth

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, ".")
from config import GCP_PROJECT_ID, BQ_DATASET_BRONZE, PUBSUB_TOPIC_ID, PUBSUB_SUBSCRIPTION_ID

TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.streaming_eventos"

SCHEMA = [
    bigquery.SchemaField("tipo",                   "STRING"),
    bigquery.SchemaField("timestamp",              "TIMESTAMP"),
    bigquery.SchemaField("ano",                    "INTEGER"),
    bigquery.SchemaField("sigla_uf",               "STRING"),
    bigquery.SchemaField("serie",                  "STRING"),
    bigquery.SchemaField("rede",                   "STRING"),
    bigquery.SchemaField("source",                 "STRING"),
    bigquery.SchemaField("taxa_alfabetizacao",     "FLOAT"),
    bigquery.SchemaField("media_portugues",        "FLOAT"),
    bigquery.SchemaField("meta_2030",              "FLOAT"),
    bigquery.SchemaField("percentual_atingido",    "FLOAT"),
    bigquery.SchemaField("taxa_anterior",          "FLOAT"),
    bigquery.SchemaField("taxa_revisada",          "FLOAT"),
    bigquery.SchemaField("motivo_revisao",         "STRING"),
    bigquery.SchemaField("pubsub_message_id",      "STRING"),
    bigquery.SchemaField("ingestao_ts",            "TIMESTAMP"),
]


def ensure_table(bq_client: bigquery.Client) -> None:
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}")
    dataset_ref.location = "US"
    bq_client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = bigquery.Table(TABLE_ID, schema=SCHEMA)
    bq_client.create_table(table_ref, exists_ok=True)


def ensure_subscription(credentials, subscription_path: str) -> None:
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)
    try:
        publisher.get_topic(topic=topic_path)
    except NotFound:
        publisher.create_topic(name=topic_path)
        log.info(f"Created topic {topic_path}")

    sub_client = pubsub_v1.SubscriberClient(credentials=credentials)
    with sub_client:
        try:
            sub_client.get_subscription(subscription=subscription_path)
        except NotFound:
            sub_client.create_subscription(name=subscription_path, topic=topic_path)
            log.info(f"Created subscription {subscription_path}")


def parse_message(message: pubsub_v1.subscriber.message.Message) -> dict:
    data = json.loads(message.data.decode("utf-8"))
    return {
        "tipo":                data.get("tipo"),
        "timestamp":           data.get("timestamp"),
        "ano":                 data.get("ano"),
        "sigla_uf":            data.get("sigla_uf"),
        "serie":               data.get("serie"),
        "rede":                data.get("rede"),
        "source":              data.get("source"),
        "taxa_alfabetizacao":  data.get("taxa_alfabetizacao"),
        "media_portugues":     data.get("media_portugues"),
        "meta_2030":           data.get("meta_2030"),
        "percentual_atingido": data.get("percentual_atingido"),
        "taxa_anterior":       data.get("taxa_anterior"),
        "taxa_revisada":       data.get("taxa_revisada"),
        "motivo_revisao":      data.get("motivo_revisao"),
        "pubsub_message_id":   message.message_id,
        "ingestao_ts":         datetime.now(timezone.utc).isoformat(),
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
    ensure_subscription(credentials, subscription_path)
    log.info(f"Listening on {subscription_path} (max={args.max_mensagens}, timeout={args.timeout}s)")

    received = []
    done = threading.Event()

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        try:
            row = parse_message(message)
            received.append(row)
            message.ack()
            log.info(f"Received [{len(received)}] tipo={row['tipo']} uf={row['sigla_uf']}")
            if len(received) >= args.max_mensagens:
                done.set()
        except Exception as exc:
            log.error(f"Error processing message: {exc}")
            message.nack()

    streaming_pull = sub_client.subscribe(subscription_path, callback=callback)

    done.wait(timeout=args.timeout)
    streaming_pull.cancel()
    streaming_pull.result()

    if received:
        # Micro-batch load job instead of insert_rows_json: streaming inserts
        # are billed (and blocked on the free tier), load jobs are free.
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = bq_client.load_table_from_json(received, TABLE_ID, job_config=job_config)
        job.result()
        log.info(f"Loaded {len(received)} rows into bronze.streaming_eventos")
    else:
        log.info("No messages received")


if __name__ == "__main__":
    main()
