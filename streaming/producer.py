import sys
import json
import time
import random
import argparse
import logging
from datetime import datetime, timezone

from google.cloud import pubsub_v1
import google.auth

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, ".")
from config import GCP_PROJECT_ID, PUBSUB_TOPIC_ID

UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]
SERIES = ["2º ano EF", "3º ano EF"]
REDES  = ["Municipal", "Estadual", "Privada", "Federal"]
TIPOS  = ["medicao_desempenho", "atualizacao_meta", "revisao_indicador"]


def build_event(tipo: str) -> dict:
    base = {
        "tipo": tipo,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ano": random.choice([2021, 2022, 2023]),
        "sigla_uf": random.choice(UFS),
        "serie": random.choice(SERIES),
        "rede": random.choice(REDES),
        "source": "streaming-simulator",
    }
    if tipo == "medicao_desempenho":
        base["taxa_alfabetizacao"] = round(random.uniform(50, 99), 2)
        base["media_portugues"]    = round(random.uniform(600, 850), 1)
    elif tipo == "atualizacao_meta":
        base["meta_2030"]           = round(random.uniform(85, 100), 2)
        base["percentual_atingido"] = round(random.uniform(60, 100), 2)
    elif tipo == "revisao_indicador":
        base["taxa_anterior"]  = round(random.uniform(50, 90), 2)
        base["taxa_revisada"]  = round(random.uniform(50, 99), 2)
        base["motivo_revisao"] = random.choice(["correcao_calculo", "novos_dados", "reprocessamento"])
    return base


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eventos",   type=int,   default=10,  help="Number of events to publish")
    parser.add_argument("--intervalo", type=float, default=1.0, help="Seconds between events")
    args = parser.parse_args()

    credentials, _ = google.auth.default()
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)

    log.info(f"Publishing {args.eventos} events to {topic_path}")

    published = 0
    for i in range(args.eventos):
        tipo  = random.choice(TIPOS)
        event = build_event(tipo)
        data  = json.dumps(event, ensure_ascii=False).encode("utf-8")

        future = publisher.publish(topic_path, data=data, tipo=tipo)
        msg_id = future.result()
        published += 1
        log.info(f"[{i+1}/{args.eventos}] Published {tipo} | msg_id={msg_id}")

        if i < args.eventos - 1:
            time.sleep(args.intervalo)

    log.info(f"Done — {published} events published")


if __name__ == "__main__":
    main()
