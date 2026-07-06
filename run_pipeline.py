import sys
import subprocess
import logging
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BATCH_STEPS = [
    ("Bronze Ingestion", ["python", "ingestion/batch/ingest_bronze.py"]),
]

REMAINING_STEPS = [
    ("Silver Transforms",  ["python", "silver/transform_silver.py"]),
    ("Gold Build",         ["python", "gold/build_gold.py"]),
    ("Quality Validation", ["python", "quality/validate.py"]),
]


def run_step(name: str, cmd: list) -> bool:
    log.info(f"--- START: {name} ---")
    t0 = datetime.now()
    result = subprocess.run(cmd, capture_output=False)
    elapsed = (datetime.now() - t0).total_seconds()

    if result.returncode == 0:
        log.info(f"--- OK: {name} ({elapsed:.1f}s) ---")
        return True
    log.error(f"--- FAILED: {name} (exit={result.returncode}, {elapsed:.1f}s) ---")
    return False


def run_streaming(eventos: int = 20, max_mensagens: int = 20, timeout: int = 60) -> bool:
    name = "Streaming Simulation"
    log.info(f"--- START: {name} ---")
    t0 = datetime.now()

    consumer = subprocess.Popen([
        "python", "streaming/consumer.py",
        "--max-mensagens", str(max_mensagens), "--timeout", str(timeout),
    ])
    time.sleep(3)  # give the subscription time to attach before events are published

    producer = subprocess.run([
        "python", "streaming/producer.py",
        "--eventos", str(eventos), "--intervalo", "1.0",
    ])
    consumer.wait()
    elapsed = (datetime.now() - t0).total_seconds()

    if producer.returncode == 0 and consumer.returncode == 0:
        log.info(f"--- OK: {name} ({elapsed:.1f}s) ---")
        return True
    log.error(f"--- FAILED: {name} (producer={producer.returncode}, consumer={consumer.returncode}, {elapsed:.1f}s) ---")
    return False


def main() -> None:
    skip_quality = "--skip-quality" in sys.argv
    skip_streaming = "--skip-streaming" in sys.argv

    pipeline_start = datetime.now()
    failed = []

    for name, cmd in BATCH_STEPS:
        if not run_step(name, cmd):
            failed.append(name)
            break

    if not failed and not skip_streaming:
        if not run_streaming():
            failed.append("Streaming Simulation")

    if not failed:
        for name, cmd in REMAINING_STEPS:
            if skip_quality and name == "Quality Validation":
                log.info(f"Skipping {name}")
                continue
            if not run_step(name, cmd):
                failed.append(name)
                break

    elapsed = (datetime.now() - pipeline_start).total_seconds()

    if failed:
        log.error(f"Pipeline finished with errors in {elapsed:.1f}s — failed steps: {failed}")
        sys.exit(1)
    else:
        log.info(f"Pipeline completed successfully in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
