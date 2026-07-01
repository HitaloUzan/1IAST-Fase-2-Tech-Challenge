import sys
import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

STEPS = [
    ("Bronze Ingestion",    ["python", "ingestion/batch/ingest_bronze.py"]),
    ("Silver Transforms",   ["python", "silver/transform_silver.py"]),
    ("Gold Build",          ["python", "gold/build_gold.py"]),
    ("Quality Validation",  ["python", "quality/validate.py"]),
]


def run_step(name: str, cmd: list) -> bool:
    log.info(f"--- START: {name} ---")
    t0 = datetime.now()
    result = subprocess.run(cmd, capture_output=False)
    elapsed = (datetime.now() - t0).total_seconds()

    if result.returncode == 0:
        log.info(f"--- OK: {name} ({elapsed:.1f}s) ---")
        return True
    else:
        log.error(f"--- FAILED: {name} (exit={result.returncode}, {elapsed:.1f}s) ---")
        return False


def main() -> None:
    skip_quality = "--skip-quality" in sys.argv

    pipeline_start = datetime.now()
    failed = []

    for name, cmd in STEPS:
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
