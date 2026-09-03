from datetime import datetime
from pathlib import Path
import logging
import shutil

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"

LANDING_DIR = DATA_DIR / "landing"
RAW_DIR = DATA_DIR / "raw"
REJECTED_DIR = DATA_DIR / "rejected"
PROCESSED_DIR = DATA_DIR / "processed"
ANALYTICS_DIR = DATA_DIR / "analytics"
REFERENCE_DIR = DATA_DIR / "reference"

GEOJSON_PATH = REFERENCE_DIR / "milano-grid.geojson"

SPARK_SCRIPT = PROJECT_ROOT / "spark" / "telecom_pipeline.py"

PYTHON_EXECUTABLE = "python3"


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger(__name__)


# ============================================================
# INGESTION
# ============================================================

def ingest_valid_files():
    """
    Move validated files from landing to raw.

    DE3 assumes the files reaching this task have already
    passed DE2 validation.
    """

    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    files = list(
        LANDING_DIR.glob("sms-call-internet-mi-*.csv")
    )

    logger.info("Landing directory: %s", LANDING_DIR)
    logger.info("Found %d file(s) in landing", len(files))

    if not files:
        raise FileNotFoundError(
            f"No telecom CSV files found in {LANDING_DIR}"
        )

    moved = 0

    for source in files:

        destination = RAW_DIR / source.name

        logger.info(
            "Moving file from landing to raw: %s -> %s",
            source,
            destination,
        )

        shutil.copy2(str(source), str(destination))

        moved += 1

    logger.info(
        "Ingestion completed successfully. "
        "%d file(s) moved to raw.",
        moved,
    )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="de3_network_processing_pipeline",
    description="DE3 - Ingestion followed by Spark processing",
    start_date=datetime(2026, 9, 2),
    schedule=None,
    catchup=False,
    tags=["network", "DE3", "spark"],
) as dag:

    # --------------------------------------------------------
    # TASK 1: INGESTION
    # --------------------------------------------------------

    ingest_task = PythonOperator(
        task_id="ingest_to_raw",
        python_callable=ingest_valid_files,
    )

    # --------------------------------------------------------
    # TASK 2: SPARK PROCESSING
    # --------------------------------------------------------

    spark_task = BashOperator(
        task_id="run_spark_processing",

        bash_command=(
            'echo "========================================"\n'
            'echo "SPARK JOB START: $(date)"\n'
            'echo "========================================"\n'

            f'echo "Input: {RAW_DIR}"\n'
            f'echo "Processed output: {PROCESSED_DIR}"\n'
            f'echo "Reference: {GEOJSON_PATH}"\n'

            f'"{PYTHON_EXECUTABLE}" '
            f'"{SPARK_SCRIPT}" '
            f'--input "{RAW_DIR}" '
            f'--output "{PROCESSED_DIR}" '
            f'--reference "{GEOJSON_PATH}"\n'

            'STATUS=$?\n'

            'echo "========================================"\n'
            'echo "SPARK JOB END: $(date)"\n'
            'echo "SPARK JOB STATUS: $STATUS"\n'
            'echo "========================================"\n'

            'exit $STATUS'
        ),

        # Prevent Airflow from hiding the Spark exit code.
        do_xcom_push=False,
    )

    # --------------------------------------------------------
    # DEPENDENCY
    # --------------------------------------------------------

    ingest_task >> spark_task