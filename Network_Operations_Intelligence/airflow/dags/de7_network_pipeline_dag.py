"""
DE7 - End-to-End Airflow Orchestration
Network Operations Intelligence

Pipeline:

    landing
       |
       v
    ingest
       |
       v
    validate
       |
       v
    spark_process
       |
       v
    load_warehouse
       |
       v
    quality_check
       |
       v
    notify

DE7 reuses:
    - DE2 ingestion/validation concepts
    - SP7 spark/telecom_pipeline.py
    - DE6 src/de6_load_mysql.py

The pipeline status is written by quality_check as a
machine-readable JSON file consumed by downstream services.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# ============================================================
# PROJECT PATHS
# ============================================================

# This file is:
#     <project>/dags/de7_network_pipeline_dag.py
#
# Therefore parents[1] = project root.

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"

LANDING_DIR = DATA_DIR / "landing"
RAW_DIR = DATA_DIR / "raw"
REJECTED_DIR = DATA_DIR / "rejected"
REFERENCE_DIR = DATA_DIR / "reference"

OUTPUT_DIR = DATA_DIR / "output" / "telecom_activity"

# SP7 writes the aggregated output underneath this directory.
AGGREGATED_DIR = OUTPUT_DIR / "aggregated"

STATUS_DIR = DATA_DIR / "analytics"
STATUS_FILE = STATUS_DIR / "pipeline_status.json"

LOG_DIR = PROJECT_ROOT / "logs"

REFERENCE_FILE = REFERENCE_DIR / "milano-grid.geojson"

SPARK_JOB = PROJECT_ROOT / "spark" / "telecom_pipeline.py"
MYSQL_LOADER = PROJECT_ROOT / "src" / "DE6" /"de6_load_mysql.py"


# ============================================================
# PYTHON / ENVIRONMENT
# ============================================================

PYTHON_EXECUTABLE = os.getenv(
    "NETWORK_OPS_PYTHON",
    sys.executable,
)


# ============================================================
# MYSQL CONFIGURATION
# ============================================================

MYSQL_HOST = os.getenv(
    "MYSQL_HOST",
    "172.23.64.1",
)

MYSQL_PORT = os.getenv(
    "MYSQL_PORT",
    "3306",
)

MYSQL_USER = os.getenv(
    "MYSQL_USER",
    "root",
)

MYSQL_PASSWORD = os.getenv(
    "MYSQL_PASSWORD",
    "root",
)

MYSQL_DATABASE = os.getenv(
    "MYSQL_DATABASE",
    "network_operations",
)


# ============================================================
# LOGGING
# ============================================================

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

logger = logging.getLogger(
    "de7_network_pipeline"
)

logger.setLevel(logging.INFO)


# ============================================================
# EXPECTED RAW SCHEMA
# ============================================================

REQUIRED_COLUMNS = [
    "datetime",
    "CellID",
    "countrycode",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
]


# ============================================================
# COMMON HELPERS
# ============================================================

def ensure_directories():
    """
    Ensure required pipeline directories exist.
    """

    directories = [
        LANDING_DIR,
        RAW_DIR,
        REJECTED_DIR,
        REFERENCE_DIR,
        OUTPUT_DIR,
        STATUS_DIR,
        LOG_DIR,
    ]

    for directory in directories:
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


def get_csv_files(directory: Path):
    """
    Return CSV files in a directory.
    """

    if not directory.exists():
        return []

    return sorted(
        [
            path
            for path in directory.iterdir()
            if path.is_file()
            and path.suffix.lower() == ".csv"
        ]
    )


def run_command(
    command: list[str],
    task_name: str,
):
    """
    Execute an external pipeline component.

    Raises AirflowException when the command fails.
    """

    logger.info(
        "[%s] Executing command:",
        task_name,
    )

    logger.info(
        "%s",
        " ".join(
            f'"{argument}"'
            if " " in str(argument)
            else str(argument)
            for argument in command
        ),
    )

    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        env=os.environ.copy(),
        text=True,
    )

    if result.returncode != 0:
        raise AirflowException(
            f"{task_name} failed with exit code "
            f"{result.returncode}."
        )

    logger.info(
        "[%s] Command completed successfully.",
        task_name,
    )


# ============================================================
# TASK 1 - INGEST
# ============================================================

def ingest():
    """
    Detect the held-back file in landing and move it into raw.

    Existing accumulated history already present in raw is preserved.

    This implements the operational movement:

        landing -> raw

    The validation gate happens in the next Airflow task.
    """

    ensure_directories()

    landing_files = get_csv_files(
        LANDING_DIR
    )

    logger.info(
        "Landing CSV files detected: %d",
        len(landing_files),
    )

    if not landing_files:
        raise AirflowException(
            "No incoming CSV files found in "
            f"{LANDING_DIR}."
        )

    moved_files = []

    for source in landing_files:

        destination = RAW_DIR / source.name

        if destination.exists():

            logger.warning(
                "File already exists in raw: %s",
                destination,
            )

            # Do not overwrite an existing raw file.
            # Move the duplicate to rejected.
            rejected_destination = (
                REJECTED_DIR / source.name
            )

            if rejected_destination.exists():
                rejected_destination = (
                    REJECTED_DIR
                    / f"{source.stem}_duplicate"
                    f"{source.suffix}"
                )

            shutil.copy2(
                str(source),
                str(rejected_destination),
            )

            logger.warning(
                "Duplicate incoming file moved to rejected: %s",
                rejected_destination,
            )

            continue

        shutil.copy2(
            str(source),
            str(destination),
        )

        moved_files.append(
            destination.name
        )

        logger.info(
            "Moved landing file to raw: %s",
            destination,
        )

    if not moved_files:
        raise AirflowException(
            "No new files were ingested."
        )

    logger.info(
        "Ingest completed. Files moved: %d",
        len(moved_files),
    )


# ============================================================
# TASK 2 - VALIDATE
# ============================================================

def validate():
    """
    Validate raw CSV files before Spark processing.

    Validation rules:

        1. Required columns must exist.
        2. Files must contain data rows.
        3. datetime must not be empty.
        4. CellID must not be empty.
        5. Activity measures must not be negative.

    Invalid files are moved to rejected.
    """

    ensure_directories()

    raw_files = get_csv_files(
        RAW_DIR
    )

    if not raw_files:
        raise AirflowException(
            f"No CSV files available in {RAW_DIR}."
        )

    logger.info(
        "Validating %d raw files.",
        len(raw_files),
    )

    valid_files = []
    invalid_files = []

    for file_path in raw_files:

        logger.info(
            "Validating: %s",
            file_path,
        )

        errors = []

        try:

            with open(
                file_path,
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as file:

                reader = csv.DictReader(
                    file
                )

                if reader.fieldnames is None:
                    errors.append(
                        "Missing CSV header."
                    )

                    rows = []

                else:

                    actual_columns = [
                        column.strip()
                        for column in reader.fieldnames
                    ]

                    missing_columns = [
                        column
                        for column in REQUIRED_COLUMNS
                        if column not in actual_columns
                    ]

                    if missing_columns:
                        errors.append(
                            "Missing columns: "
                            + ", ".join(
                                missing_columns
                            )
                        )

                    rows = list(reader)

                if not rows:
                    errors.append(
                        "CSV contains zero data rows."
                    )

                else:

                    activity_columns = [
                        "smsin",
                        "smsout",
                        "callin",
                        "callout",
                        "internet",
                    ]

                    for row_number, row in enumerate(
                        rows,
                        start=2,
                    ):

                        timestamp = (
                            row.get("datetime")
                            or ""
                        ).strip()

                        grid_id = (
                            row.get("CellID")
                            or ""
                        ).strip()

                        if not timestamp:
                            errors.append(
                                f"Row {row_number}: "
                                "datetime is empty."
                            )

                        if not grid_id:
                            errors.append(
                                f"Row {row_number}: "
                                "CellID is empty."
                            )

                        for column in activity_columns:

                            value = (
                                row.get(column)
                                or ""
                            ).strip()

                            if not value:
                                continue

                            try:

                                numeric_value = float(
                                    value
                                )

                                if numeric_value < 0:
                                    errors.append(
                                        f"Row {row_number}: "
                                        f"{column} is negative."
                                    )

                            except ValueError:

                                errors.append(
                                    f"Row {row_number}: "
                                    f"{column} is not numeric."
                                )

                        # Avoid generating enormous error lists.
                        if len(errors) >= 25:
                            errors.append(
                                "Validation stopped after "
                                "25 errors."
                            )
                            break

        except Exception as exc:

            errors.append(
                f"Could not read file: {exc}"
            )

        if errors:

            invalid_files.append(
                file_path.name
            )

            rejected_path = (
                REJECTED_DIR / file_path.name
            )

            if rejected_path.exists():
                rejected_path = (
                    REJECTED_DIR
                    / f"{file_path.stem}_invalid"
                    f"{file_path.suffix}"
                )

            shutil.move(
                str(file_path),
                str(rejected_path),
            )

            logger.error(
                "Validation failed for %s",
                file_path.name,
            )

            for error in errors:
                logger.error(
                    "  %s",
                    error,
                )

        else:

            valid_files.append(
                file_path.name
            )

            logger.info(
                "Validation passed: %s",
                file_path.name,
            )

    if not valid_files:

        raise AirflowException(
            "Validation failed: no valid raw files remain."
        )

    logger.info(
        "Validation complete. Valid files: %d | "
        "Invalid files: %d",
        len(valid_files),
        len(invalid_files),
    )


# ============================================================
# TASK 3 - SPARK PROCESS
# ============================================================

def spark_process():
    """
    Run the existing SP7 Spark ETL job.

    Reuses:

        spark/telecom_pipeline.py

    instead of rewriting Spark processing in the DAG.
    """

    ensure_directories()

    if not SPARK_JOB.exists():
        raise AirflowException(
            f"SP7 job not found: {SPARK_JOB}"
        )

    if not RAW_DIR.exists():
        raise AirflowException(
            f"Raw directory does not exist: {RAW_DIR}"
        )

    command = [
        PYTHON_EXECUTABLE,
        str(SPARK_JOB),
        "--input",
        str(RAW_DIR),
        "--output",
        str(OUTPUT_DIR),
        "--reference",
        str(REFERENCE_FILE),
        "--log-dir",
        str(LOG_DIR),
    ]

    run_command(
        command,
        "spark_process",
    )

    if not AGGREGATED_DIR.exists():
        raise AirflowException(
            "Spark completed but aggregated output "
            f"was not found: {AGGREGATED_DIR}"
        )

    parquet_files = list(
        AGGREGATED_DIR.rglob("*.parquet")
    )

    if not parquet_files:
        raise AirflowException(
            "Spark completed but no Parquet files were "
            f"found in {AGGREGATED_DIR}."
        )

    logger.info(
        "Spark output verified. Parquet files: %d",
        len(parquet_files),
    )


# ============================================================
# TASK 4 - LOAD WAREHOUSE
# ============================================================

def load_warehouse():
    """
    Run the existing DE6 MySQL warehouse loader.

    Reuses:

        src/de6_load_mysql.py
    """

    if not MYSQL_LOADER.exists():
        raise AirflowException(
            f"DE6 loader not found: {MYSQL_LOADER}"
        )

    if not AGGREGATED_DIR.exists():
        raise AirflowException(
            f"Aggregated output not found: {AGGREGATED_DIR}"
        )

    command = [
        PYTHON_EXECUTABLE,
        str(MYSQL_LOADER),
        "--input",
        str(AGGREGATED_DIR),
        "--reference",
        str(REFERENCE_FILE),
        "--host",
        MYSQL_HOST,
        "--port",
        str(MYSQL_PORT),
        "--user",
        MYSQL_USER,
        "--password",
        MYSQL_PASSWORD,
        "--database",
        MYSQL_DATABASE,
        "--log-dir",
        str(LOG_DIR),
    ]

    run_command(
        command,
        "load_warehouse",
    )


# ============================================================
# TASK 5 - QUALITY CHECK
# ============================================================

def quality_check(**context):
    """
    Validate the complete pipeline and write the
    machine-readable status record.

    IMPORTANT:

    This task uses TriggerRule.ALL_DONE so it executes even
    when an upstream task fails.

    It writes pipeline_status.json first and then raises an
    exception if the pipeline was unsuccessful.

    Therefore:

        status file -> always available
        Airflow DAG -> correctly marked FAILED
    """

    from src.DE7.pipeline_status import (
        write_pipeline_status,
    )

    from src.DE7.quality_check import (
        run_quality_checks,
    )

    dag_run = context["dag_run"]

    task_instances = dag_run.get_task_instances()

    task_states = {
        task_instance.task_id: task_instance.state
        for task_instance in task_instances
    }

    logger.info(
        "Task states: %s",
        task_states,
    )

    # Do not count this quality_check task itself as an
    # upstream processing failure.
    pipeline_tasks = [
        "ingest",
        "validate",
        "spark_process",
        "load_warehouse",
    ]

    failed_tasks = [
        task_id
        for task_id in pipeline_tasks
        if task_states.get(task_id) not in {"success"}
    ]

    quality_results = {
        "raw_files": 0,
        "processed_parquet_files": 0,
        "warehouse_fact_rows": None,
        "checks": [],
    }

    quality_errors = []

    try:

        results = run_quality_checks(
            raw_dir=str(RAW_DIR),
            processed_dir=str(AGGREGATED_DIR),
            reference_file=str(REFERENCE_FILE),
            mysql_host=MYSQL_HOST,
            mysql_port=int(MYSQL_PORT),
            mysql_user=MYSQL_USER,
            mysql_password=MYSQL_PASSWORD,
            mysql_database=MYSQL_DATABASE,
        )

        quality_results.update(
            results
        )

    except Exception as exc:

        quality_errors.append(
            str(exc)
        )

        logger.exception(
            "Quality checks raised an exception."
        )

    pipeline_success = (
        not failed_tasks
        and not quality_errors
        and all(
            check.get("status") == "PASS"
            for check in quality_results.get(
                "checks",
                [],
            )
        )
    )

    status = (
        "SUCCESS"
        if pipeline_success
        else "FAILED"
    )

    started_at = (
        dag_run.start_date
        if dag_run.start_date
        else datetime.now(
            timezone.utc
        )
    )

    completed_at = datetime.now(
        timezone.utc
    )

    status_record = {
        "pipeline": "network_operations_intelligence",
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "status": status,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "failed_tasks": failed_tasks,
        "task_states": task_states,
        "quality": quality_results,
        "quality_errors": quality_errors,
    }

    write_pipeline_status(
        status_record,
        str(STATUS_FILE),
    )

    logger.info(
        "Machine-readable pipeline status written to: %s",
        STATUS_FILE,
    )

    if not pipeline_success:

        raise AirflowException(
            "DE7 quality check FAILED. "
            f"Failed tasks: {failed_tasks}. "
            f"Quality errors: {quality_errors}"
        )

    logger.info(
        "DE7 quality check PASSED."
    )


# ============================================================
# TASK 6 - NOTIFY
# ============================================================

def notify(**context):
    """
    Lightweight success/failure notification.

    This deliberately uses logging only so DE7 does not
    require an external notification service.
    """

    dag_run = context["dag_run"]

    status = (
        dag_run.get_task_instance(
            task_id="quality_check"
        ).state
    )

    if status == "success":

        logger.info(
            "=" * 70
        )

        logger.info(
            "DE7 PIPELINE SUCCESS"
        )

        logger.info(
            "DAG: %s",
            dag_run.dag_id,
        )

        logger.info(
            "Run ID: %s",
            dag_run.run_id,
        )

        logger.info(
            "Pipeline status: SUCCESS"
        )

        logger.info(
            "Status record: %s",
            STATUS_FILE,
        )

        logger.info(
            "=" * 70
        )

    else:

        logger.error(
            "=" * 70
        )

        logger.error(
            "DE7 PIPELINE FAILED"
        )

        logger.error(
            "DAG: %s",
            dag_run.dag_id,
        )

        logger.error(
            "Run ID: %s",
            dag_run.run_id,
        )

        logger.error(
            "Pipeline status: FAILED"
        )

        logger.error(
            "Status record: %s",
            STATUS_FILE,
        )

        logger.error(
            "Troubleshooting map:"
        )

        logger.error(
            "  ingest         -> data/landing and DE2 ingestion"
        )

        logger.error(
            "  validate       -> data/rejected and raw CSV schema"
        )

        logger.error(
            "  spark_process  -> logs/telecom_pipeline.log"
        )

        logger.error(
            "  load_warehouse -> logs/de6_mysql_loader.log"
        )

        logger.error(
            "  quality_check  -> data/analytics/pipeline_status.json"
        )

        logger.error(
            "=" * 70
        )


# ============================================================
# AIRFLOW DEFAULT ARGUMENTS
# ============================================================

default_args = {
    "owner": "network-operations",
    "depends_on_past": False,
    "retries": 0,
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="de7_network_pipeline",
    description=(
        "End-to-end Network Operations Intelligence "
        "pipeline: ingest -> validate -> Spark -> MySQL "
        "-> quality -> notify."
    ),
    default_args=default_args,
    start_date=datetime(
        2026,
        9,
        1,
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "network-operations",
        "DE7",
        "data-engineering",
    ],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    spark_process_task = PythonOperator(
        task_id="spark_process",
        python_callable=spark_process,
    )

    load_warehouse_task = PythonOperator(
        task_id="load_warehouse",
        python_callable=load_warehouse,
    )

    quality_check_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    notify_task = PythonOperator(
        task_id="notify",
        python_callable=notify,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        ingest_task
        >> validate_task
        >> spark_process_task
        >> load_warehouse_task
        >> quality_check_task
        >> notify_task
    )