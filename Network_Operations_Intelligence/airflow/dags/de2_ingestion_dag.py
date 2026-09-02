from datetime import datetime
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.DE2.ingestion import (
    configure_logging,
    create_directories,
    detect_files,
    process_file,
    write_metadata,
)


# ============================================================
# DAG CONFIGURATION
# ============================================================

default_args = {
    "owner": "network-operations",
    "retries": 0,
}


with DAG(
    dag_id="de2_landing_to_raw_ingestion",
    default_args=default_args,
    description="DE2 daily telecom landing-to-raw ingestion",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=[
        "network-operations",
        "data-engineering",
        "de2",
        "ingestion",
    ],
) as dag:

    # --------------------------------------------------------
    # DETECT
    # --------------------------------------------------------

    def detect_task(**context):

        configure_logging()
        create_directories()

        files = detect_files()

        file_paths = [
            str(file)
            for file in files
        ]

        context["ti"].xcom_push(
            key="detected_files",
            value=file_paths,
        )

    detect = PythonOperator(
        task_id="detect_files",
        python_callable=detect_task,
    )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    def validate_task(**context):

        from pathlib import Path

        detected_files = context["ti"].xcom_pull(
            task_ids="detect_files",
            key="detected_files",
        )

        results = []

        for file_string in detected_files or []:

            file_path = Path(file_string)

            try:
                import pandas as pd

                df = pd.read_csv(file_path)

                from src.DE2.ingestion import (
                    validate_schema,
                    validate_minimum_quality,
                )

                schema_valid, schema_reason = validate_schema(df)

                if not schema_valid:

                    results.append({
                        "file": str(file_path),
                        "valid": False,
                        "reason": schema_reason,
                    })
                    continue

                quality_valid, quality_reason = (
                    validate_minimum_quality(df)
                )

                results.append({
                    "file": str(file_path),
                    "valid": quality_valid,
                    "reason": quality_reason,
                })

            except Exception as exc:

                results.append({
                    "file": str(file_path),
                    "valid": False,
                    "reason": f"READ_ERROR: {exc}",
                })

        context["ti"].xcom_push(
            key="validation_results",
            value=results,
        )

    validate = PythonOperator(
        task_id="validate_files",
        python_callable=validate_task,
    )

    # --------------------------------------------------------
    # ROUTE
    # --------------------------------------------------------

    def route_task(**context):

        from pathlib import Path

        validation_results = context["ti"].xcom_pull(
            task_ids="validate_files",
            key="validation_results",
        )

        for result in validation_results or []:

            file_path = Path(result["file"])

            process_file(file_path)

    route = PythonOperator(
        task_id="route_files",
        python_callable=route_task,
    )

    # --------------------------------------------------------
    # LOG
    # --------------------------------------------------------

    def log_task(**context):

        configure_logging()

        validation_results = context["ti"].xcom_pull(
            task_ids="validate_files",
            key="validation_results",
        )

        for result in validation_results or []:

            status = (
                "VALID"
                if result["valid"]
                else "REJECTED"
            )

            from pathlib import Path
            import pandas as pd

            file_path = Path(result["file"])

            try:
                row_count = len(
                    pd.read_csv(file_path)
                )
            except Exception:
                row_count = 0

            write_metadata(
                filename=file_path.name,
                status=status,
                row_count=row_count,
                reason=(
                    "Schema and minimum quality checks passed"
                    if result["valid"]
                    else result["reason"]
                ),
            )

    log = PythonOperator(
        task_id="log_ingestion",
        python_callable=log_task,
    )

    # --------------------------------------------------------
    # DEPENDENCY
    # --------------------------------------------------------

    detect >> validate >> route >> log