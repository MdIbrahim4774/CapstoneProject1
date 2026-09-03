"""
DE7 - Pipeline Status Product

Writes the machine-readable status record consumed by
downstream components such as API6, C3, C12 and C14.

The status record is deliberately stored as a file rather
than only appearing in an Airflow log.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


logger = logging.getLogger(
    "de7_pipeline_status"
)


def utc_now_iso() -> str:
    """
    Return the current UTC timestamp in ISO-8601 format.
    """

    return datetime.now(
        timezone.utc
    ).isoformat()


def write_pipeline_status(
    status_record: dict[str, Any],
    output_path: str,
) -> Path:
    """
    Write the pipeline status record atomically.

    The file is first written to a temporary file and then
    replaced so downstream consumers do not observe a
    partially-written JSON document.

    Parameters
    ----------
    status_record:
        Dictionary containing the complete pipeline status.

    output_path:
        Destination JSON file.

    Returns
    -------
    Path
        Path to the written status file.
    """

    path = Path(
        output_path
    )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    record = dict(
        status_record
    )

    record.setdefault(
        "record_type",
        "pipeline_status",
    )

    record.setdefault(
        "schema_version",
        "1.0",
    )

    record.setdefault(
        "recorded_at",
        utc_now_iso(),
    )

    temporary_path = path.with_suffix(
        path.suffix + ".tmp"
    )

    with open(
        temporary_path,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            record,
            file,
            indent=2,
            default=str,
        )

        file.write(
            "\n"
        )

    temporary_path.replace(
        path
    )

    logger.info(
        "Pipeline status written: %s",
        path,
    )

    return path


def read_pipeline_status(
    input_path: str,
) -> dict[str, Any]:
    """
    Read a previously-created pipeline status record.
    """

    path = Path(
        input_path
    )

    if not path.exists():
        raise FileNotFoundError(
            f"Pipeline status file does not exist: {path}"
        )

    with open(
        path,
        "r",
        encoding="utf-8",
    ) as file:

        record = json.load(
            file
        )

    return record


def is_pipeline_successful(
    input_path: str,
) -> bool:
    """
    Return True when the status record reports SUCCESS.
    """

    record = read_pipeline_status(
        input_path
    )

    return (
        record.get("status")
        == "SUCCESS"
    )


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Read a DE7 pipeline status record."
        )
    )

    parser.add_argument(
        "path",
        help="Path to pipeline_status.json",
    )

    args = parser.parse_args()

    record = read_pipeline_status(
        args.path
    )

    print(
        json.dumps(
            record,
            indent=2,
        )
    )