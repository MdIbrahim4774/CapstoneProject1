import csv
import logging
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd


# ============================================================
# PATH CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

LANDING_DIR = PROJECT_ROOT / "data" / "landing"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
REFERENCE_DIR = PROJECT_ROOT / "data" / "reference"
LOG_DIR = PROJECT_ROOT / "logs"

METADATA_FILE = LOG_DIR / "ingestion_metadata.csv"
LOG_FILE = LOG_DIR / "ingestion.log"


# ============================================================
# EXPECTED DAILY ACTIVITY SCHEMA
# ============================================================

REQUIRED_COLUMNS = {
    "datetime",
    "CellID",
    "countrycode",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
}


# ============================================================
# LOGGING
# ============================================================

def configure_logging():
    """
    Configure file and console logging.
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )


logger = logging.getLogger(__name__)


# ============================================================
# DIRECTORY SETUP
# ============================================================

def create_directories():
    """
    Create all DE2 data zones and log directory.
    """

    for directory in [
        LANDING_DIR,
        RAW_DIR,
        REJECTED_DIR,
        REFERENCE_DIR,
        LOG_DIR,
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    logger.info("Required directories verified.")


# ============================================================
# 1. DETECT FILES
# ============================================================

def detect_files():
    """
    Detect daily telecom activity CSV files in the landing zone.

    Only files matching:

        sms-call-internet-mi-*.csv

    are considered ingestion candidates.

    Reference data such as milano-grid.geojson is ignored.
    """

    pattern = "sms-call-internet-mi-2013-11-01.csv"

    files = sorted(LANDING_DIR.glob(pattern))

    logger.info(
        "Detected %d daily activity file(s) in landing zone.",
        len(files),
    )

    for file in files:
        logger.info("Detected candidate: %s", file.name)

    return files


# ============================================================
# READ CSV
# ============================================================

def read_csv(file_path):
    """
    Read a CSV safely and return a pandas DataFrame.
    """

    return pd.read_csv(file_path)


# ============================================================
# 2. SCHEMA VALIDATION
# ============================================================

def validate_schema(df):
    """
    Validate that all required daily activity columns exist.

    Returns:

        (True, "")
        or
        (False, reason)
    """

    actual_columns = set(df.columns)

    missing_columns = REQUIRED_COLUMNS - actual_columns

    if missing_columns:
        reason = (
            "SCHEMA_ERROR: missing required column(s): "
            + ", ".join(sorted(missing_columns))
        )

        return False, reason

    return True, ""


# ============================================================
# 3. MINIMUM QUALITY VALIDATION
# ============================================================

def validate_minimum_quality(df):
    """
    Perform minimum data quality checks.

    Rules:
        - file must contain at least one row
        - datetime must be parseable
        - CellID cannot be null
        - activity fields may be empty
        - non-empty activity values must be numeric
        - negative activity values are rejected
    """

    if df.empty:
        return False, "QUALITY_ERROR: file contains zero rows"

    # --------------------------------------------------------
    # Timestamp validation
    # --------------------------------------------------------

    parsed_datetime = pd.to_datetime(
        df["datetime"],
        errors="coerce"
    )

    malformed_timestamp_count = parsed_datetime.isna().sum()

    if malformed_timestamp_count > 0:
        return (
            False,
            f"QUALITY_ERROR: {malformed_timestamp_count} malformed timestamp(s)"
        )

    # --------------------------------------------------------
    # CellID validation
    # --------------------------------------------------------

    null_cell_ids = df["CellID"].isna().sum()

    if null_cell_ids > 0:
        return (
            False,
            f"QUALITY_ERROR: {null_cell_ids} null CellID value(s)"
        )

    # --------------------------------------------------------
    # Activity validation
    # --------------------------------------------------------

    activity_columns = [
        "smsin",
        "smsout",
        "callin",
        "callout",
        "internet",
    ]

    for column in activity_columns:

        # Convert to numeric.
        #
        # Empty strings / NaN become NaN, which is allowed.
        numeric_values = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        # Identify values that were non-empty but could not
        # be converted to numeric.
        original_values = df[column].astype("string").str.strip()

        non_empty_mask = (
            original_values.notna()
            & original_values.ne("")
        )

        invalid_numeric_mask = (
            non_empty_mask
            & numeric_values.isna()
        )

        invalid_numeric_count = invalid_numeric_mask.sum()

        if invalid_numeric_count > 0:
            return (
                False,
                f"QUALITY_ERROR: non-numeric value(s) in {column}"
            )

        # ----------------------------------------------------
        # Negative value validation
        # ----------------------------------------------------

        negative_count = (
            numeric_values.dropna() < 0
        ).sum()

        if negative_count > 0:
            return (
                False,
                f"QUALITY_ERROR: {negative_count} negative value(s) in {column}"
            )

    return True, ""

# ============================================================
# METADATA LOGGING
# ============================================================

def write_metadata(
    filename,
    status,
    row_count,
    reason,
):
    """
    Append one ingestion metadata record.
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    record = {
        "filename": filename,
        "status": status,
        "row_count": row_count,
        "reason": reason,
        "processed_at": datetime.now().isoformat(),
    }

    file_exists = METADATA_FILE.exists()

    with open(
        METADATA_FILE,
        "a",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "filename",
                "status",
                "row_count",
                "reason",
                "processed_at",
            ],
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(record)

    logger.info(
        "Metadata written: %s | %s",
        filename,
        status,
    )


# ============================================================
# 4. ROUTE FILE
# ============================================================

def route_file(
    file_path,
    validation_status,
    reason="",
):
    """
    Route a validated file to raw or rejected.

    VALID:
        data/landing -> data/raw

    INVALID:
        data/landing -> data/rejected
    """

    try:
        df = read_csv(file_path)
        row_count = len(df)

    except Exception as exc:

        row_count = 0

        validation_status = False

        reason = f"READ_ERROR: {exc}"

    # --------------------------------------------------------
    # VALID FILE
    # --------------------------------------------------------

    if validation_status:

        destination = RAW_DIR / file_path.name

        shutil.copy2(
            file_path,
            destination,
        )

        logger.info(
            "VALID file routed to raw: %s",
            destination,
        )

        write_metadata(
            filename=file_path.name,
            status="VALID",
            row_count=row_count,
            reason="Schema and minimum quality checks passed",
        )

        return destination

    # --------------------------------------------------------
    # INVALID FILE
    # --------------------------------------------------------

    destination = REJECTED_DIR / file_path.name

    shutil.copy2(
        file_path,
        destination,
    )

    logger.warning(
        "INVALID file routed to rejected: %s | %s",
        destination,
        reason,
    )

    write_metadata(
        filename=file_path.name,
        status="REJECTED",
        row_count=row_count,
        reason=reason,
    )

    return destination


# ============================================================
# PROCESS ONE FILE
# ============================================================

def process_file(file_path):
    """
    Validate and route one landing file.
    """

    logger.info(
        "Processing landing file: %s",
        file_path.name,
    )

    # --------------------------------------------------------
    # Read
    # --------------------------------------------------------

    try:
        df = read_csv(file_path)

    except Exception as exc:

        return route_file(
            file_path,
            False,
            f"READ_ERROR: {exc}",
        )

    # --------------------------------------------------------
    # Schema validation
    # --------------------------------------------------------

    schema_valid, schema_reason = validate_schema(df)

    if not schema_valid:

        return route_file(
            file_path,
            False,
            schema_reason,
        )

    # --------------------------------------------------------
    # Quality validation
    # --------------------------------------------------------

    quality_valid, quality_reason = validate_minimum_quality(df)

    if not quality_valid:

        return route_file(
            file_path,
            False,
            quality_reason,
        )

    # --------------------------------------------------------
    # Everything passed
    # --------------------------------------------------------

    return route_file(
        file_path,
        True,
        "Schema and quality validation passed",
    )


# ============================================================
# RUN COMPLETE INGESTION
# ============================================================

def run_ingestion():
    """
    Complete DE2 ingestion workflow:

        detect
          ↓
        validate
          ↓
        route
          ↓
        log
    """

    configure_logging()
    create_directories()

    logger.info("==============================================")
    logger.info("Starting DE2 landing-to-raw ingestion")
    logger.info("==============================================")

    files = detect_files()

    if not files:
        logger.info("No daily activity files found.")
        return

    for file_path in files:
        process_file(file_path)

    logger.info("==============================================")
    logger.info("DE2 ingestion completed")
    logger.info("==============================================")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    run_ingestion()