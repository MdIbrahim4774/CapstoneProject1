import logging
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# UsageProcessor
# ---------------------------------------------------------

class UsageProcessor:
    """
    Reusable processor for daily telecom usage CSV files.

    Expected canonical columns:
        timestamp
        grid_id
        sms_in
        sms_out
        call_in
        call_out
        internet

    Optional:
        country_code
    """

    REQUIRED_COLUMNS = {
        "timestamp",
        "grid_id",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet",
    }

    ACTIVITY_COLUMNS = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet",
    ]

    def __init__(self, source):
        """
        source can be:
            - CSV file path
            - pandas DataFrame
        """

        self.source = source
        self.df = None

        self.hourly_summary = None
        self.daily_summary = None
        self.grid_summary = None

    # -----------------------------------------------------
    # 1. Load data
    # -----------------------------------------------------

    def load_data(self):
        """Load data from CSV or use the supplied DataFrame."""

        if isinstance(self.source, pd.DataFrame):
            self.df = self.source.copy()

        elif isinstance(self.source, (str, Path)):
            path = Path(self.source)

            if not path.exists():
                raise FileNotFoundError(
                    f"Input file does not exist: {path}"
                )

            self.df = pd.read_csv(path)

        else:
            raise TypeError(
                "source must be a pandas DataFrame or CSV file path"
            )

        logger.info(
            "Loaded %d rows and %d columns",
            len(self.df),
            len(self.df.columns),
        )

        return self.df

    # -----------------------------------------------------
    # 2. Clean data
    # -----------------------------------------------------

    def clean_data(self):
        """Validate schema and clean invalid records."""

        if self.df is None:
            raise RuntimeError(
                "Data not loaded. Call load_data() first."
            )

        # Normalize column names
        self.df = self.df.rename(
            columns={
        "datetime": "timestamp",
        "CellID": "grid_id",
        "countrycode": "country_code",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
    }
)

        missing_columns = self.REQUIRED_COLUMNS - set(self.df.columns)

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {sorted(missing_columns)}"
            )

        original_count = len(self.df)

        # -------------------------------------------------
        # Timestamp validation
        # -------------------------------------------------

        self.df["timestamp"] = pd.to_datetime(
            self.df["timestamp"],
            errors="coerce",
        )

        invalid_timestamp = self.df["timestamp"].isna()

        # -------------------------------------------------
        # grid_id validation
        # -------------------------------------------------

        self.df["grid_id"] = self.df["grid_id"].astype("string")

        invalid_grid = (
            self.df["grid_id"].isna()
            | self.df["grid_id"].str.strip().eq("")
        )

        # -------------------------------------------------
        # Activity columns
        # -------------------------------------------------

        for column in self.ACTIVITY_COLUMNS:
            self.df[column] = pd.to_numeric(
                self.df[column],
                errors="coerce",
            )

        # Negative activity values are invalid.
        negative_mask = (
            self.df[self.ACTIVITY_COLUMNS]
            .lt(0)
            .any(axis=1)
        )
        print(f"Negative activity rows: {negative_mask}")

        # -------------------------------------------------
        # Curated-layer null handling
        #
        # Rule:
        #   - invalid timestamp/grid rows are dropped
        #   - negative activity rows are dropped
        #   - activity NULLs are treated as zero
        # -------------------------------------------------

        invalid_rows = (
            invalid_grid
            | invalid_timestamp
            | negative_mask
        )

        dropped_count = invalid_rows.sum()

        self.df = self.df.loc[~invalid_rows].copy()

        # Curated rule: missing activity means no activity
        self.df[self.ACTIVITY_COLUMNS] = (
            self.df[self.ACTIVITY_COLUMNS]
            .fillna(0)
        )

        logger.info(
            "Dropped %d invalid rows",
            dropped_count,
        )

        logger.info(
            "Rows after cleaning: %d (from %d)",
            len(self.df),
            original_count,
        )

        return self.df

    # -----------------------------------------------------
    # 3. Derive time features
    # -----------------------------------------------------

    def derive_time_features(self):
        """Create date, hour and time-related features."""

        if self.df is None:
            raise RuntimeError("Data must be loaded first.")

        if "timestamp" not in self.df.columns:
            raise ValueError("timestamp column is required.")

        self.df["date"] = self.df["timestamp"].dt.date
        self.df["hour"] = self.df["timestamp"].dt.hour
        self.df["day_of_week"] = (
            self.df["timestamp"].dt.day_name()
        )

        self.df["is_weekend"] = (
            self.df["timestamp"].dt.dayofweek >= 5
        )

        return self.df

    # -----------------------------------------------------
    # 4. Aggregate to grid/hour
    # -----------------------------------------------------

    def aggregate_to_grid_time(self):
        """
        Generate exactly one record per grid/hour.
        """

        required = {
            "grid_id",
            "timestamp",
            *self.ACTIVITY_COLUMNS,
        }

        missing = required - set(self.df.columns)

        if missing:
            raise ValueError(
                f"Missing columns for aggregation: {sorted(missing)}"
            )

        self.df["hour_timestamp"] = (
            self.df["timestamp"].dt.floor("h")
        )

        self.hourly_summary = (
            self.df
            .groupby(
                ["grid_id", "hour_timestamp"],
                as_index=False,
            )[self.ACTIVITY_COLUMNS]
            .sum()
        )

        logger.info(
            "Created %d grid/hour records",
            len(self.hourly_summary),
        )

        return self.hourly_summary

    # -----------------------------------------------------
    # 5. Derive activity features
    # -----------------------------------------------------

    def derive_activity_features(self):
        """Create useful activity KPIs."""

        if self.hourly_summary is None:
            raise RuntimeError(
                "Run aggregate_to_grid_time() first."
            )

        df = self.hourly_summary

        df["total_sms"] = (
            df["sms_in"] +
            df["sms_out"]
        )

        df["total_calls"] = (
            df["call_in"] +
            df["call_out"]
        )

        df["total_activity"] = (
            df["total_sms"] +
            df["total_calls"] +
            df["internet"]
        )

        df["active"] = (
            df["total_activity"] > 0
        ).astype(int)

        df["date"] = (
            df["hour_timestamp"]
            .dt.date
        )

        df["hour"] = (
            df["hour_timestamp"]
            .dt.hour
        )

        self.hourly_summary = df

        return self.hourly_summary

    # -----------------------------------------------------
    # 6. Compute KPIs
    # -----------------------------------------------------

    def compute_kpis(self):
        """
        Produce daily and grid-level summary tables.
        """

        if self.hourly_summary is None:
            raise RuntimeError(
                "Run aggregate_to_grid_time() first."
            )

        df = self.hourly_summary

        # -----------------------------------------------
        # Daily summary
        # -----------------------------------------------

        self.daily_summary = (
            df.groupby("date", as_index=False)
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                total_internet=("internet", "sum"),
                total_activity=("total_activity", "sum"),
                active_grid_hours=("active", "sum"),
            )
        )

        # -----------------------------------------------
        # Grid summary
        # -----------------------------------------------

        self.grid_summary = (
            df.groupby("grid_id", as_index=False)
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                total_internet=("internet", "sum"),
                total_activity=("total_activity", "sum"),
                active_hours=("active", "sum"),
            )
        )

        logger.info(
            "Daily summary contains %d records",
            len(self.daily_summary),
        )

        logger.info(
            "Grid summary contains %d records",
            len(self.grid_summary),
        )

        return self.daily_summary, self.grid_summary

    # -----------------------------------------------------
    # 7. Export summary
    # -----------------------------------------------------

    def export_summary(self, output_dir="output"):
        """Export daily and grid-level summaries."""

        if self.daily_summary is None:
            raise RuntimeError(
                "Run compute_kpis() before exporting."
            )

        output_path = Path(output_dir)
        output_path.mkdir(
            parents=True,
            exist_ok=True,
        )

        daily_path = output_path / "daily_summary.csv"
        grid_path = output_path / "grid_summary.csv"
        hourly_path = output_path / "grid_hour_summary.csv"

        self.daily_summary.to_csv(
            daily_path,
            index=False,
        )

        self.grid_summary.to_csv(
            grid_path,
            index=False,
        )

        self.hourly_summary.to_csv(
            hourly_path,
            index=False,
        )

        logger.info(
            "Daily summary exported to %s",
            daily_path,
        )

        logger.info(
            "Grid summary exported to %s",
            grid_path,
        )

        logger.info(
            "Hourly summary exported to %s",
            hourly_path,
        )

        return daily_path, grid_path , hourly_path

    # -----------------------------------------------------
    # Convenience method
    # -----------------------------------------------------

    def run(self, output_dir="output"):
        """Run the complete processing pipeline."""

        self.load_data()
        self.clean_data()
        self.derive_time_features()
        self.aggregate_to_grid_time()
        self.derive_activity_features()
        self.compute_kpis()
        self.export_summary(output_dir)

        logger.info("Usage processing completed successfully.")

        return self

UsageProcessor("data/sms-call-internet-mi-2013-11-01.csv").run()