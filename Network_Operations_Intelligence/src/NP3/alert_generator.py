import logging
from pathlib import Path

import pandas as pd

# from usage_processor import UsageProcessor


# ---------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Rule thresholds
# ---------------------------------------------------------

HIGH_ACTIVITY_MULTIPLIER = 2.0
SPIKE_MULTIPLIER = 1.5
DROP_MULTIPLIER = 0.5


# ---------------------------------------------------------
# Input / Output
# ---------------------------------------------------------

INPUT_FILE = Path(
    "output/grid_hour_summary.csv"
)

OUTPUT_FILE = Path(
    "output/network_alerts.csv"
)


# ---------------------------------------------------------
# Load NP2 output
# ---------------------------------------------------------

def load_np2_output(filepath):
    """Load hourly analytics produced by NP2."""

    logger.info(
        "Loading NP2 output: %s",
        filepath
    )

    df = pd.read_csv(filepath)

    required_columns = {
        "grid_id",
        "hour_timestamp",
        "total_activity",
    }

    missing_columns = (
        required_columns - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: "
            f"{sorted(missing_columns)}"
        )

    df["hour_timestamp"] = pd.to_datetime(
        df["hour_timestamp"]
    )

    df = df.sort_values(
        ["grid_id", "hour_timestamp"]
    ).reset_index(drop=True)

    # processor = UsageProcessor(
    #     "data/sms-call-internet-mi-2013-11-01.csv"
    # )

    # processor.load_data()
    # processor.clean_data()
    # processor.derive_time_features()
    # processor.aggregate_to_grid_time()
    # processor.derive_activity_features()

    # df = processor.hourly_summary

    return df


# ---------------------------------------------------------
# Activity floor
# ---------------------------------------------------------

def calculate_activity_floor(df):
    """
    Use the 25th percentile of grid-level daily
    activity totals as the activity floor.
    """

    daily_grid_totals = (
        df.groupby("grid_id")["total_activity"]
        .sum()
    )

    activity_floor = (
        daily_grid_totals.quantile(0.25)
    )

    logger.info(
        "Activity floor: %.2f",
        activity_floor
    )

    return activity_floor


# ---------------------------------------------------------
# Within-day baseline
# ---------------------------------------------------------

def calculate_baseline(group):
    """
    Calculate the leave-one-hour-out median.

    The current hour is excluded from its own baseline.
    """

    group = group.copy()

    activities = (
        group["total_activity"]
        .tolist()
    )

    baselines = []

    for index in range(len(activities)):

        other_hours = [
            value
            for i, value in enumerate(activities)
            if i != index
        ]

        if other_hours:
            baseline = pd.Series(
                other_hours
            ).median()
        else:
            baseline = None

        baselines.append(baseline)

    group["baseline_activity"] = baselines

    return group


def add_baselines(df):
    """Add within-day baseline for each grid/hour."""

    logger.info(
        "Calculating within-day baselines"
    )

    result = (
        df.groupby(
            "grid_id",
            group_keys=False
        )
        .apply(calculate_baseline)
        .reset_index(drop=True)
    )

    return result


# ---------------------------------------------------------
# Generate alerts
# ---------------------------------------------------------

def generate_alerts(df, activity_floor):
    """
    Apply the three NP3 rules.

    HIGH_ACTIVITY:
        current vs baseline

    ACTIVITY_SPIKE:
        current vs previous hour

    ACTIVITY_DROP:
        current vs baseline
    """

    logger.info(
        "Generating network alerts"
    )

    alerts = []

    for grid_id, group in df.groupby("grid_id"):

        group = (
            group
            .sort_values("hour_timestamp")
            .reset_index(drop=True)
        )

        # ---------------------------------------------
        # Activity floor
        # ---------------------------------------------

        daily_total = (
            group["total_activity"].sum()
        )

        if daily_total < activity_floor:
            continue

        # ---------------------------------------------
        # Evaluate every hour
        # ---------------------------------------------

        for index, row in group.iterrows():

            current_activity = (
                row["total_activity"]
            )

            baseline_activity = (
                row["baseline_activity"]
            )

            if pd.isna(baseline_activity):
                continue

            # Previous hour
            previous_activity = None

            if index > 0:
                previous_activity = group.loc[
                    index - 1,
                    "total_activity"
                ]

            # -----------------------------------------
            # HIGH_ACTIVITY
            #
            # Current vs baseline
            # -----------------------------------------

            if (
                baseline_activity > 0
                and current_activity
                >= baseline_activity
                * HIGH_ACTIVITY_MULTIPLIER
            ):

                alerts.append({
                    "grid_id": grid_id,
                    "timestamp": row["hour_timestamp"],
                    "alert_type": "HIGH_ACTIVITY",
                    "current_activity": current_activity,
                    "baseline_activity": baseline_activity,
                    "reason": (
                        "Current activity is at least "
                        "2x the within-day baseline."
                    ),
                })

            # -----------------------------------------
            # ACTIVITY_SPIKE
            #
            # Current vs previous hour ONLY
            # -----------------------------------------

            if (
                previous_activity is not None
                and previous_activity > 0
                and current_activity
                >= previous_activity
                * SPIKE_MULTIPLIER
            ):

                alerts.append({
                    "grid_id": grid_id,
                    "timestamp": row["hour_timestamp"],
                    "alert_type": "ACTIVITY_SPIKE",
                    "current_activity": current_activity,
                    "baseline_activity": baseline_activity,
                    "reason": (
                        "Current activity increased by "
                        "at least 50% from the "
                        "previous hour."
                    ),
                })

            # -----------------------------------------
            # ACTIVITY_DROP
            #
            # Current vs baseline
            # -----------------------------------------

            if (
                baseline_activity > 0
                and current_activity
                <= baseline_activity
                * DROP_MULTIPLIER
            ):

                alerts.append({
                    "grid_id": grid_id,
                    "timestamp": row["hour_timestamp"],
                    "alert_type": "ACTIVITY_DROP",
                    "current_activity": current_activity,
                    "baseline_activity": baseline_activity,
                    "reason": (
                        "Current activity is at or below "
                        "50% of the within-day baseline."
                    ),
                })

    return pd.DataFrame(alerts)


# ---------------------------------------------------------
# Operational summary
# ---------------------------------------------------------

def print_operational_summary(
    alerts,
    total_grid_hours
):
    """Print the required NP3 operational summary."""

    print(
        "\n========== NETWORK ALERT SUMMARY ==========\n"
    )

    if alerts.empty:
        print("No alerts generated.")
        return

    # Alerts by type
    print("Alerts by type:")

    print(
        alerts["alert_type"]
        .value_counts()
        .to_string()
    )

    # Top 10 grids
    print("\nTop 10 grids by alert count:")

    print(
        alerts["grid_id"]
        .value_counts()
        .head(10)
        .to_string()
    )

    # Unique grid/hour combinations
    alerting_grid_hours = (
        alerts[
            ["grid_id", "timestamp"]
        ]
        .drop_duplicates()
        .shape[0]
    )

    # Proportion
    proportion = (
        alerting_grid_hours
        / total_grid_hours
    )

    print(
        f"\nProportion of all grid/hours "
        f"that alerted: {proportion:.2%}"
    )

    print(
        "\n===========================================\n"
    )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    # 1. Load NP2 hourly output
    df = load_np2_output(
        INPUT_FILE
    )

    # 2. Calculate data-driven activity floor
    activity_floor = (
        calculate_activity_floor(df)
    )

    # 3. Calculate leave-one-hour-out baseline
    df = add_baselines(df)

    # 4. Generate alerts
    alerts = generate_alerts(
        df,
        activity_floor
    )

    # 5. Create output directory
    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # 6. Save alerts
    alerts.to_csv(
        OUTPUT_FILE,
        index=False
    )

    logger.info(
        "Saved %d alerts to %s",
        len(alerts),
        OUTPUT_FILE
    )

    # 7. Total grid/hour intervals
    total_grid_hours = (
        df[
            ["grid_id", "hour_timestamp"]
        ]
        .drop_duplicates()
        .shape[0]
    )

    # 8. Operational summary
    print_operational_summary(
        alerts,
        total_grid_hours
    )


if __name__ == "__main__":
    main()