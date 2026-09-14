import { useEffect, useState } from "react";

import { getNetworkSummary } from "../api/networkApi";

import MetricCard from "../components/MetricCard";
import Loading from "../components/Loading";
import ErrorMessage from "../components/ErrorMessage";
import StatusBanner from "../components/StatusBanner";

function OverviewPage() {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function loadSummary() {
      try {
        setLoading(true);
        setError(null);

        const data = await getNetworkSummary();

        setSummary(data);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }

    loadSummary();
  }, []);

  if (loading) {
    return <Loading message="Loading network summary..." />;
  }

  return (
    <section>
      <div className="page-header">
        <div>
          <h1>Network Overview</h1>
          <p>
            Current network activity and operational status.
          </p>
        </div>

        {summary?.as_of && (
          <div className="reporting-time">
            Reporting time
            <strong>{summary.as_of}</strong>
          </div>
        )}
      </div>

      {error && (
        <>
          <StatusBanner message={error} />
          <ErrorMessage message={error} />
        </>
      )}

      {!error && summary && (
        <div className="metrics-grid">
          <MetricCard
            title="Total Activity"
            value={formatNumber(
              summary.total_activity ??
              summary.activity ??
              0
            )}
            subtitle="Network-wide"
          />

          <MetricCard
            title="Peak Hour"
            value={
              summary.peak_hour ??
              summary.peak_time ??
              "N/A"
            }
            subtitle="Highest observed activity"
          />

          <MetricCard
            title="Active Grids"
            value={formatNumber(
              summary.active_grids ??
              summary.grid_count ??
              0
            )}
            subtitle="Grids with activity"
          />

          <MetricCard
            title="Highest Grid"
            value={
              summary.highest_grid ??
              summary.top_grid ??
              summary.grid_id ??
              "N/A"
            }
            subtitle="Highest current activity"
          />
        </div>
      )}

      {!error && !summary && (
        <div className="empty-state">
          No network summary data is available.
        </div>
      )}
    </section>
  );
}

function formatNumber(value) {
  if (value === null || value === undefined) {
    return "N/A";
  }

  if (typeof value === "number") {
    return value.toLocaleString();
  }

  return value;
}

export default OverviewPage;