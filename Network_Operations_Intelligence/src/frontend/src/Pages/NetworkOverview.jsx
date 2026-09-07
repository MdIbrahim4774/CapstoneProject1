import { useEffect, useState } from "react";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;

function NetworkOverview() {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    async function fetchSummary() {
      try {
        setLoading(true);
        setError(false);

        const response = await fetch(`${API_BASE_URL}/network/summary`);

        if (!response.ok) {
          throw new Error("API request failed");
        }

        const data = await response.json();
        setSummary(data);
      } catch (err) {
        console.error("Failed to load network summary:", err);
        setError(true);
      } finally {
        setLoading(false);
      }
    }

    fetchSummary();
  }, []);

  if (loading) {
    return <div>Loading network overview...</div>;
  }

  return (
    <main>
      <h1>Network Overview</h1>

      {error && (
        <div role="alert">
          Network summary is currently unavailable.
        </div>
      )}

      {summary && (
        <>
          <section>
            <div>
              <h2>Total Activity</h2>
              <p>{summary.total_activity.toLocaleString()}</p>
            </div>

            <div>
              <h2>Peak Hour</h2>
              <p>{summary.peak_hour}:00</p>
            </div>

            <div>
              <h2>Active Grids</h2>
              <p>{summary.active_grids}</p>
            </div>

            <div>
              <h2>Top Grid</h2>
              <p>{summary.top_grid}</p>
            </div>
          </section>

          <p>
            Reporting time:{" "}
            {new Date(summary.as_of).toLocaleString()}
          </p>
        </>
      )}
    </main>
  );
}

export default NetworkOverview;