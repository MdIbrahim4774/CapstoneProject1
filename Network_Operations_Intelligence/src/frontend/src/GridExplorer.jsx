import { useState } from "react";
import { getGridActivity } from "./api";

function GridExplorer() {
  const [gridId, setGridId] = useState("");
  const [activity, setActivity] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function handleSearch(event) {
    event.preventDefault();

    if (!gridId.trim()) {
      setError("Please enter a grid ID.");
      return;
    }

    setLoading(true);
    setError(null);
    setActivity([]);

    try {
      const data = await getGridActivity(gridId.trim());

      // Supports either a direct array or { data: [...] }
      const rows = Array.isArray(data) ? data : data.data;

      setActivity(rows || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="grid-explorer">
      <h2>Grid Explorer</h2>

      <form onSubmit={handleSearch}>
        <input
          type="text"
          value={gridId}
          onChange={(event) => setGridId(event.target.value)}
          placeholder="Enter grid ID"
        />

        <button type="submit" disabled={loading}>
          {loading ? "Loading..." : "Explore"}
        </button>
      </form>

      {error && (
        <p className="error">
          {error}
        </p>
      )}

      {!loading && !error && activity.length === 0 && (
        <p>Enter a grid ID to view recent activity.</p>
      )}

      {activity.length > 0 && (
        <GridActivityTable activity={activity} />
      )}
    </section>
  );
}

function GridActivityTable({ activity }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Timestamp</th>
          <th>Calls</th>
          <th>SMS</th>
          <th>Internet</th>
          <th>Total Activity</th>
        </tr>
      </thead>

      <tbody>
        {activity.map((row, index) => (
          <tr key={row.timestamp || index}>
            <td>{row.timestamp}</td>
            <td>{row.total_calls ?? row.calls ?? 0}</td>
            <td>{row.total_sms ?? row.sms ?? 0}</td>
            <td>{row.internet ?? 0}</td>
            <td>{row.total_activity ?? 0}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default GridExplorer;