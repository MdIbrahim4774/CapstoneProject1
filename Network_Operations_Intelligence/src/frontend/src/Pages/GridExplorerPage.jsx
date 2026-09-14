import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";

import { getGridActivity } from "../api/networkApi";

import ActivityGraph from "../components/ActivityGraph";
import ActivityTable from "../components/ActivityTable";
import Loading from "../components/Loading";
import ErrorMessage from "../components/ErrorMessage";

function GridExplorerPage() {
  const { gridId: routeGridId } = useParams();
  const navigate = useNavigate();

  const [inputGridId, setInputGridId] = useState(
    routeGridId || ""
  );

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedGrid, setSelectedGrid] = useState(
    routeGridId || null
  );

  useEffect(() => {
    if (routeGridId) {
      setInputGridId(routeGridId);
      loadGrid(routeGridId);
    }
  }, [routeGridId]);

  async function loadGrid(gridId) {
    try {
      setLoading(true);
      setError(null);

      const data = await getGridActivity(gridId);

      const activityRows = normalizeRows(data);

      if (activityRows.length === 0) {
        setError(
          `No activity was found for grid ${gridId}.`
        );
      }

      setRows(activityRows);
      setSelectedGrid(gridId);
    } catch (err) {
      setRows([]);
      setError(
        `Grid ${gridId} could not be loaded. ${err.message}`
      );
    } finally {
      setLoading(false);
    }
  }

  function handleSubmit(event) {
    event.preventDefault();

    const gridId = inputGridId.trim();

    if (!gridId) {
      setError("Please enter a grid ID.");
      return;
    }

    navigate(
      `/grid/${encodeURIComponent(gridId)}`
    );
  }

  return (
    <section>
      <div className="page-header">
        <div>
          <h1>Grid Explorer</h1>

          <p>
            Drill down into network activity for a
            specific grid.
          </p>
        </div>
      </div>

      <form
        className="search-form"
        onSubmit={handleSubmit}
      >
        <input
          type="text"
          value={inputGridId}
          onChange={(event) =>
            setInputGridId(event.target.value)
          }
          placeholder="Enter grid ID"
        />

        <button type="submit">
          Search Grid
        </button>
      </form>

      {loading && (
        <Loading message="Loading grid activity..." />
      )}

      {error && !loading && (
        <ErrorMessage message={error} />
      )}

      {!loading &&
        selectedGrid &&
        rows.length > 0 && (
          <>
            <div className="section-header">
              <div>
                <h2>Grid {selectedGrid}</h2>

                <span className="data-count">
                  {rows.length} records
                </span>
              </div>
            </div>

            {/* Activity Graph */}

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h2>Activity Over Time</h2>

                  <p>
                    Hourly network activity for Grid{" "}
                    {selectedGrid}.
                  </p>
                </div>
              </div>

              <ActivityGraph rows={rows} />
            </div>

            {/* Activity Table */}

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h2>Hourly Activity</h2>

                  <p>
                    Detailed activity measurements for
                    the selected grid.
                  </p>
                </div>
              </div>

              <ActivityTable rows={rows} />
            </div>
          </>
        )}
    </section>
  );
}

function normalizeRows(data) {
  if (Array.isArray(data)) {
    return data;
  }

  if (Array.isArray(data?.activity)) {
    return data.activity;
  }

  if (Array.isArray(data?.data)) {
    return data.data;
  }

  if (Array.isArray(data?.rows)) {
    return data.rows;
  }

  return [];
}

export default GridExplorerPage;