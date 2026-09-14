import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import {
  getHotspots,
  getAlerts,
  getAllHotspots,
  getAllAlerts,
} from "../api/networkApi";

import MilanMap from "../components/MilanMap";
import RiskBadge from "../components/RiskBadge";
import Loading from "../components/Loading";
import ErrorMessage from "../components/ErrorMessage";

function HotspotsAlertsPage() {
  const navigate = useNavigate();

  // ----------------------------------------
  // TABLE DATA
  // ----------------------------------------

  const [hotspots, setHotspots] = useState([]);
  const [alerts, setAlerts] = useState([]);

  // ----------------------------------------
  // MAP DATA
  // ----------------------------------------

  const [allHotspots, setAllHotspots] = useState([]);
  const [allAlerts, setAllAlerts] = useState([]);

  // ----------------------------------------
  // GEOJSON
  // ----------------------------------------

  const [geojson, setGeojson] = useState(null);

  // ----------------------------------------
  // FILTERS
  // ----------------------------------------

  const [limit, setLimit] = useState(10);
  const [severity, setSeverity] = useState("");

  // ----------------------------------------
  // LOADING / ERROR
  // ----------------------------------------

  const [loading, setLoading] = useState(true);
  const [mapLoading, setMapLoading] = useState(true);

  const [error, setError] = useState(null);
  const [mapError, setMapError] = useState(null);

  // ----------------------------------------
  // LOAD GEOJSON ONCE
  // ----------------------------------------

  useEffect(() => {
    async function loadGeoJSON() {
      try {
        setMapLoading(true);

        const response = await fetch(
          "/reference/milano-grid.geojson"
        );

        if (!response.ok) {
          throw new Error(
            `GeoJSON request failed: ${response.status}`
          );
        }

        const data = await response.json();

        setGeojson(data);
      } catch (err) {
        setMapError(err.message);
      } finally {
        setMapLoading(false);
      }
    }

    loadGeoJSON();
  }, []);

  // ----------------------------------------
  // LOAD TABLE DATA
  // ----------------------------------------
  //
  // This respects the selected LIMIT.
  //

  useEffect(() => {
    async function loadTableData() {
      try {
        setLoading(true);
        setError(null);

        const [hotspotData, alertData] =
          await Promise.all([
            getHotspots(limit),
            getAlerts(limit, severity),
          ]);

        setHotspots(
          normalizeArray(hotspotData)
        );

        setAlerts(
          normalizeArray(alertData)
        );
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }

    loadTableData();
  }, [limit, severity]);

  // ----------------------------------------
  // LOAD ALL MAP DATA
  // ----------------------------------------
  //
  // IMPORTANT:
  // This does NOT use the selected table limit.
  //
  // Therefore the map can color every grid.
  //

  useEffect(() => {
    async function loadMapData() {
      try {
        setMapLoading(true);
        setMapError(null);

        const [hotspotData, alertData] =
          await Promise.all([
            getAllHotspots(),
            getAllAlerts(severity),
          ]);

        setAllHotspots(
          normalizeArray(hotspotData)
        );

        setAllAlerts(
          normalizeArray(alertData)
        );
      } catch (err) {
        setMapError(err.message);
      } finally {
        setMapLoading(false);
      }
    }

    loadMapData();
  }, [severity]);

  // ----------------------------------------
  // BUILD MAP STATUS
  // ----------------------------------------
  //
  // This uses ALL hotspots + ALL alerts,
  // NOT the limited table datasets.
  //

  const mapStatuses = useMemo(() => {
    const statuses = {};

    // --------------------------------------
    // ALL HOTSPOTS
    // --------------------------------------

    allHotspots.forEach((item) => {
      const gridId = getGridId(item);

      if (!gridId) {
        return;
      }

      const status = getStatus(item);

      statuses[gridId] = {
        status,
        activity:
          item.total_activity ??
          item.activity ??
          0,
      };
    });

    // --------------------------------------
    // ALL ALERTS
    // --------------------------------------

    allAlerts.forEach((item) => {
      const gridId = getGridId(item);

      if (!gridId) {
        return;
      }

      const alertStatus = getStatus(item);

      const existingStatus =
        statuses[gridId]?.status || "NORMAL";

      /*
       * Keep the most severe status.
       *
       * HIGH > ATTENTION > NORMAL
       */

      const finalStatus = getHigherSeverity(
        existingStatus,
        alertStatus
      );

      statuses[gridId] = {
        ...(statuses[gridId] || {}),
        status: finalStatus,
        alert: item,
      };
    });

    return statuses;
  }, [allHotspots, allAlerts]);

  // ----------------------------------------
  // OPEN GRID EXPLORER
  // ----------------------------------------

  function openGrid(gridId) {
    navigate(
      `/grid/${encodeURIComponent(gridId)}`
    );
  }

  return (
    <section>
      {/* ---------------------------------- */}
      {/* HEADER */}
      {/* ---------------------------------- */}

      <div className="page-header">
        <div>
          <h1>Hotspots & Alerts</h1>

          <p>
            Operational attention areas across
            the Milan network.
          </p>
        </div>
      </div>

      {/* ---------------------------------- */}
      {/* FILTERS */}
      {/* ---------------------------------- */}

      <div className="filters">
        <label>
          Limit

          <select
            value={limit}
            onChange={(event) =>
              setLimit(
                Number(event.target.value)
              )
            }
          >
            <option value={5}>5</option>
            <option value={10}>10</option>
            <option value={20}>20</option>
            <option value={50}>50</option>
          </select>
        </label>

        <label>
          Severity

          <select
            value={severity}
            onChange={(event) =>
              setSeverity(event.target.value)
            }
          >
            <option value="">
              All
            </option>

            <option value="NORMAL">
              NORMAL
            </option>

            <option value="ATTENTION">
              ATTENTION
            </option>

            <option value="HIGH">
              HIGH
            </option>
          </select>
        </label>
      </div>

      {/* ---------------------------------- */}
      {/* API ERROR */}
      {/* ---------------------------------- */}

      {error && (
        <ErrorMessage message={error} />
      )}

      {/* ---------------------------------- */}
      {/* TABLES */}
      {/* ---------------------------------- */}

      {loading ? (
        <Loading message="Loading hotspots and alerts..." />
      ) : (
        <>
          <div className="two-column">

            {/* HOTSPOTS */}

            <div className="panel">
              <div className="panel-header">
                <h2>Hotspots</h2>
              </div>

              <OperationalTable
                rows={hotspots}
                onGridSelect={openGrid}
              />
            </div>

            {/* ALERTS */}

            <div className="panel">
              <div className="panel-header">
                <h2>Alerts</h2>
              </div>

              <OperationalTable
                rows={alerts}
                onGridSelect={openGrid}
                showRiskScore
              />
            </div>

          </div>

          {/* -------------------------------- */}
          {/* MAP */}
          {/* -------------------------------- */}

          <div className="panel map-panel">

            <div className="panel-header">

              <div>
                <h2>Milan Grid Map</h2>

                <p>
                  All Milan grids are shown.
                  Click a grid to open Grid Explorer.
                </p>
              </div>

              <div className="legend">

                <span>
                  <i className="legend-normal" />
                  NORMAL
                </span>

                <span>
                  <i className="legend-attention" />
                  ATTENTION
                </span>

                <span>
                  <i className="legend-high" />
                  HIGH
                </span>

              </div>

            </div>

            {mapLoading && (
              <Loading message="Loading Milan grid..." />
            )}

            {mapError && (
              <ErrorMessage message={mapError} />
            )}

            {!mapLoading && !mapError && (
              <MilanMap
                geojson={geojson}
                statuses={mapStatuses}
                onGridSelect={openGrid}
              />
            )}

          </div>
        </>
      )}
    </section>
  );
}

// ========================================
// TABLE
// ========================================

function OperationalTable({
  rows,
  onGridSelect,
  showRiskScore = false,
}) {
  if (!rows || rows.length === 0) {
    return (
      <div className="empty-state">
        No records found.
      </div>
    );
  }

  return (
    <div className="table-container">
      <table>
        <thead>
          <tr>
            <th>Grid</th>
            <th>{showRiskScore ? "Risk Score" : "Activity"}</th>
            <th>Status</th>
            <th>Timestamp</th>
          </tr>
        </thead>

        <tbody>
          {rows.map((row, index) => {
            const gridId = getGridId(row);

            return (
              <tr
                key={`${gridId}-${index}`}
              >
                <td>
                  <button
                    className="link-button"
                    onClick={() => onGridSelect(gridId)}
                  >
                    {gridId || "N/A"}
                  </button>
                </td>

                <td>
                  {showRiskScore
                    ? formatNumber(row?.risk?.score)
                    : formatNumber(
                        row.total_activity ??
                        row.activity ??
                        0
                      )}
                </td>

                <td>
                  <RiskBadge
                    level={getStatus(row)}
                  />
                </td>

                <td>
                  {row.timestamp ??
                    row.datetime ??
                    row.hour ??
                    row.as_of ??
                    "N/A"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
// ========================================
// NORMALIZE API RESPONSE
// ========================================

function normalizeArray(data) {
  if (Array.isArray(data)) {
    return data;
  }

  if (Array.isArray(data?.data)) {
    return data.data;
  }

  if (Array.isArray(data?.results)) {
    return data.results;
  }

  if (Array.isArray(data?.items)) {
    return data.items;
  }

  return [];
}

// ========================================
// GRID ID
// ========================================

function getGridId(row) {
  return String(
    row?.grid_id ??
    row?.gridId ??
    row?.cellId ??
    row?.CellID ??
    ""
  );
}

// ========================================
// STATUS
// ========================================

function getStatus(row) {
  return String(
    row?.severity ??
    row?.status ??
    row?.risk_level ??
    row?.riskLevel ??
    "NORMAL"
  ).toUpperCase();
}

// ========================================
// STATUS PRIORITY
// ========================================

function getStatusPriority(status) {
  switch (String(status).toUpperCase()) {
    case "HIGH":
    case "CRITICAL":
      return 3;

    case "ATTENTION":
    case "MEDIUM":
    case "WARNING":
      return 2;

    case "NORMAL":
    default:
      return 1;
  }
}

function getHigherSeverity(
  current,
  incoming
) {
  return getStatusPriority(incoming) >
    getStatusPriority(current)
    ? incoming
    : current;
}

// ========================================
// NUMBER FORMAT
// ========================================

function formatNumber(value) {
  if (typeof value === "number") {
    return value.toLocaleString();
  }

  return value ?? "N/A";
}

export default HotspotsAlertsPage;
