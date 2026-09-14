
import { apiFetch } from "./api";

export async function getNetworkSummary() {
  return apiFetch("/network/summary");
}

export async function getGridActivity(gridId) {
  if (!gridId) {
    throw new Error("Grid ID is required.");
  }

  return apiFetch(`/network/grid/${encodeURIComponent(gridId)}`);
}

export async function getHotspots(limit = 10) {
  return apiFetch(`/network/hotspots?limit=${limit}`);
}

export async function getAlerts(limit = 20, severity = "") {
  const params = new URLSearchParams();

  params.append("limit", limit);

  if (severity) {
    params.append("severity", severity);
  }

  return apiFetch(`/network/alerts?${params.toString()}`);
}

/*
 * Fetch ALL hotspots for the map.
 *
 * This is intentionally separate from getHotspots()
 * because the table is controlled by the user's limit.
 */
export async function getAllHotspots() {
  return apiFetch("/network/hotspots?limit=500");
}

/*
 * Fetch ALL alerts for the map.
 *
 * The map needs the complete operational picture,
 * while the table can still respect the selected limit.
 */
export async function getAllAlerts(severity = "") {
  const params = new URLSearchParams();

  params.append("limit", "500");

  if (severity) {
    params.append("severity", severity);
  }

  return apiFetch(`/network/alerts?${params.toString()}`);
}

export async function predictRisk(payload) {
  return apiFetch("/network/predict-risk", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
