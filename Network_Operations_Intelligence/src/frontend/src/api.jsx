const API_BASE_URL = "http://localhost:8000";

export async function getNetworkSummary() {
    const response = await fetch(`${API_BASE_URL}/network/summary`);

    if (!response.ok) {
        throw new Error(`API request failed: ${response.status}`);
    }

    return response.json();
}

export async function getGridActivity(gridId) {
  const response = await fetch(
    `${API_BASE_URL}/network/grid/${encodeURIComponent(gridId)}`
  );

  if (!response.ok) {
    if (response.status === 404) {
      throw new Error("Grid not found");
    }

    throw new Error("Failed to load grid activity");
  }

  return response.json();
}