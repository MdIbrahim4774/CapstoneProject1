import {
  MapContainer,
  TileLayer,
  GeoJSON,
} from "react-leaflet";

import { useMemo } from "react";

import "leaflet/dist/leaflet.css";

function MilanMap({
  geojson,
  statuses = {},
  onGridSelect,
}) {
  const geoJsonKey = useMemo(
    () => JSON.stringify(statuses),
    [statuses]
  );

  if (!geojson) {
    return (
      <div className="map-empty">
        Milan grid reference is unavailable.
      </div>
    );
  }

  function getGridId(feature) {
    return String(
      feature?.properties?.grid_id ??
      feature?.properties?.cellId ??
      feature?.properties?.cellID ??
      feature?.properties?.CellID ??
      ""
    );
  }

  function getStatus(gridId) {
    return String(
      statuses[gridId]?.status ?? "NORMAL"
    ).toUpperCase();
  }

  function getStatusColor(status) {
    switch (status) {
      case "HIGH":
      case "CRITICAL":
        return "#e74c3c";

      case "ATTENTION":
      case "MEDIUM":
      case "WARNING":
        return "#f1c40f";

      case "NORMAL":
      default:
        return "#2ecc71";
    }
  }

  function getStatusBorderColor(status) {
    switch (status) {
      case "HIGH":
      case "CRITICAL":
        return "#922b21";

      case "ATTENTION":
      case "MEDIUM":
      case "WARNING":
        return "#9a6700";

      case "NORMAL":
      default:
        return "#117864";
    }
  }

  function styleFeature(feature) {
    const gridId = getGridId(feature);
    const status = getStatus(gridId);

    return {
      color: getStatusBorderColor(status),
      weight: 1,
      opacity: 0.9,

      fillColor: getStatusColor(status),
      fillOpacity: 0.65,
    };
  }

  function onEachFeature(feature, layer) {
    const gridId = getGridId(feature);
    const status = getStatus(gridId);

    const activity =
      statuses[gridId]?.activity;

    let tooltip = `
      <strong>Grid: ${gridId}</strong><br/>
      Status: ${status}
    `;

    if (
      activity !== undefined &&
      activity !== null
    ) {
      tooltip += `<br/>Activity: ${Number(activity).toLocaleString()}`;
    }

    layer.bindTooltip(tooltip);

    layer.on({
      mouseover: (event) => {
        event.target.setStyle({
          weight: 3,
          fillOpacity: 0.85,
        });

        event.target.bringToFront();
      },

      mouseout: (event) => {
        event.target.setStyle(
          styleFeature(feature)
        );
      },

      click: () => {
        if (gridId && onGridSelect) {
          onGridSelect(gridId);
        }
      },
    });
  }

  return (
    <div className="map-container">
      <MapContainer
        center={[45.4642, 9.19]}
        zoom={11}
        scrollWheelZoom={true}
        style={{
          width: "100%",
          height: "600px",
        }}
      >
        <TileLayer
          attribution="&copy; OpenStreetMap contributors"
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        <GeoJSON
          key={geoJsonKey}
          data={geojson}
          style={styleFeature}
          onEachFeature={onEachFeature}
        />
      </MapContainer>
    </div>
  );
}

export default MilanMap;