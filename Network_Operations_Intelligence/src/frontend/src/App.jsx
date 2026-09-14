import { Routes, Route, Navigate } from "react-router-dom";

import Layout from "./components/Layout";

import OverviewPage from "./pages/OverviewPage";
import GridExplorerPage from "./pages/GridExplorerPage";
import HotspotsAlertsPage from "./pages/HotspotsAlertsPage";
import PredictiveRiskPage from "./pages/PredictiveRiskPage";

function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Navigate to="/overview" replace />} />

        <Route path="overview" element={<OverviewPage />} />

        <Route path="grid" element={<GridExplorerPage />} />

        <Route
          path="grid/:gridId"
          element={<GridExplorerPage />}
        />

        <Route
          path="hotspots-alerts"
          element={<HotspotsAlertsPage />}
        />

        <Route
          path="predictive-risk"
          element={<PredictiveRiskPage />}
        />
      </Route>
    </Routes>
  );
}

export default App;