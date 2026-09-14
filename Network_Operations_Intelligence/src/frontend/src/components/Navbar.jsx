import { NavLink } from "react-router-dom";

function Navbar() {
  return (
    <nav className="navbar">
      <div className="brand">
        <span className="brand-icon">◆</span>
        NOC Intelligence
      </div>

      <div className="nav-links">
        <NavLink to="/overview">
          Overview
        </NavLink>

        <NavLink to="/grid">
          Grid Explorer
        </NavLink>

        <NavLink to="/hotspots-alerts">
          Hotspots & Alerts
        </NavLink>

        <NavLink to="/predictive-risk">
          Predictive Risk
        </NavLink>
      </div>
    </nav>
  );
}

export default Navbar;