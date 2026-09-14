import { Outlet } from "react-router-dom";
import Navbar from "./Navbar";

function Layout() {
  return (
    <div className="app">
      <Navbar />

      <main className="main-content">
        <Outlet />
      </main>
    </div>
  );
}

export default Layout;