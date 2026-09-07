import { useEffect, useState } from "react";
import { getNetworkSummary } from "./api";

function App() {
    const [summary, setSummary] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        async function loadSummary() {
            try {
                setLoading(true);
                setError(null);

                const data = await getNetworkSummary();
                setSummary(data);
            } catch (err) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        }

        loadSummary();
    }, []);

    return (
        <div>
            <h1>Network Operations Center</h1>

            <nav>
                <a href="/">Dashboard</a>{" "}
                <a href="/alerts">Alerts</a>{" "}
                <a href="/grid">Grid</a>
            </nav>

            <hr />

            {loading && <p>Loading network summary...</p>}

            {error && (
                <p role="alert">
                    Network summary is currently unavailable.
                </p>
            )}

            {summary && (
                <div>
                    <h2>Network Summary</h2>

                    <div>
                        <div>
                            <h3>Total Activity</h3>
                            <p>{summary.total_activity.toLocaleString()}</p>
                        </div>

                        <div>
                            <h3>Peak Hour</h3>
                            <p>{summary.peak_hour}:00</p>
                        </div>

                        <div>
                            <h3>Active Grids</h3>
                            <p>{summary.active_grids}</p>
                        </div>

                        <div>
                            <h3>Top Grid</h3>
                            <p>{summary.top_grid}</p>
                        </div>
                    </div>

                    <p>
                        Reporting time: {summary.as_of}
                    </p>
                </div>
            )}
        </div>
    );
}

export default App;