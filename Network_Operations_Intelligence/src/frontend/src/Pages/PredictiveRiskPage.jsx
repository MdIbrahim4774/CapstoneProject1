import { useState } from "react";

import { predictRisk } from "../api/networkApi";

import RiskBadge from "../components/RiskBadge";
import Loading from "../components/Loading";
import ErrorMessage from "../components/ErrorMessage";

function PredictiveRiskPage() {
  const [form, setForm] = useState({
    grid_id: "",
    total_activity: "",
    total_calls: "",
    total_sms: "",
    internet: "",
  });

  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  function handleChange(event) {
    const { name, value } = event.target;

    setForm((previous) => ({
      ...previous,
      [name]: value,
    }));
  }

  async function handleSubmit(event) {
    event.preventDefault();

    try {
      setLoading(true);
      setError(null);
      setResult(null);

      const payload = {
        grid_id: form.grid_id,
        total_activity: Number(
          form.total_activity
        ),
        total_calls: Number(
          form.total_calls
        ),
        total_sms: Number(
          form.total_sms
        ),
        internet: Number(
          form.internet
        ),
      };

      const response = await predictRisk(payload);

      setResult(response);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  const riskScore =
    result?.risk_score ??
    result?.riskScore ??
    result?.score;

  const riskLevel =
    result?.risk_level ??
    result?.riskLevel ??
    result?.level ??
    "N/A";

  const modelVersion =
    result?.model_version ??
    result?.modelVersion ??
    result?.version ??
    "N/A";

  return (
    <section>
      <div className="page-header">
        <div>
          <h1>Predictive Risk</h1>
          <p>
            Request a model-based risk or anomaly score.
          </p>
        </div>
      </div>

      <div className="prediction-layout">
        <div className="panel">
          <div className="panel-header">
            <h2>Prediction Input</h2>
          </div>

          <form
            className="prediction-form"
            onSubmit={handleSubmit}
          >
            <label>
              Grid ID

              <input
                name="grid_id"
                value={form.grid_id}
                onChange={handleChange}
                placeholder="e.g. 123"
                required
              />
            </label>

            <label>
              Total Activity

              <input
                name="total_activity"
                type="number"
                step="any"
                value={form.total_activity}
                onChange={handleChange}
                required
              />
            </label>

            <label>
              Total Calls

              <input
                name="total_calls"
                type="number"
                step="any"
                value={form.total_calls}
                onChange={handleChange}
                required
              />
            </label>

            <label>
              Total SMS

              <input
                name="total_sms"
                type="number"
                step="any"
                value={form.total_sms}
                onChange={handleChange}
                required
              />
            </label>

            <label>
              Internet

              <input
                name="internet"
                type="number"
                step="any"
                value={form.internet}
                onChange={handleChange}
                required
              />
            </label>

            <button
              type="submit"
              disabled={loading}
            >
              {loading
                ? "Predicting..."
                : "Run Prediction"}
            </button>
          </form>

          {error && (
            <ErrorMessage message={error} />
          )}
        </div>

        <div className="panel prediction-result">
          <div className="panel-header">
            <h2>Model Output</h2>
          </div>

          {!result && !loading && (
            <div className="empty-state">
              Submit features to receive a prediction.
            </div>
          )}

          {loading && (
            <Loading message="Running prediction..." />
          )}

          {result && !loading && (
            <>
              <div className="prediction-score">
                <span>Risk Score</span>

                <strong>
                  {formatScore(riskScore)}
                </strong>
              </div>

              <div className="prediction-level">
                <span>Risk Level</span>

                <RiskBadge level={riskLevel} />
              </div>

              <div className="model-version">
                <span>Model Version</span>

                <strong>
                  {modelVersion}
                </strong>
              </div>

              <div className="prediction-disclaimer">
                Model output is a statistical prediction and
                should not be interpreted as certainty or as
                a confirmed operational event.
              </div>

              <button
                className="ai-button"
                disabled
                title="Available in the later Claude phase"
              >
                Explain with AI
              </button>
            </>
          )}
        </div>
      </div>
    </section>
  );
}

function formatScore(score) {
  if (
    score === null ||
    score === undefined ||
    score === ""
  ) {
    return "N/A";
  }

  const number = Number(score);

  if (Number.isNaN(number)) {
    return score;
  }

  return number.toFixed(4);
}

export default PredictiveRiskPage;