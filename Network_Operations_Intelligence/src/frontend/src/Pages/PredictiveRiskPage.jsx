import { useState } from "react";

import { predictRisk } from "../api/networkApi";

import RiskBadge from "../components/RiskBadge";

import Loading from "../components/Loading";

import ErrorMessage from "../components/ErrorMessage";

function PredictiveRiskPage() {
  const [form, setForm] = useState({
    grid_id: "",
    avg_activity: "",
    activity_growth: "",
    active_hours: "",
    peak_ratio: "",
    variability: "",
    internet_share: "",
    feature_timestamp: "",
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
        avg_activity: Number(form.avg_activity),
        activity_growth: Number(form.activity_growth),
        active_hours: Number(form.active_hours),
        peak_ratio: Number(form.peak_ratio),
        variability: Number(form.variability),
        internet_share: Number(form.internet_share),
        feature_timestamp: form.feature_timestamp,
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
            Submit ML2 features to generate an ML3
            model-based network risk prediction.
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
              Average Activity

              <input
                name="avg_activity"
                type="number"
                step="any"
                value={form.avg_activity}
                onChange={handleChange}
                placeholder="e.g. 120.5"
                required
              />
            </label>

            <label>
              Activity Growth

              <input
                name="activity_growth"
                type="number"
                step="any"
                value={form.activity_growth}
                onChange={handleChange}
                placeholder="e.g. 0.05"
                required
              />
            </label>

            <label>
              Active Hours

              <input
                name="active_hours"
                type="number"
                min="0"
                max="24"
                step="1"
                value={form.active_hours}
                onChange={handleChange}
                placeholder="e.g. 22"
                required
              />
            </label>

            <label>
              Peak Ratio

              <input
                name="peak_ratio"
                type="number"
                min="0"
                step="any"
                value={form.peak_ratio}
                onChange={handleChange}
                placeholder="e.g. 2.8"
                required
              />
            </label>

            <label>
              Variability

              <input
                name="variability"
                type="number"
                min="0"
                step="any"
                value={form.variability}
                onChange={handleChange}
                placeholder="e.g. 0.15"
                required
              />
            </label>

            <label>
              Internet Share

              <input
                name="internet_share"
                type="number"
                min="0"
                max="1"
                step="any"
                value={form.internet_share}
                onChange={handleChange}
                placeholder="e.g. 0.82"
                required
              />
            </label>

            <label>
              Feature Timestamp

              <input
                name="feature_timestamp"
                type="datetime-local"
                value={form.feature_timestamp}
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
              Submit ML2 features to receive a prediction.
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

              {result.feature_timestamp && (
                <div className="model-version">
                  <span>Feature Timestamp</span>

                  <strong>
                    {result.feature_timestamp}
                  </strong>
                </div>
              )}

              {result.explanation_note && (
                <div className="prediction-disclaimer">
                  {result.explanation_note}
                </div>
              )}

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