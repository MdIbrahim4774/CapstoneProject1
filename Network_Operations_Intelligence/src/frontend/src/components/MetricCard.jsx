function MetricCard({ title, value, subtitle }) {
  return (
    <div className="metric-card">
      <div className="metric-title">
        {title}
      </div>

      <div className="metric-value">
        {value}
      </div>

      {subtitle && (
        <div className="metric-subtitle">
          {subtitle}
        </div>
      )}
    </div>
  );
}

export default MetricCard;