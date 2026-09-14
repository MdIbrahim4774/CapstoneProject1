function RiskBadge({ level }) {
  const normalized =
    String(level || "NORMAL").toUpperCase();

  let className = "risk-normal";

  if (
    normalized === "HIGH" ||
    normalized === "CRITICAL"
  ) {
    className = "risk-high";
  } else if (
    normalized === "ATTENTION" ||
    normalized === "MEDIUM" ||
    normalized === "WARNING"
  ) {
    className = "risk-attention";
  }

  return (
    <span className={`risk-badge ${className}`}>
      {normalized}
    </span>
  );
}

export default RiskBadge;