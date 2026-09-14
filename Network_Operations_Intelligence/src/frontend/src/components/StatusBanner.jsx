function StatusBanner({ message }) {
  if (!message) {
    return null;
  }

  return (
    <div className="status-banner">
      <strong>API unavailable</strong>
      <span>{message}</span>
    </div>
  );
}

export default StatusBanner;