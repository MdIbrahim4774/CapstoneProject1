function ErrorMessage({ message }) {
  return (
    <div className="error-message">
      <strong>Unable to load data</strong>
      <span>{message}</span>
    </div>
  );
}

export default ErrorMessage;