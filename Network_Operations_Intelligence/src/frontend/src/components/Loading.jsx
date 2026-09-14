function Loading({ message = "Loading..." }) {
  return (
    <div className="loading">
      <div className="spinner"></div>
      <span>{message}</span>
    </div>
  );
}

export default Loading;