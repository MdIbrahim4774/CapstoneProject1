function ActivityTable({ rows }) {
  if (!rows || rows.length === 0) {
    return (
      <div className="empty-state">
        No activity data found.
      </div>
    );
  }

  return (
    <div className="table-container">
      <table>
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Calls</th>
            <th>SMS</th>
            <th>Internet</th>
            <th>Total Activity</th>
          </tr>
        </thead>

        <tbody>
          {rows.map((row, index) => (
            <tr key={row.timestamp || index}>
              <td>
                {row.timestamp ??
                  row.datetime ??
                  row.time ??
                  "N/A"}
              </td>

              <td>
                {formatValue(
                  row.call_activity ??
                  row.calls ??
                  row.total_calls ??
                  row.callin ??
                  0
                )}
              </td>

              <td>
                {formatValue(
                  row.sms_activity ??
                  row.sms ??
                  row.total_sms ??
                  row.smsin ??
                  0
                )}
              </td>

              <td>
                {formatValue(
                  row.internet ??
                  row.internet_activity ??
                  0
                )}
              </td>

              <td>
                <strong>
                  {formatValue(
                    row.total_activity ?? 0
                  )}
                </strong>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatValue(value) {
  if (typeof value === "number") {
    return value.toLocaleString();
  }

  return value ?? "N/A";
}

export default ActivityTable;