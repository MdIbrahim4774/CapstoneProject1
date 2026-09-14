import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";

function ActivityGraph({ rows }) {
  const chartData = normalizeChartData(rows);

  if (chartData.length === 0) {
    return (
      <div className="empty-state">
        No activity data available for the graph.
      </div>
    );
  }

  return (
    <div
      className="activity-graph"
      style={{
        width: "100%",
        height: "420px",
      }}
    >
      <ResponsiveContainer width="100%" height="100%">
        <LineChart
          data={chartData}
          margin={{
            top: 20,
            right: 30,
            left: 20,
            bottom: 20,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />

          <XAxis
            dataKey="timestamp"
            tickFormatter={formatTime}
            interval="preserveStartEnd"
          />

          <YAxis tickFormatter={formatNumber} />

          <Tooltip
            labelFormatter={formatDateTime}
            formatter={(value, name) => [
              formatNumber(value),
              formatSeriesName(name),
            ]}
          />

          <Legend />

          <Line
            type="monotone"
            dataKey="call_activity"
            name="Calls"
            dot={false}
            strokeWidth={2}
          />

          <Line
            type="monotone"
            dataKey="sms_activity"
            name="SMS"
            dot={false}
            strokeWidth={2}
          />

          <Line
            type="monotone"
            dataKey="internet_activity"
            name="Internet"
            dot={false}
            strokeWidth={2}
          />

          <Line
            type="monotone"
            dataKey="total_activity"
            name="Total Activity"
            dot={false}
            strokeWidth={3}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

function normalizeChartData(rows) {
  return rows
    .map((row) => ({
      timestamp:
        row.timestamp ??
        row.datetime ??
        row.time ??
        "",

      call_activity: toNumber(
        row.call_activity ??
        row.calls ??
        row.total_calls ??
        row.callin ??
        0
      ),

      sms_activity: toNumber(
        row.sms_activity ??
        row.sms ??
        row.total_sms ??
        row.smsin ??
        0
      ),

      internet_activity: toNumber(
        row.internet_activity ??
        row.internet ??
        0
      ),

      total_activity: toNumber(
        row.total_activity ??
        0
      ),
    }))
    .filter((row) => row.timestamp)
    .sort(
      (a, b) =>
        new Date(a.timestamp) -
        new Date(b.timestamp)
    );
}

function toNumber(value) {
  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : 0;
}

function formatTime(value) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatDateTime(value) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
}

function formatNumber(value) {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return "0";
  }

  return number.toLocaleString();
}

function formatSeriesName(name) {
  const names = {
    call_activity: "Calls",
    sms_activity: "SMS",
    internet_activity: "Internet",
    total_activity: "Total Activity",
  };

  return names[name] || name;
}

export default ActivityGraph;