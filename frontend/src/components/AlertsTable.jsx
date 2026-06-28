/** Table of recent data-quality failures from the /api/alerts endpoint. */
export default function AlertsTable({ alerts }) {
  if (!alerts || alerts.length === 0) {
    return <p className="empty">No active data-quality alerts. 🎉</p>
  }

  return (
    <table className="alerts-table">
      <thead>
        <tr>
          <th>Severity</th>
          <th>Check</th>
          <th>Table</th>
          <th>Message</th>
          <th>When</th>
        </tr>
      </thead>
      <tbody>
        {alerts.map((a, i) => (
          <tr key={`${a.check_name}-${a.measured_at}-${i}`}>
            <td>
              <span className={`badge badge-${a.severity}`}>{a.severity}</span>
            </td>
            <td className="mono">{a.check_name}</td>
            <td className="mono">{a.table}</td>
            <td>{a.message}</td>
            <td className="muted">
              {a.measured_at ? new Date(a.measured_at).toLocaleString() : '—'}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
