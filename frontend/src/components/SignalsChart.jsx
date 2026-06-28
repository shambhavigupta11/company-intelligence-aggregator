import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

/** Horizontal bar chart of stars for the top repos in the current view. */
export default function SignalsChart({ repos }) {
  const data = [...repos]
    .sort((a, b) => b.stars - a.stars)
    .slice(0, 8)
    .map((r) => ({ name: r.name, stars: r.stars }))

  if (data.length === 0) return null

  return (
    <div className="chart-panel">
      <h2 className="section-title">Top repos by stars</h2>
      <ResponsiveContainer width="100%" height={Math.max(160, data.length * 32)}>
        <BarChart data={data} layout="vertical" margin={{ left: 24, right: 24 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" horizontal={false} />
          <XAxis type="number" stroke="#94a3b8" fontSize={12} />
          <YAxis
            type="category"
            dataKey="name"
            stroke="#94a3b8"
            fontSize={12}
            width={140}
          />
          <Tooltip
            contentStyle={{ background: '#1e293b', border: '1px solid #334155', color: '#e2e8f0' }}
            cursor={{ fill: 'rgba(56,189,248,0.1)' }}
          />
          <Bar dataKey="stars" fill="#38bdf8" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
