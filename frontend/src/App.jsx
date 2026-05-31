import { useEffect, useState } from 'react'
import CompanyCard from './components/CompanyCard.jsx'

const DEFAULT_ORGS = ['databricks', 'snowflake-labs', 'dbt-labs', 'apache']

export default function App() {
  const [org, setOrg] = useState('databricks')
  const [repos, setRepos] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetch(`/api/companies/${org}/github?limit=10`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(setRepos)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [org])

  return (
    <div className="app">
      <header>
        <h1>Mosaic</h1>
        <p className="subtitle">Real-time engineering, hiring, and mention signals for tracked companies.</p>
      </header>

      <nav className="orgs">
        {DEFAULT_ORGS.map((o) => (
          <button
            key={o}
            className={o === org ? 'active' : ''}
            onClick={() => setOrg(o)}
          >
            {o}
          </button>
        ))}
      </nav>

      <main>
        {loading && <p>Loading…</p>}
        {error && <p className="error">Error: {error}</p>}
        {!loading && !error && (
          <div className="repo-grid">
            {repos.map((r) => (
              <CompanyCard key={r.full_name} repo={r} />
            ))}
          </div>
        )}
      </main>
    </div>
  )
}
