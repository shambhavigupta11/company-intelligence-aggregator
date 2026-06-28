import { useState } from 'react'
import { useApi } from './api.js'
import AlertsTable from './components/AlertsTable.jsx'
import CompanyCard from './components/CompanyCard.jsx'
import HnStoryCard from './components/HnStoryCard.jsx'
import SignalsChart from './components/SignalsChart.jsx'

const DEFAULT_ORGS = ['databricks', 'snowflake-labs', 'dbt-labs', 'apache']

const TABS = [
  { key: 'github', label: 'GitHub' },
  { key: 'hn', label: 'HackerNews' },
  { key: 'alerts', label: 'Data Quality' },
]

function Status({ loading, error }) {
  if (loading) return <p className="status">Loading…</p>
  if (error) return <p className="error">Error: {error}</p>
  return null
}

function GitHubView({ org }) {
  const { data, loading, error } = useApi(`/api/companies/${org}/github?limit=12`, [org])
  const repos = data || []
  return (
    <>
      <Status loading={loading} error={error} />
      {!loading && !error && (
        <>
          <SignalsChart repos={repos} />
          <div className="repo-grid">
            {repos.map((r) => (
              <CompanyCard key={r.full_name} repo={r} />
            ))}
          </div>
        </>
      )}
    </>
  )
}

function HackerNewsView({ org }) {
  const { data, loading, error } = useApi(`/api/companies/${org}/hn?limit=20`, [org])
  const stories = data || []
  return (
    <>
      <Status loading={loading} error={error} />
      {!loading && !error && (
        <div className="hn-list">
          {stories.length === 0 && <p className="empty">No HackerNews mentions found.</p>}
          {stories.map((s, i) => (
            <HnStoryCard key={`${s.story_url || s.title}-${i}`} story={s} />
          ))}
        </div>
      )}
    </>
  )
}

function AlertsView() {
  const { data, loading, error } = useApi('/api/alerts?limit=50', [])
  return (
    <>
      <Status loading={loading} error={error} />
      {!loading && !error && <AlertsTable alerts={data || []} />}
    </>
  )
}

export default function App() {
  const [tab, setTab] = useState('github')
  const [org, setOrg] = useState('databricks')

  const showOrgs = tab !== 'alerts'

  return (
    <div className="app">
      <header>
        <h1>Mosaic</h1>
        <p className="subtitle">
          Real-time engineering, hiring, and mention signals for tracked companies.
        </p>
      </header>

      <nav className="tabs">
        {TABS.map((t) => (
          <button
            key={t.key}
            className={t.key === tab ? 'active' : ''}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </nav>

      {showOrgs && (
        <nav className="orgs">
          {DEFAULT_ORGS.map((o) => (
            <button key={o} className={o === org ? 'active' : ''} onClick={() => setOrg(o)}>
              {o}
            </button>
          ))}
        </nav>
      )}

      <main>
        {tab === 'github' && <GitHubView org={org} />}
        {tab === 'hn' && <HackerNewsView org={org} />}
        {tab === 'alerts' && <AlertsView />}
      </main>
    </div>
  )
}
