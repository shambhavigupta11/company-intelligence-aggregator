export default function CompanyCard({ repo }) {
  return (
    <article className="repo-card">
      <h3>{repo.name}</h3>
      <p className="description">{repo.description || 'No description'}</p>
      <dl className="stats">
        <div>
          <dt>Stars</dt>
          <dd>{repo.stars.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Forks</dt>
          <dd>{repo.forks.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Open issues</dt>
          <dd>{repo.open_issues.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Language</dt>
          <dd>{repo.language || '—'}</dd>
        </div>
      </dl>
    </article>
  )
}
