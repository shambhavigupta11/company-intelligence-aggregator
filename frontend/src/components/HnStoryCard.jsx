/** A single HackerNews story mentioning the selected company. */
export default function HnStoryCard({ story }) {
  const created = story.created_at ? new Date(story.created_at).toLocaleDateString() : '—'
  return (
    <article className="hn-card">
      <div className="hn-score">
        <span className="hn-points">{story.points ?? 0}</span>
        <span className="hn-points-label">points</span>
      </div>
      <div className="hn-body">
        <h3>
          {story.url ? (
            <a href={story.url} target="_blank" rel="noreferrer">
              {story.title}
            </a>
          ) : (
            story.title
          )}
        </h3>
        <p className="hn-meta">
          by {story.author || 'unknown'} · {story.num_comments ?? 0} comments · {created}
        </p>
      </div>
    </article>
  )
}
