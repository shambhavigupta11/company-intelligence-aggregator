import { useEffect, useState } from 'react'

// Base URL for the API. Empty in dev (Vite proxies /api to the Flask server);
// set VITE_API_BASE at build time to point a deployed dashboard at a hosted API.
const API_BASE = import.meta.env.VITE_API_BASE || ''

/** Fetch JSON from an API path, throwing on non-2xx responses. */
export async function fetchJson(path) {
  const res = await fetch(`${API_BASE}${path}`)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

/**
 * Fetch `path` whenever any dependency changes, returning request state.
 *
 * Pass `path = null` to skip the request (e.g. when no org is selected).
 * A stale-guard prevents an earlier slow response from overwriting a newer one.
 */
export function useApi(path, deps = []) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!path) return undefined
    let active = true
    setLoading(true)
    setError(null)
    fetchJson(path)
      .then((json) => {
        if (active) setData(json)
      })
      .catch((e) => {
        if (active) setError(e.message)
      })
      .finally(() => {
        if (active) setLoading(false)
      })
    return () => {
      active = false
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)

  return { data, loading, error }
}
