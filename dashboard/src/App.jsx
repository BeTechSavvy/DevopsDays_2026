import { useState, useEffect } from 'react'
import './App.css'

const API_BASE = 'http://localhost:5050/api'

const SEVERITY_COLORS = {
  high: '#E5484D',
  medium: '#E8A23D',
  low: '#4FB8AF',
}

const DETECTION_LABELS = {
  ml: 'ML',
  rule: 'Rule',
  'ml+rule': 'ML+Rule',
}

// Same reasons ml-engine/k8s_signals.py treats as high severity
const HIGH_SEVERITY_REASONS = ['OOMKilled', 'CrashLoopBackOff', 'ImagePullBackOff', 'ErrImagePull', 'CreateContainerConfigError']

const shortTime = (t) => t?.split('.')[0] || t

// Plain-English version of one k8s_signals entry (mirrors describe_signal in k8s_signals.py)
function describeSignal(s) {
  const window = `${shortTime(s.first_seen)} → ${shortTime(s.last_seen)}`
  if (s.kind === 'restart') {
    const n = Math.round(s.value)
    return `Container ${s.target} restarted ${n} time${n === 1 ? '' : 's'} (last terminated: ${s.reason}), ${window}`
  }
  if (s.kind === 'waiting') {
    return `Container ${s.target} stuck waiting in ${s.reason}, ${window}`
  }
  return `Deployment ${s.target} had up to ${Math.round(s.value)} unavailable replica(s), ${window}`
}

function ConfidenceMeter({ value, color }) {
  const segments = 10
  const filled = Math.round(value * segments)

  return (
    <div className="confidence-meter">
      {Array.from({ length: segments }).map((_, i) => (
        <div
          key={i}
          className="confidence-segment"
          style={{
            background: i < filled ? color : 'var(--border)',
          }}
        />
      ))}
      <span className="confidence-label">{Math.round(value * 100)}%</span>
    </div>
  )
}

function IncidentRow({ incident, isSelected, onClick }) {
  const color = SEVERITY_COLORS[incident.max_severity] || SEVERITY_COLORS.low

  return (
    <button
      className={`incident-row ${isSelected ? 'selected' : ''}`}
      onClick={onClick}
      style={{ borderLeftColor: color }}
    >
      <div className="incident-row-top">
        <span className="incident-id">{incident.incident_id}</span>
        <span className="severity-badge" style={{ color, background: `${color}1a` }}>
          {incident.max_severity}
        </span>
      </div>
      <div className="incident-row-meta">
        <span>{incident.start_time?.split('.')[0] || incident.start_time}</span>
        <span className="dot">·</span>
        <span>{incident.anomaly_count} anomal{incident.anomaly_count === 1 ? 'y' : 'ies'}</span>
      </div>
      {incident.diagnosis?.summary && (
        <p className="incident-row-summary">{incident.diagnosis.summary}</p>
      )}
    </button>
  )
}

function DetailPanel({ incident }) {
  if (!incident) {
    return (
      <div className="detail-empty">
        <p>Select an incident to view its diagnosis.</p>
      </div>
    )
  }

  const color = SEVERITY_COLORS[incident.max_severity] || SEVERITY_COLORS.low
  const diagnosis = incident.diagnosis
  const detection = incident.detection || 'ml' // incidents saved before k8s signals existed
  const signals = incident.k8s_signals || []

  return (
    <div className="detail-panel">
      <div className="detail-header">
        <div>
          <span className="detail-eyebrow">incident</span>
          <h2 className="detail-id">{incident.incident_id}</h2>
        </div>
        <div className="detail-badges">
          <span className="detection-badge">{DETECTION_LABELS[detection] || detection}</span>
          <span className="severity-badge large" style={{ color, background: `${color}1a` }}>
            {incident.max_severity}
          </span>
        </div>
      </div>

      {detection === 'rule' ? (
        <p className="rule-match">rule match · no ML confidence score</p>
      ) : (
        <ConfidenceMeter value={incident.avg_confidence} color={color} />
      )}

      <div className="detail-section">
        <span className="detail-label">Window</span>
        <p className="detail-value mono">{incident.start_time} &rarr; {incident.end_time}</p>
      </div>

      {signals.length > 0 && (
        <div className="detail-section">
          <span className="detail-label">Kubernetes signals ({signals.length})</span>
          <div className="signal-list">
            {signals.map((s, i) => {
              const reasonColor =
                HIGH_SEVERITY_REASONS.includes(s.reason) || s.kind === 'replicas_unavailable'
                  ? SEVERITY_COLORS.high
                  : SEVERITY_COLORS.medium
              return (
                <div key={i} className="signal-item">
                  <span className="signal-reason" style={{ color: reasonColor, background: `${reasonColor}1a` }}>
                    {s.reason}
                  </span>
                  <span className="detail-value muted">{describeSignal(s)}</span>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {diagnosis ? (
        <>
          <div className="detail-section">
            <span className="detail-label">Summary</span>
            <p className="detail-value">{diagnosis.summary}</p>
          </div>
          <div className="detail-section">
            <span className="detail-label">Likely root cause</span>
            <p className="detail-value">{diagnosis.likely_root_cause}</p>
          </div>
          <div className="detail-section">
            <span className="detail-label">Confidence reasoning</span>
            <p className="detail-value muted">{diagnosis.confidence_reasoning}</p>
          </div>
          <div className="detail-section fix">
            <span className="detail-label">Suggested fix</span>
            <p className="detail-value mono fix-text">{diagnosis.suggested_fix}</p>
          </div>
        </>
      ) : (
        <div className="detail-section">
          <p className="detail-value muted">No diagnosis attached to this incident yet.</p>
        </div>
      )}

      <div className="detail-section">
        <span className="detail-label">Raw anomalies ({incident.anomaly_count})</span>
        {incident.anomaly_count === 0 && (
          <p className="detail-value muted">No metric anomalies — detected from cluster state.</p>
        )}
        <div className="anomaly-list">
          {incident.anomalies?.map((a, i) => (
            <div key={i} className="anomaly-item">
              <span className="mono anomaly-time">{a.timestamp?.split('.')[0]}</span>
              <span className="anomaly-metrics mono">
                {Object.entries(a.metrics || {}).map(([k, v]) => `${k}: ${Number(v).toFixed(1)}`).join('  ')}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function App() {
  const [incidents, setIncidents] = useState([])
  const [selectedId, setSelectedId] = useState(null)
  const [status, setStatus] = useState('loading')
  const [filter, setFilter] = useState('all')

  const fetchIncidents = () => {
    fetch(`${API_BASE}/incidents`)
      .then((res) => {
        if (!res.ok) throw new Error('API error')
        return res.json()
      })
      .then((data) => {
        setIncidents(data)
        setStatus('ok')
        setSelectedId((prev) => (prev === null && data.length > 0 ? data[0].incident_id : prev))
      })
      .catch(() => setStatus('error'))
  }

  useEffect(() => {
    fetchIncidents()
    const interval = setInterval(fetchIncidents, 15000)
    return () => clearInterval(interval)
  }, [])

  const filtered = filter === 'all' ? incidents : incidents.filter((i) => i.max_severity === filter)
  const selected = incidents.find((i) => i.incident_id === selectedId)

  const counts = {
    high: incidents.filter((i) => i.max_severity === 'high').length,
    medium: incidents.filter((i) => i.max_severity === 'medium').length,
    low: incidents.filter((i) => i.max_severity === 'low').length,
  }

  return (
    <div className="app">
      <header className="app-header">
        <div>
          <h1>Chaos Healer</h1>
          <span className="app-subtitle">Kubernetes incident intelligence · advisory only</span>
        </div>
        <div className={`status-pill ${status}`}>
          <span className="status-dot" />
          {status === 'ok' ? 'Connected' : status === 'loading' ? 'Connecting…' : 'API unreachable'}
        </div>
      </header>

      {status === 'error' && (
        <div className="error-banner">
          Can't reach the API at {API_BASE}. Make sure <code>python api.py</code> is running.
        </div>
      )}

      <div className="summary-row">
        {['high', 'medium', 'low'].map((sev) => (
          <button
            key={sev}
            className={`summary-card ${filter === sev ? 'active' : ''}`}
            style={{ '--sev-color': SEVERITY_COLORS[sev] }}
            onClick={() => setFilter(filter === sev ? 'all' : sev)}
          >
            <span className="summary-count">{counts[sev]}</span>
            <span className="summary-label">{sev}</span>
          </button>
        ))}
      </div>

      <div className="main-grid">
        <div className="incident-list">
          {filtered.length === 0 && status === 'ok' && (
            <p className="empty-list">No incidents detected yet.</p>
          )}
          {filtered.map((incident) => (
            <IncidentRow
              key={incident.incident_id}
              incident={incident}
              isSelected={incident.incident_id === selectedId}
              onClick={() => setSelectedId(incident.incident_id)}
            />
          ))}
        </div>
        <DetailPanel incident={selected} />
      </div>
    </div>
  )
}

export default App