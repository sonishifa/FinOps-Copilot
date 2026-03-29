import { useEffect, useState } from 'react';
import { supabase } from '../supabase';

export default function AuditTrail() {
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState(null);

  // Filters
  const [filterAgent, setFilterAgent] = useState('all');
  const [filterType, setFilterType] = useState('all');
  const [filterSeverity, setFilterSeverity] = useState('all');
  const [agents, setAgents] = useState([]);
  const [eventTypes, setEventTypes] = useState([]);

  useEffect(() => {
    async function fetchEvents() {
      setLoading(true);

      const { data } = await supabase
        .from('audit_events')
        .select('*')
        .order('timestamp', { ascending: false })
        .limit(300);

      if (data) {
        setEvents(data);
        setAgents([...new Set(data.map(e => e.agent).filter(Boolean))].sort());
        setEventTypes([...new Set(data.map(e => e.event_type).filter(Boolean))].sort());
      }

      setLoading(false);
    }
    fetchEvents();
  }, []);

  const filtered = events.filter(ev => {
    if (filterAgent !== 'all' && ev.agent !== filterAgent) return false;
    if (filterType !== 'all' && ev.event_type !== filterType) return false;
    if (filterSeverity !== 'all' && ev.severity !== filterSeverity) return false;
    return true;
  });

  const severityClass = (sev) => {
    const s = (sev || '').toLowerCase();
    if (s === 'critical' || s === 'error') return 'badge-critical';
    if (s === 'high' || s === 'warning') return 'badge-high';
    if (s === 'medium' || s === 'info') return 'badge-medium';
    return 'badge-low';
  };

  const formatTime = (ts) => {
    if (!ts) return '—';
    try {
      const d = new Date(ts);
      return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch {
      return ts;
    }
  };

  if (loading) return <div className="loading-state">Loading audit trail...</div>;

  return (
    <div>
      <div className="page-header">
        <h1>Audit Trail</h1>
        <p>Immutable log of every agent action, decision, and event</p>
      </div>

      {/* Filters */}
      <div className="filter-bar">
        <select value={filterAgent} onChange={e => setFilterAgent(e.target.value)}>
          <option value="all">All Agents</option>
          {agents.map(a => (
            <option key={a} value={a}>{a}</option>
          ))}
        </select>

        <select value={filterType} onChange={e => setFilterType(e.target.value)}>
          <option value="all">All Event Types</option>
          {eventTypes.map(t => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>

        <select value={filterSeverity} onChange={e => setFilterSeverity(e.target.value)}>
          <option value="all">All Severities</option>
          <option value="critical">Critical</option>
          <option value="warning">Warning</option>
          <option value="info">Info</option>
        </select>

        <span className="text-xs text-muted" style={{ marginLeft: 'auto' }}>
          {filtered.length} events
        </span>
      </div>

      {/* Event list */}
      {filtered.length > 0 ? (
        <div className="audit-timeline">
          {filtered.map(ev => (
            <div key={ev.id}>
              <div
                className="audit-event"
                onClick={() => setExpandedId(expandedId === ev.id ? null : ev.id)}
                style={{ cursor: 'pointer' }}
              >
                <span className="event-time">{formatTime(ev.timestamp)}</span>
                <span className="event-agent">{ev.agent || '—'}</span>
                <span className="event-type">{ev.event_type}</span>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', overflow: 'hidden' }}>
                  <span className={`badge ${severityClass(ev.severity)}`}>
                    {ev.severity || 'info'}
                  </span>
                  <span className="event-detail">
                    {ev.run_id}
                    {ev.anomaly_id ? ` · ${ev.anomaly_id}` : ''}
                    {ev.action_id ? ` · ${ev.action_id}` : ''}
                  </span>
                </div>
              </div>

              {expandedId === ev.id && (
                <div style={{ padding: '0 8px 8px' }}>
                  <div className="expandable-content">
                    <pre>
                      {JSON.stringify(
                        typeof ev.payload === 'string' ? JSON.parse(ev.payload) : ev.payload,
                        null, 2
                      )}
                    </pre>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      ) : (
        <div className="empty-state">
          <div className="empty-icon">▤</div>
          <p>No audit events match the current filters.</p>
        </div>
      )}
    </div>
  );
}
