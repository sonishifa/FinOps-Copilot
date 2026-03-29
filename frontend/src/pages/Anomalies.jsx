import { useEffect, useState } from 'react';
import { supabase } from '../supabase';

const SEVERITY_ORDER = { critical: 0, high: 1, medium: 2, low: 3 };

const ANOMALY_TYPES = [
  'spend_spike', 'duplicate_vendor', 'sla_breach_risk', 'shadow_it',
  'contract_anomaly', 'churn_risk', 'resource_waste', 'instance_overpay',
  'fraud_signal', 'invoice_anomaly',
];

export default function Anomalies() {
  const [anomalies, setAnomalies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState(null);

  // Filters
  const [filterType, setFilterType] = useState('all');
  const [filterSeverity, setFilterSeverity] = useState('all');
  const [filterRun, setFilterRun] = useState('all');
  const [runs, setRuns] = useState([]);

  useEffect(() => {
    async function fetchAnomalies() {
      setLoading(true);

      const { data } = await supabase
        .from('audit_events')
        .select('*')
        .eq('event_type', 'anomaly_detected')
        .order('timestamp', { ascending: false })
        .limit(200);

      if (data) {
        const parsed = data.map(ev => {
          const payload = typeof ev.payload === 'string'
            ? JSON.parse(ev.payload)
            : (ev.payload || {});
          return { ...ev, parsed: payload };
        });
        setAnomalies(parsed);

        // Collect unique run IDs
        const uniqueRuns = [...new Set(parsed.map(a => a.run_id).filter(Boolean))];
        setRuns(uniqueRuns);
      }

      setLoading(false);
    }
    fetchAnomalies();
  }, []);

  const filtered = anomalies.filter(a => {
    if (filterType !== 'all' && a.parsed.anomaly_type !== filterType) return false;
    if (filterSeverity !== 'all' && a.parsed.severity !== filterSeverity) return false;
    if (filterRun !== 'all' && a.run_id !== filterRun) return false;
    return true;
  });

  const severityClass = (sev) => {
    const s = (sev || '').toLowerCase();
    if (s === 'critical') return 'badge-critical';
    if (s === 'high') return 'badge-high';
    if (s === 'medium') return 'badge-medium';
    return 'badge-low';
  };

  const fmt = (n) => {
    if (n == null) return '—';
    return '$' + Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
  };

  if (loading) return <div className="loading-state">Loading anomalies...</div>;

  return (
    <div>
      <div className="page-header">
        <h1>Anomalies</h1>
        <p>All detected cost anomalies across enterprise data sources</p>
      </div>

      {/* Filters */}
      <div className="filter-bar">
        <select value={filterType} onChange={e => setFilterType(e.target.value)}>
          <option value="all">All Types</option>
          {ANOMALY_TYPES.map(t => (
            <option key={t} value={t}>{t.replace(/_/g, ' ')}</option>
          ))}
        </select>

        <select value={filterSeverity} onChange={e => setFilterSeverity(e.target.value)}>
          <option value="all">All Severities</option>
          <option value="critical">Critical</option>
          <option value="high">High</option>
          <option value="medium">Medium</option>
          <option value="low">Low</option>
        </select>

        <select value={filterRun} onChange={e => setFilterRun(e.target.value)}>
          <option value="all">All Runs</option>
          {runs.map(r => (
            <option key={r} value={r}>{r}</option>
          ))}
        </select>

        <span className="text-xs text-muted" style={{ marginLeft: 'auto' }}>
          {filtered.length} of {anomalies.length} anomalies
        </span>
      </div>

      {/* Table */}
      <div className="data-table-container">
        {filtered.length > 0 ? (
          <table className="data-table">
            <thead>
              <tr>
                <th>Anomaly ID</th>
                <th>Type</th>
                <th>Severity</th>
                <th>Entity</th>
                <th>Impact</th>
                <th>Assigned Agent</th>
                <th>Run</th>
              </tr>
            </thead>
            <tbody>
              {filtered
                .sort((a, b) => (SEVERITY_ORDER[a.parsed.severity] || 9) - (SEVERITY_ORDER[b.parsed.severity] || 9))
                .map(a => (
                <>
                  <tr
                    key={a.anomaly_id || a.id}
                    onClick={() => setExpandedId(
                      expandedId === (a.anomaly_id || a.id) ? null : (a.anomaly_id || a.id)
                    )}
                    style={{ cursor: 'pointer' }}
                  >
                    <td className="cell-mono">{a.anomaly_id || '—'}</td>
                    <td>
                      <span className="badge-type">
                        {(a.parsed.anomaly_type || '').replace(/_/g, ' ')}
                      </span>
                    </td>
                    <td>
                      <span className={`badge ${severityClass(a.parsed.severity)}`}>
                        {a.parsed.severity || '—'}
                      </span>
                    </td>
                    <td style={{ maxWidth: '200px', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {a.parsed.affected_entity || '—'}
                    </td>
                    <td className="cell-amount">
                      {fmt(a.parsed.financial_impact_usd)}
                    </td>
                    <td className="text-muted">
                      {a.parsed.assigned_agent || '—'}
                    </td>
                    <td className="cell-mono">{a.run_id || '—'}</td>
                  </tr>
                  {expandedId === (a.anomaly_id || a.id) && (
                    <tr key={`${a.anomaly_id || a.id}-detail`}>
                      <td colSpan={7} style={{ padding: '0 16px 12px' }}>
                        <div className="expandable-content">
                          <p style={{ marginBottom: '8px', color: 'var(--text-secondary)', fontSize: '0.82rem' }}>
                            {a.parsed.description || 'No description available.'}
                          </p>
                          {a.parsed.evidence && (
                            <pre>{JSON.stringify(a.parsed.evidence, null, 2)}</pre>
                          )}
                        </div>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty-state">
            <div className="empty-icon">⚡</div>
            <p>No anomalies match the current filters.</p>
          </div>
        )}
      </div>
    </div>
  );
}
