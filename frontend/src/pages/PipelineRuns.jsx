import { useEffect, useState } from 'react';
import { supabase } from '../supabase';

export default function PipelineRuns() {
  const [runs, setRuns] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedRun, setExpandedRun] = useState(null);
  const [runEvents, setRunEvents] = useState([]);
  const [runResults, setRunResults] = useState([]);
  const [loadingDetail, setLoadingDetail] = useState(false);

  useEffect(() => {
    async function fetchRuns() {
      setLoading(true);
      const { data } = await supabase
        .from('pipeline_runs')
        .select('*')
        .order('run_id', { ascending: false });

      setRuns(data || []);
      setLoading(false);
    }
    fetchRuns();
  }, []);

  const handleExpand = async (runId) => {
    if (expandedRun === runId) {
      setExpandedRun(null);
      return;
    }

    setExpandedRun(runId);
    setLoadingDetail(true);

    // Fetch events + results for this run
    const [eventsRes, resultsRes] = await Promise.all([
      supabase
        .from('audit_events')
        .select('*')
        .eq('run_id', runId)
        .order('timestamp', { ascending: true })
        .limit(50),
      supabase
        .from('execution_results')
        .select('*')
        .eq('run_id', runId),
    ]);

    setRunEvents(eventsRes.data || []);
    setRunResults(resultsRes.data || []);
    setLoadingDetail(false);
  };

  const fmt = (n) => {
    if (n == null) return '—';
    return '$' + Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
  };

  if (loading) return <div className="loading-state">Loading pipeline runs...</div>;

  return (
    <div>
      <div className="page-header">
        <h1>Pipeline Runs</h1>
        <p>Historical pipeline executions with drill-down into each run</p>
      </div>

      {runs.length > 0 ? (
        runs.map(run => (
          <div key={run.run_id} style={{ marginBottom: '12px' }}>
            <div
              className="approval-card"
              onClick={() => handleExpand(run.run_id)}
              style={{ cursor: 'pointer', marginBottom: 0 }}
            >
              <div className="approval-card-header">
                <h4 className="text-mono" style={{ fontSize: '0.85rem' }}>
                  {run.run_id}
                </h4>
                <span className="badge badge-success">COMPLETED</span>
              </div>

              <div className="card-meta">
                <span className="meta-item">
                  Exposure: <strong style={{ color: 'var(--color-danger)' }}>
                    {fmt(run.total_financial_exposure_usd)}
                  </strong>
                </span>
                <span className="meta-item">
                  Recoverable: <strong style={{ color: 'var(--color-success)' }}>
                    {fmt(run.total_recoverable_savings_usd)}
                  </strong>
                </span>
                <span className="meta-item">
                  Auto-executed: <strong>{fmt(run.auto_executed_savings_usd)}</strong>
                </span>
                <span className="meta-item">
                  Pending: <strong style={{ color: 'var(--color-warning)' }}>
                    {fmt(run.pending_human_approval_savings_usd)}
                  </strong>
                </span>
                <span className="meta-item">
                  ROI: <strong>{run.roi_multiple != null ? `${run.roi_multiple}%` : '—'}</strong>
                </span>
              </div>
            </div>

            {expandedRun === run.run_id && (
              <div style={{
                background: 'var(--bg-elevated)',
                border: '1px solid var(--border-subtle)',
                borderTop: 'none',
                borderRadius: '0 0 var(--radius-lg) var(--radius-lg)',
                padding: '16px',
              }}>
                {loadingDetail ? (
                  <div className="text-muted text-sm">Loading run details...</div>
                ) : (
                  <>
                    {/* Execution results */}
                    {runResults.length > 0 && (
                      <div style={{ marginBottom: '16px' }}>
                        <div className="panel-title" style={{ fontSize: '0.75rem' }}>
                          Execution Results <span className="count">{runResults.length}</span>
                        </div>
                        <table className="data-table" style={{ background: 'var(--bg-card)', borderRadius: 'var(--radius-md)' }}>
                          <thead>
                            <tr>
                              <th>Action ID</th>
                              <th>Outcome</th>
                              <th>Details</th>
                            </tr>
                          </thead>
                          <tbody>
                            {runResults.map(r => (
                              <tr key={r.id}>
                                <td className="cell-mono">{r.action_id}</td>
                                <td>
                                  <span className={`badge ${r.outcome === 'success' ? 'badge-success' : 'badge-warning'}`}>
                                    {r.outcome}
                                  </span>
                                </td>
                                <td style={{ maxWidth: '400px', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                                  {r.details}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}

                    {/* Event timeline summary */}
                    <div>
                      <div className="panel-title" style={{ fontSize: '0.75rem' }}>
                        Event Timeline <span className="count">{runEvents.length}</span>
                      </div>
                      <div style={{ maxHeight: '300px', overflow: 'auto' }}>
                        {runEvents.map(ev => (
                          <div key={ev.id} className="audit-event" style={{ marginBottom: '2px' }}>
                            <span className="event-time">
                              {ev.timestamp ? new Date(ev.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '—'}
                            </span>
                            <span className="event-agent">{ev.agent}</span>
                            <span className="event-type">{ev.event_type}</span>
                            <span className="event-detail">{ev.anomaly_id || ev.action_id || ''}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        ))
      ) : (
        <div className="empty-state">
          <div className="empty-icon">↻</div>
          <p>No pipeline runs recorded yet. Run <code>python3 main.py</code> to start.</p>
        </div>
      )}
    </div>
  );
}
