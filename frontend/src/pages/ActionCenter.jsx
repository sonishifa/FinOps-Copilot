import { useEffect, useState, useCallback } from 'react';
import { supabase } from '../supabase';

export default function ActionCenter() {
  const [pendingActions, setPendingActions] = useState([]);
  const [executedResults, setExecutedResults] = useState([]);
  const [loading, setLoading] = useState(true);
  const [noteValues, setNoteValues] = useState({});
  const [processingIds, setProcessingIds] = useState(new Set());

  const fetchData = useCallback(async () => {
    setLoading(true);

    // Fetch action recommendations (pending_human) from audit_events
    const { data: actionEvents } = await supabase
      .from('audit_events')
      .select('*')
      .in('event_type', ['action_recommended', 'escalation_sent'])
      .order('timestamp', { ascending: false })
      .limit(100);

    // Fetch existing approvals to know which are already decided
    const { data: approvals } = await supabase
      .from('action_approvals')
      .select('*');

    const approvalMap = {};
    (approvals || []).forEach(a => {
      approvalMap[a.action_id] = a;
    });

    // Fetch execution results
    const { data: execResults } = await supabase
      .from('execution_results')
      .select('*')
      .order('executed_at', { ascending: false })
      .limit(100);

    // Parse pending actions from escalation events
    const pending = [];
    const seen = new Set();
    (actionEvents || []).forEach(ev => {
      const payload = typeof ev.payload === 'string' ? JSON.parse(ev.payload) : (ev.payload || {});
      const actionId = ev.action_id || payload.action_id;

      if (!actionId || seen.has(actionId)) return;
      seen.add(actionId);

      // Skip if already approved/rejected
      if (approvalMap[actionId]) return;

      // Only show pending_human (escalation events)
      if (ev.event_type === 'escalation_sent' || payload.status === 'pending_human') {
        pending.push({
          action_id: actionId,
          run_id: ev.run_id,
          title: payload.title || payload.subject || `Action ${actionId}`,
          description: payload.description || payload.body || '',
          action_type: payload.action_type || '—',
          expected_savings_usd: payload.savings_usd || payload.expected_savings_usd || 0,
          implementation_steps: payload.implementation_steps || [],
          severity: ev.severity || 'warning',
          anomaly_id: payload.anomaly_id || '—',
          recipient: payload.recipient || '—',
        });
      }
    });

    setPendingActions(pending);
    setExecutedResults(execResults || []);
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleDecision = async (actionId, runId, status) => {
    setProcessingIds(prev => new Set(prev).add(actionId));

    try {
      await supabase.from('action_approvals').insert({
        run_id: runId,
        action_id: actionId,
        status: status,
        approved_by: 'enterprise-manager',
        notes: noteValues[actionId] || null,
      });

      // Remove from pending list
      setPendingActions(prev => prev.filter(a => a.action_id !== actionId));
    } catch (err) {
      console.error('Approval failed:', err);
    }

    setProcessingIds(prev => {
      const next = new Set(prev);
      next.delete(actionId);
      return next;
    });
  };

  const fmt = (n) => {
    if (n == null || n === 0) return '—';
    return '$' + Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
  };

  const outcomeClass = (outcome) => {
    if (outcome === 'success') return 'badge-success';
    if (outcome === 'staged_for_approval') return 'badge-warning';
    if (outcome === 'failed') return 'badge-danger';
    return 'badge-info';
  };

  if (loading) return <div className="loading-state">Loading action center...</div>;

  return (
    <div>
      <div className="page-header">
        <h1>Action Center</h1>
        <p>Review and approve recommended actions from the agentic pipeline</p>
      </div>

      {/* Pending section */}
      <div className="section-panel">
        <div className="panel-title">
          Pending Human Approval
          <span className="count">{pendingActions.length}</span>
        </div>

        {pendingActions.length > 0 ? (
          pendingActions.map(action => (
            <div className="approval-card" key={action.action_id}>
              <div className="approval-card-header">
                <h4>{action.title}</h4>
                <span className="badge badge-warning">PENDING</span>
              </div>

              <div className="card-meta">
                <span className="meta-item">
                  ID: <strong className="text-mono">{action.action_id}</strong>
                </span>
                <span className="meta-item">
                  Type: <strong>{action.action_type.replace(/_/g, ' ')}</strong>
                </span>
                <span className="meta-item">
                  Savings: <strong style={{ color: 'var(--color-success)' }}>
                    {fmt(action.expected_savings_usd)}
                  </strong>
                </span>
                <span className="meta-item">
                  Escalated to: <strong>{action.recipient}</strong>
                </span>
              </div>

              {action.description && (
                <div className="card-description">{action.description}</div>
              )}

              {action.implementation_steps && action.implementation_steps.length > 0 && (
                <div className="card-steps">
                  <h5>Implementation Steps</h5>
                  <ol>
                    {action.implementation_steps.map((step, i) => (
                      <li key={i}>{step}</li>
                    ))}
                  </ol>
                </div>
              )}

              <div className="card-actions">
                <input
                  className="notes-input"
                  type="text"
                  placeholder="Add approval notes (optional)..."
                  value={noteValues[action.action_id] || ''}
                  onChange={e => setNoteValues(prev => ({
                    ...prev, [action.action_id]: e.target.value
                  }))}
                />
                <button
                  className="btn btn-success"
                  disabled={processingIds.has(action.action_id)}
                  onClick={() => handleDecision(action.action_id, action.run_id, 'approved')}
                >
                  ✓ Approve
                </button>
                <button
                  className="btn btn-danger"
                  disabled={processingIds.has(action.action_id)}
                  onClick={() => handleDecision(action.action_id, action.run_id, 'rejected')}
                >
                  ✕ Reject
                </button>
              </div>
            </div>
          ))
        ) : (
          <div className="empty-state">
            <div className="empty-icon">☰</div>
            <p>No actions pending approval.</p>
          </div>
        )}
      </div>

      {/* Executed section */}
      <div className="section-panel">
        <div className="panel-title">
          Execution Results
          <span className="count">{executedResults.length}</span>
        </div>

        {executedResults.length > 0 ? (
          <div className="data-table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Action ID</th>
                  <th>Outcome</th>
                  <th>Details</th>
                  <th>Rollback</th>
                  <th>Notified</th>
                  <th>Escalated</th>
                  <th>Run</th>
                </tr>
              </thead>
              <tbody>
                {executedResults.map(r => (
                  <tr key={r.id}>
                    <td className="cell-mono">{r.action_id}</td>
                    <td>
                      <span className={`badge ${outcomeClass(r.outcome)}`}>
                        {r.outcome}
                      </span>
                    </td>
                    <td style={{
                      maxWidth: '300px',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    }}>
                      {r.details || '—'}
                    </td>
                    <td>{r.rollback_available ? '✓' : '—'}</td>
                    <td>{r.stakeholder_notified ? '✓' : '—'}</td>
                    <td>{r.escalation_brief_sent ? '✓' : '—'}</td>
                    <td className="cell-mono">{r.run_id}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="empty-state">
            <div className="empty-icon">↻</div>
            <p>No execution results yet.</p>
          </div>
        )}
      </div>
    </div>
  );
}
