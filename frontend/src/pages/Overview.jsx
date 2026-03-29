import { useEffect, useState } from 'react';
import { supabase } from '../supabase';

export default function Overview() {
  const [runs, setRuns] = useState([]);
  const [anomalyCounts, setAnomalyCounts] = useState({});
  const [loading, setLoading] = useState(true);
  const [latestRun, setLatestRun] = useState(null);

  useEffect(() => {
    async function fetchData() {
      setLoading(true);

      // Fetch pipeline runs
      const { data: runData } = await supabase
        .from('pipeline_runs')
        .select('*')
        .order('run_id', { ascending: false })
        .limit(10);

      if (runData && runData.length > 0) {
        setRuns(runData);
        // Pick the run with the highest exposure (most meaningful data)
        const best = runData.reduce((best, r) =>
          (r.total_financial_exposure_usd || 0) > (best.total_financial_exposure_usd || 0)
            ? r : best
        , runData[0]);
        setLatestRun(best);
      }

      // Fetch anomaly type distribution
      const { data: events } = await supabase
        .from('audit_events')
        .select('payload')
        .eq('event_type', 'anomaly_detected');

      if (events) {
        const counts = {};
        events.forEach(ev => {
          const payload = typeof ev.payload === 'string'
            ? JSON.parse(ev.payload)
            : ev.payload;
          const type = payload?.anomaly_type || 'unknown';
          counts[type] = (counts[type] || 0) + 1;
        });
        setAnomalyCounts(counts);
      }

      setLoading(false);
    }
    fetchData();
  }, []);

  const fmt = (n) => {
    if (n == null) return '—';
    return '$' + Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
  };

  if (loading) return <div className="loading-state">Loading dashboard...</div>;

  const totalAnomalies = Object.values(anomalyCounts).reduce((s, n) => s + n, 0);

  return (
    <div>
      <div className="page-header">
        <h1>Overview</h1>
        <p>FinOps Copilot — agentic cost intelligence pipeline</p>
      </div>

      {/* Metrics row */}
      <div className="metrics-row">
        <div className="metric-card">
          <div className="metric-label">Total Exposure</div>
          <div className="metric-value danger">
            {fmt(latestRun?.total_financial_exposure_usd)}
          </div>
          <div className="metric-sub">Financial risk detected</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Recoverable Savings</div>
          <div className="metric-value positive">
            {fmt(latestRun?.total_recoverable_savings_usd)}
          </div>
          <div className="metric-sub">Actions recommended</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Auto-Executed</div>
          <div className="metric-value positive">
            {fmt(latestRun?.auto_executed_savings_usd)}
          </div>
          <div className="metric-sub">Savings realized</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Pending Approval</div>
          <div className="metric-value warning">
            {fmt(latestRun?.pending_human_approval_savings_usd)}
          </div>
          <div className="metric-sub">Awaiting human decision</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">ROI</div>
          <div className="metric-value">
            {latestRun?.roi_multiple != null ? `${latestRun.roi_multiple}%` : '—'}
          </div>
          <div className="metric-sub">Exposure recovered</div>
        </div>
      </div>

      {/* Anomaly breakdown */}
      <div className="data-table-container">
        <div className="data-table-header">
          <h3>Anomaly Type Distribution</h3>
          <span className="badge badge-info">{totalAnomalies} total</span>
        </div>
        {totalAnomalies > 0 ? (
          <table className="data-table">
            <thead>
              <tr>
                <th>Type</th>
                <th>Count</th>
                <th>Distribution</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(anomalyCounts)
                .sort((a, b) => b[1] - a[1])
                .map(([type, count]) => (
                  <tr key={type}>
                    <td>
                      <span className="badge-type">
                        {type.replace(/_/g, ' ')}
                      </span>
                    </td>
                    <td className="cell-amount">{count}</td>
                    <td>
                      <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px'
                      }}>
                        <div style={{
                          height: '6px',
                          width: `${Math.max((count / totalAnomalies) * 200, 8)}px`,
                          background: 'var(--accent)',
                          borderRadius: '3px',
                        }} />
                        <span className="text-xs text-muted">
                          {((count / totalAnomalies) * 100).toFixed(0)}%
                        </span>
                      </div>
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        ) : (
          <div className="empty-state">
            <div className="empty-icon">◈</div>
            <p>No anomalies detected yet. Run the pipeline first.</p>
          </div>
        )}
      </div>

      {/* Recent runs */}
      <div className="data-table-container mt-4">
        <div className="data-table-header">
          <h3>Recent Pipeline Runs</h3>
        </div>
        {runs.length > 0 ? (
          <table className="data-table">
            <thead>
              <tr>
                <th>Run ID</th>
                <th>Exposure</th>
                <th>Recoverable</th>
                <th>Auto-Executed</th>
                <th>Pending</th>
                <th>ROI</th>
              </tr>
            </thead>
            <tbody>
              {runs.map(run => (
                <tr key={run.run_id}>
                  <td className="cell-mono">{run.run_id}</td>
                  <td className="cell-amount">{fmt(run.total_financial_exposure_usd)}</td>
                  <td className="cell-amount" style={{ color: 'var(--color-success)' }}>
                    {fmt(run.total_recoverable_savings_usd)}
                  </td>
                  <td className="cell-amount">{fmt(run.auto_executed_savings_usd)}</td>
                  <td className="cell-amount" style={{ color: 'var(--color-warning)' }}>
                    {fmt(run.pending_human_approval_savings_usd)}
                  </td>
                  <td>{run.roi_multiple != null ? `${run.roi_multiple}%` : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty-state">
            <div className="empty-icon">↻</div>
            <p>No pipeline runs yet. Run <code>python3 main.py</code> to start.</p>
          </div>
        )}
      </div>
    </div>
  );
}
