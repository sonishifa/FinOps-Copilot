import { NavLink } from 'react-router-dom';

export default function Sidebar() {
  const navItems = [
    { to: '/',          icon: '◈', label: 'Overview' },
    { to: '/anomalies', icon: '⚡', label: 'Anomalies' },
    { to: '/actions',   icon: '☰', label: 'Action Center' },
    { to: '/audit',     icon: '▤', label: 'Audit Trail' },
    { to: '/runs',      icon: '↻', label: 'Pipeline Runs' },
  ];

  return (
    <nav className="sidebar">
      <div className="sidebar-brand">
        <img src="/logo.png" alt="FinOps Copilot" className="brand-logo" />
        <div className="brand-text">
          FinOps Copilot
          <span>Agentic Cost Engine</span>
        </div>
      </div>
      <div className="sidebar-nav">
        {navItems.map(item => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === '/'}
            className={({ isActive }) =>
              `nav-item ${isActive ? 'active' : ''}`
            }
          >
            <span className="nav-icon">{item.icon}</span>
            {item.label}
          </NavLink>
        ))}
      </div>
      <div className="sidebar-footer">
        FinOps Copilot v1.0
      </div>
    </nav>
  );
}
