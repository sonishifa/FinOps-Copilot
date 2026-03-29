import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Sidebar from './components/Sidebar';
import Overview from './pages/Overview';
import Anomalies from './pages/Anomalies';
import ActionCenter from './pages/ActionCenter';
import AuditTrail from './pages/AuditTrail';
import PipelineRuns from './pages/PipelineRuns';

export default function App() {
  return (
    <BrowserRouter>
      <div className="app-layout">
        <Sidebar />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/anomalies" element={<Anomalies />} />
            <Route path="/actions" element={<ActionCenter />} />
            <Route path="/audit" element={<AuditTrail />} />
            <Route path="/runs" element={<PipelineRuns />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
