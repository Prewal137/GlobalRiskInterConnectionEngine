import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Navbar from './components/Navbar';
import LiveDashboard from './pages/LiveDashboard';
import HistoricalPage from './pages/HistoricalPage';
import StateAnalysis from './pages/StateAnalysis';
import WhatIfPage from './pages/WhatIfPage';
import SystemAudit from './pages/SystemAudit';

function App() {
  return (
    <Router future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <div className="min-h-screen bg-[#050505] text-white">
        <Navbar />
        <main className="mx-auto max-w-7xl px-6 py-8">
          <Routes>
            <Route path="/" element={<LiveDashboard />} />
            <Route path="/audit" element={<SystemAudit />} />
            <Route path="/history" element={<HistoricalPage />} />
            <Route path="/state" element={<StateAnalysis />} />
            <Route path="/whatif" element={<WhatIfPage />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}


export default App;
