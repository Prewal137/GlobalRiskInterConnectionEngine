import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Navbar from './components/Navbar';
import LiveDashboard from './pages/LiveDashboard';
import HistoricalPage from './pages/HistoricalPage';
import StateAnalysis from './pages/StateAnalysis';
import WhatIfPage from './pages/WhatIfPage';

function App() {
  return (
    <Router future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <div className="App">
        <Navbar />
        <Routes>
          <Route path="/" element={<LiveDashboard />} />
          <Route path="/history" element={<HistoricalPage />} />
          <Route path="/state" element={<StateAnalysis />} />
          <Route path="/whatif" element={<WhatIfPage />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
