import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { getLiveRisk } from '../services/api';
import RiskCard from '../components/RiskCard';
import RiskGraph from '../components/RiskGraph';
import IndiaMap from '../components/IndiaMap';

const PageContainer = styled.div`
  max-width: 1400px;
  margin: 0 auto;
  padding: 2rem;
`;

const Header = styled.div`
  text-align: center;
  margin-bottom: 2rem;
  
  h1 {
    font-size: 2.5rem;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 0.5rem;
  }
  
  p {
    color: #666;
    font-size: 1.1rem;
  }
`;

const CardsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
  margin-bottom: 2rem;
`;

const ContentGrid = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 2rem;
  margin-bottom: 2rem;
`;

const Section = styled.div`
  background: white;
  border-radius: 12px;
  padding: 1.5rem;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  
  h2 {
    margin-bottom: 1rem;
    color: #333;
  }
`;

const CascadeTable = styled.table`
  width: 100%;
  border-collapse: collapse;
  
  th, td {
    padding: 0.75rem;
    text-align: left;
    border-bottom: 1px solid #eee;
  }
  
  th {
    background: #f5f5f5;
    font-weight: bold;
    color: #333;
  }
  
  tr:hover {
    background: #f9f9f9;
  }
`;

const LoadingSpinner = styled.div`
  text-align: center;
  padding: 3rem;
  font-size: 1.5rem;
  color: #667eea;
`;

const LiveDashboard = () => {
  const [loading, setLoading] = useState(true);
  const [initialRisk, setInitialRisk] = useState(null);
  const [finalRisk, setFinalRisk] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchRiskData();
  }, []);

  const fetchRiskData = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Fetch live risk data from backend
      const response = await getLiveRisk();
      
      setInitialRisk(response.initial_risk);
      setFinalRisk(response.final_risk);
      
      setLoading(false);
    } catch (err) {
      console.error('Error fetching risk data:', err);
      setError('Failed to load risk data. Please ensure the backend server is running.');
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <PageContainer>
        <LoadingSpinner>🔄 Loading live risk data...</LoadingSpinner>
      </PageContainer>
    );
  }

  if (error) {
    return (
      <PageContainer>
        <div style={{ textAlign: 'center', padding: '3rem', color: 'red' }}>
          <h2>⚠️ Error</h2>
          <p>{error}</p>
          <button onClick={fetchRiskData} style={{ marginTop: '1rem', padding: '0.5rem 1rem' }}>
            Retry
          </button>
        </div>
      </PageContainer>
    );
  }

  return (
    <PageContainer>
      <Header>
        <h1>🌍 Live Global Risk Dashboard</h1>
        <p>Real-time multi-sector risk assessment with cascading effects</p>
      </Header>

      {/* Risk Cards Row */}
      <CardsGrid>
        {finalRisk &&
          Object.entries(finalRisk).map(([sector, risk]) => (
            <RiskCard
              key={sector}
              sector={sector}
              risk={risk}
              initialRisk={initialRisk?.[sector]}
            />
          ))}
      </CardsGrid>

      {/* Graph and Map */}
      <ContentGrid>
        <Section>
          <h2>📊 Risk Interconnection Graph</h2>
          <RiskGraph riskData={finalRisk} height="400px" />
        </Section>

        <Section>
          <h2>🗺️ India Risk Map</h2>
          <IndiaMap stateRiskData={{}} />
        </Section>
      </ContentGrid>

      {/* Cascade Results */}
      <Section>
        <h2>📈 Cascade Simulation Results</h2>
        <CascadeTable>
          <thead>
            <tr>
              <th>Sector</th>
              <th>Initial Risk</th>
              <th>Final Risk</th>
              <th>Change</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {initialRisk &&
              Object.entries(initialRisk).map(([sector, initial]) => {
                const final = finalRisk[sector];
                const change = final - initial;
                return (
                  <tr key={sector}>
                    <td style={{ textTransform: 'capitalize' }}>{sector}</td>
                    <td>{initial.toFixed(4)}</td>
                    <td>{final.toFixed(4)}</td>
                    <td style={{ color: change > 0 ? 'red' : change < 0 ? 'green' : 'black' }}>
                      {change > 0 ? '↑' : change < 0 ? '↓' : '→'} {change >= 0 ? '+' : ''}
                      {change.toFixed(4)}
                    </td>
                    <td>
                      {final < 0.3
                        ? '🟢 Low'
                        : final < 0.6
                        ? '🟡 Medium'
                        : final < 0.8
                        ? '🟠 High'
                        : '🔴 Critical'}
                    </td>
                  </tr>
                );
              })}
          </tbody>
        </CascadeTable>
      </Section>
    </PageContainer>
  );
};

export default LiveDashboard;
