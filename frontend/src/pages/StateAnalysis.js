import React, { useState } from 'react';
import styled from 'styled-components';
import { getStateRisk, getStateImpact } from '../services/api';
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

const DetailsPanel = styled.div`
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  border-radius: 12px;
  padding: 1.5rem;
  margin-top: 1rem;
`;

const MetricCard = styled.div`
  background: rgba(255, 255, 255, 0.1);
  border-radius: 8px;
  padding: 1rem;
  margin-bottom: 0.5rem;
  
  h4 {
    margin: 0 0 0.5rem 0;
    font-size: 0.875rem;
    opacity: 0.8;
  }
  
  p {
    margin: 0;
    font-size: 1.5rem;
    font-weight: bold;
  }
`;

const LoadingSpinner = styled.div`
  text-align: center;
  padding: 3rem;
  font-size: 1.5rem;
  color: #667eea;
`;

const StateAnalysis = () => {
  const [selectedState, setSelectedState] = useState('Karnataka');
  const [stateData, setStateData] = useState(null);
  const [impactData, setImpactData] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleStateClick = async (stateName) => {
    setSelectedState(stateName);
    setLoading(true);
    
    try {
      const [stateDataResult, impactDataResult] = await Promise.all([
        getStateRisk(stateName),
        getStateImpact(stateName),
      ]);
      
      setStateData(stateDataResult);
      setImpactData(impactDataResult);
    } catch (err) {
      console.error('Error fetching state data:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <PageContainer>
      <Header>
        <h1>🗺️ State-Level Risk Analysis</h1>
        <p>Spatial + Network combined analysis for Indian states</p>
      </Header>

      <ContentGrid>
        <Section>
          <h2>📍 Select State</h2>
          <IndiaMap
            stateRiskData={{}}
            onStateClick={handleStateClick}
          />
          <p style={{ textAlign: 'center', marginTop: '1rem', fontSize: '1.2rem' }}>
            Currently viewing: <strong>{selectedState}</strong>
          </p>
        </Section>

        <Section>
          <h2>📊 State Impact Graph</h2>
          {impactData ? (
            <RiskGraph riskData={impactData.final || {}} height="400px" />
          ) : (
            <p>Select a state to see impact analysis</p>
          )}
        </Section>
      </ContentGrid>

      {loading && <LoadingSpinner>🔄 Loading state data...</LoadingSpinner>}

      {stateData && (
        <Section>
          <h2>📋 State Details: {selectedState}</h2>
          {stateData.data && stateData.data.length > 0 ? (
            <div>
              {Object.entries(stateData.data[0]).map(([key, value]) => (
                <div key={key} style={{ marginBottom: '0.5rem' }}>
                  <strong>{key}:</strong> {typeof value === 'number' ? value.toFixed(2) : value}
                </div>
              ))}
            </div>
          ) : (
            <p>No detailed data available</p>
          )}
        </Section>
      )}

      {impactData && (
        <DetailsPanel>
          <h2 style={{ marginBottom: '1rem' }}>🔥 Cascade Impact Analysis</h2>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1rem' }}>
            {impactData.final &&
              Object.entries(impactData.final).map(([sector, risk]) => {
                const initial = impactData.initial?.[sector] || 0;
                const change = risk - initial;
                return (
                  <MetricCard key={sector}>
                    <h4>{sector.toUpperCase()}</h4>
                    <p>{risk.toFixed(2)}</p>
                    <small>
                      Change: {change >= 0 ? '+' : ''}
                      {change.toFixed(2)}
                    </small>
                  </MetricCard>
                );
              })}
          </div>
        </DetailsPanel>
      )}
    </PageContainer>
  );
};

export default StateAnalysis;
