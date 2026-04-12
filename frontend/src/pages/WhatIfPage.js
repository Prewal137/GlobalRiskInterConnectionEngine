import React, { useState } from 'react';
import styled from 'styled-components';
import { runWhatIfSimulation } from '../services/api';
import RiskGraph from '../components/RiskGraph';
import IndiaMap from '../components/IndiaMap';
import RiskCard from '../components/RiskCard';

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

const ControlPanel = styled.div`
  background: white;
  border-radius: 12px;
  padding: 2rem;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  margin-bottom: 2rem;
`;

const SliderContainer = styled.div`
  margin-bottom: 1.5rem;
`;

const SliderLabel = styled.label`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.5rem;
  font-weight: bold;
  text-transform: capitalize;
  color: #333;
`;

const Slider = styled.input`
  width: 100%;
  height: 8px;
  border-radius: 4px;
  background: linear-gradient(to right, #4CAF50, #FFC107, #F44336);
  outline: none;
  -webkit-appearance: none;

  &::-webkit-slider-thumb {
    -webkit-appearance: none;
    appearance: none;
    width: 24px;
    height: 24px;
    border-radius: 50%;
    background: #667eea;
    cursor: pointer;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
  }

  &::-moz-range-thumb {
    width: 24px;
    height: 24px;
    border-radius: 50%;
    background: #667eea;
    cursor: pointer;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
  }
`;

const RunButton = styled.button`
  width: 100%;
  padding: 1rem;
  font-size: 1.2rem;
  font-weight: bold;
  border: none;
  border-radius: 8px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  cursor: pointer;
  transition: transform 0.2s;

  &:hover {
    transform: scale(1.02);
  }

  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`;

const ResultsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
  margin-top: 1rem;
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

const LoadingSpinner = styled.div`
  text-align: center;
  padding: 3rem;
  font-size: 1.5rem;
  color: #667eea;
`;

const WhatIfPage = () => {
  const [sliders, setSliders] = useState({
    climate: 0.3,
    economy: 0.3,
    trade: 0.3,
    geopolitics: 0.3,
    migration: 0.3,
    social: 0.3,
    infrastructure: 0.3,
  });

  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleSliderChange = (sector) => (e) => {
    setSliders((prev) => ({
      ...prev,
      [sector]: parseFloat(e.target.value),
    }));
  };

  const handleRun = async () => {
    setLoading(true);
    try {
      const response = await runWhatIfSimulation(sliders);
      setResults(response);
    } catch (err) {
      console.error('Error running simulation:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <PageContainer>
      <Header>
        <h1>⚙️ What-If Simulation</h1>
        <p>Change sector risk values and see system-wide cascading effects</p>
      </Header>

      <ControlPanel>
        <h2 style={{ marginBottom: '1.5rem' }}>🎛️ Input Controls</h2>
        {Object.entries(sliders).map(([sector, value]) => (
          <SliderContainer key={sector}>
            <SliderLabel>
              <span>{sector}</span>
              <span style={{ color: '#667eea', fontSize: '1.2rem' }}>
                {value.toFixed(2)}
              </span>
            </SliderLabel>
            <Slider
              type="range"
              min="0"
              max="1"
              step="0.01"
              value={value}
              onChange={handleSliderChange(sector)}
            />
          </SliderContainer>
        ))}
        <RunButton onClick={handleRun} disabled={loading}>
          {loading ? '🔄 Running Simulation...' : '🚀 Run Simulation'}
        </RunButton>
      </ControlPanel>

      {loading && <LoadingSpinner>🔄 Running cascade simulation...</LoadingSpinner>}

      {results && (
        <>
          <Section>
            <h2>📊 Updated Risk Graph</h2>
            <RiskGraph riskData={results.final} height="400px" />
          </Section>

          <Section>
            <h2>🗺️ Affected Regions</h2>
            <IndiaMap stateRiskData={{}} />
          </Section>

          <Section>
            <h2>📈 Before vs After Comparison</h2>
            <ResultsGrid>
              {Object.entries(results.final).map(([sector, finalRisk]) => {
                const initialRisk = results.initial?.[sector] || 0;
                return (
                  <RiskCard
                    key={sector}
                    sector={sector}
                    risk={finalRisk}
                    initialRisk={initialRisk}
                  />
                );
              })}
            </ResultsGrid>
          </Section>

          {results.steps && (
            <Section>
              <h2>🔥 Cascade Steps</h2>
              <p>Simulation ran for {results.total_steps} steps</p>
              <div style={{ marginTop: '1rem', maxHeight: '300px', overflowY: 'auto' }}>
                {results.steps.map((step, index) => (
                  <div
                    key={index}
                    style={{
                      padding: '0.5rem',
                      marginBottom: '0.5rem',
                      background: '#f5f5f5',
                      borderRadius: '8px',
                    }}
                  >
                    <strong>Step {index + 1}:</strong>
                    {Object.entries(step).map(([sector, risk]) => (
                      <span key={sector} style={{ marginLeft: '1rem' }}>
                        {sector}: {risk.toFixed(2)}
                      </span>
                    ))}
                  </div>
                ))}
              </div>
            </Section>
          )}
        </>
      )}
    </PageContainer>
  );
};

export default WhatIfPage;
