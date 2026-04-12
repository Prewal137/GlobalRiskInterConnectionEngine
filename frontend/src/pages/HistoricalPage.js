import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { getHistoricalRisk, getTrendData } from '../services/api';
import RiskGraph from '../components/RiskGraph';
import IndiaMap from '../components/IndiaMap';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

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

const Controls = styled.div`
  display: flex;
  justify-content: center;
  gap: 1rem;
  margin-bottom: 2rem;
  align-items: center;
`;

const Select = styled.select`
  padding: 0.75rem 1.5rem;
  font-size: 1rem;
  border-radius: 8px;
  border: 2px solid #667eea;
  background: white;
  cursor: pointer;
`;

const Button = styled.button`
  padding: 0.75rem 1.5rem;
  font-size: 1rem;
  border-radius: 8px;
  border: none;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  cursor: pointer;
  transition: transform 0.2s;

  &:hover {
    transform: scale(1.05);
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

const DataTable = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-top: 1rem;
  
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

const HistoricalPage = () => {
  const [loading, setLoading] = useState(true);
  const [selectedYear, setSelectedYear] = useState(2020);
  const [historicalData, setHistoricalData] = useState(null);
  const [trendData, setTrendData] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedYear]);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const [historical, trend] = await Promise.all([
        getHistoricalRisk(selectedYear),
        getTrendData(),
      ]);
      
      setHistoricalData(historical);
      setTrendData(trend.trend || []);
      setLoading(false);
    } catch (err) {
      console.error('Error fetching historical data:', err);
      setError('Failed to load historical data.');
      setLoading(false);
    }
  };

  const handleAnimate = () => {
    const years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];
    let index = years.indexOf(selectedYear);
    
    const interval = setInterval(() => {
      index++;
      if (index >= years.length) {
        clearInterval(interval);
        return;
      }
      setSelectedYear(years[index]);
    }, 1000);
  };

  if (loading) {
    return (
      <PageContainer>
        <LoadingSpinner>🔄 Loading historical data...</LoadingSpinner>
      </PageContainer>
    );
  }

  if (error) {
    return (
      <PageContainer>
        <div style={{ textAlign: 'center', padding: '3rem', color: 'red' }}>
          <h2>⚠️ Error</h2>
          <p>{error}</p>
        </div>
      </PageContainer>
    );
  }

  return (
    <PageContainer>
      <Header>
        <h1>📅 Historical Risk Analysis</h1>
        <p>Temporal risk evolution and trend analysis</p>
      </Header>

      <Controls>
        <label style={{ fontSize: '1.1rem', fontWeight: 'bold' }}>
          Select Year:
        </label>
        <Select
          value={selectedYear}
          onChange={(e) => setSelectedYear(parseInt(e.target.value))}
        >
          {[2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024].map((year) => (
            <option key={year} value={year}>
              {year}
            </option>
          ))}
        </Select>
        <Button onClick={handleAnimate}>▶️ Animate Timeline</Button>
      </Controls>

      <ContentGrid>
        <Section>
          <h2>📊 Historical Network Graph ({selectedYear})</h2>
          <RiskGraph riskData={{}} height="400px" />
        </Section>

        <Section>
          <h2>🗺️ Historical Risk Map ({selectedYear})</h2>
          <IndiaMap stateRiskData={{}} />
        </Section>
      </ContentGrid>

      <Section>
        <h2>📈 Risk Trend Over Time</h2>
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={trendData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="year" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="global_risk" stroke="#667eea" strokeWidth={2} />
            <Line type="monotone" dataKey="economic_risk" stroke="#F44336" strokeWidth={2} />
            <Line type="monotone" dataKey="social_risk" stroke="#FFC107" strokeWidth={2} />
          </LineChart>
        </ResponsiveContainer>
      </Section>

      {historicalData?.data && (
        <Section>
          <h2>📋 Historical Data Table ({selectedYear})</h2>
          <DataTable>
            <thead>
              <tr>
                <th>Year</th>
                <th>Month</th>
                <th>Climate</th>
                <th>Economy</th>
                <th>Trade</th>
                <th>Geopolitics</th>
                <th>Migration</th>
                <th>Social</th>
                <th>Infrastructure</th>
                <th>Global Risk</th>
              </tr>
            </thead>
            <tbody>
              {historicalData.data.slice(0, 10).map((row, index) => (
                <tr key={index}>
                  <td>{row.Year}</td>
                  <td>{row.Month}</td>
                  <td>{row.climate_risk?.toFixed(2)}</td>
                  <td>{row.economic_risk?.toFixed(2)}</td>
                  <td>{row.trade_risk?.toFixed(2)}</td>
                  <td>{row.geopolitical_risk?.toFixed(2)}</td>
                  <td>{row.migration_risk?.toFixed(2)}</td>
                  <td>{row.social_risk?.toFixed(2)}</td>
                  <td>{row.infrastructure_risk?.toFixed(2)}</td>
                  <td style={{ fontWeight: 'bold' }}>{row.global_risk?.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        </Section>
      )}
    </PageContainer>
  );
};

export default HistoricalPage;
