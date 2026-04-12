import React from 'react';
import styled from 'styled-components';

const CardContainer = styled.div`
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  border-radius: 12px;
  padding: 1.5rem;
  color: white;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
  transition: transform 0.2s;

  &:hover {
    transform: translateY(-5px);
  }
`;

const SectorName = styled.h3`
  margin: 0 0 0.5rem 0;
  font-size: 1.1rem;
  text-transform: capitalize;
`;

const RiskValue = styled.div`
  font-size: 2.5rem;
  font-weight: bold;
  margin: 0.5rem 0;
`;

const RiskBar = styled.div`
  width: 100%;
  height: 8px;
  background: rgba(255, 255, 255, 0.3);
  border-radius: 4px;
  overflow: hidden;
  margin-top: 0.5rem;
`;

const RiskFill = styled.div`
  width: ${(props) => props.$risk * 100}%;
  height: 100%;
  background: ${(props) => {
    if (props.$risk < 0.3) return '#4CAF50';
    if (props.$risk < 0.6) return '#FFC107';
    return '#F44336';
  }};
  transition: width 0.3s;
`;

const StatusBadge = styled.span`
  display: inline-block;
  padding: 0.25rem 0.75rem;
  background: ${(props) => {
    if (props.$risk < 0.3) return '#4CAF50';
    if (props.$risk < 0.6) return '#FFC107';
    return '#F44336';
  }};
  border-radius: 12px;
  font-size: 0.75rem;
  font-weight: bold;
  margin-top: 0.5rem;
`;

const RiskCard = ({ sector, risk, initialRisk }) => {
  const getStatus = (risk) => {
    if (risk < 0.3) return '🟢 LOW';
    if (risk < 0.6) return '🟡 MEDIUM';
    if (risk < 0.8) return '🟠 HIGH';
    return '🔴 CRITICAL';
  };

  const change = initialRisk ? risk - initialRisk : 0;

  return (
    <CardContainer>
      <SectorName>{sector}</SectorName>
      <RiskValue>{risk.toFixed(2)}</RiskValue>
      <RiskBar>
        <RiskFill $risk={risk} />
      </RiskBar>
      <StatusBadge $risk={risk}>{getStatus(risk)}</StatusBadge>
      {initialRisk && (
        <div style={{ marginTop: '0.5rem', fontSize: '0.875rem' }}>
          {change > 0 ? '↑' : change < 0 ? '↓' : '→'} {change >= 0 ? '+' : ''}
          {change.toFixed(2)}
        </div>
      )}
    </CardContainer>
  );
};

export default RiskCard;
