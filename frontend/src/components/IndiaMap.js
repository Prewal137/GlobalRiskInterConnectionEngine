import React, { useState } from 'react';
import { ComposableMap, Geographies, Geography, Marker } from 'react-simple-maps';
import styled from 'styled-components';

const MapContainer = styled.div`
  width: 100%;
  background: #1a1a2e;
  border-radius: 12px;
  padding: 1rem;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
`;

const Tooltip = styled.div`
  position: absolute;
  background: rgba(0, 0, 0, 0.9);
  color: white;
  padding: 0.5rem 1rem;
  border-radius: 8px;
  font-size: 0.875rem;
  pointer-events: none;
  z-index: 1000;
  top: ${(props) => props.$top}px;
  left: ${(props) => props.$left}px;
`;

const getRiskColor = (risk) => {
  if (risk < 0.3) return '#4CAF50';
  if (risk < 0.6) return '#FFC107';
  return '#F44336';
};

// Sample India state coordinates
const stateCoordinates = {
  'Andhra Pradesh': [80.5432, 15.9129],
  'Karnataka': [76.8273, 14.7843],
  'Tamil Nadu': [78.6569, 11.1271],
  'Maharashtra': [75.7139, 19.7515],
  'Gujarat': [71.4460, 22.2587],
  'Rajasthan': [73.0243, 26.6139],
  'Delhi': [77.1025, 28.7041],
  'Uttar Pradesh': [80.9462, 26.8467],
  'Bihar': [85.3131, 25.0961],
  'West Bengal': [87.8410, 22.9868],
  'Kerala': [76.2711, 10.8505],
  'Punjab': [75.3412, 30.9412],
  'Haryana': [76.3040, 29.0588],
  'Madhya Pradesh': [78.3323, 23.8103],
  'Odisha': [84.8370, 20.9517],
  'Telangana': [79.2795, 17.1232],
};

const IndiaMap = ({ stateRiskData, onStateClick }) => {
  const [tooltip, setTooltip] = useState({ visible: false, content: '', x: 0, y: 0 });

  const handleMouseEnter = (state) => (event) => {
    const risk = stateRiskData?.[state] || Math.random();
    setTooltip({
      visible: true,
      content: `${state}\nRisk: ${risk.toFixed(2)}`,
      x: event.clientX + 10,
      y: event.clientY - 30,
    });
  };

  const handleMouseLeave = () => {
    setTooltip({ visible: false, content: '', x: 0, y: 0 });
  };

  const handleClick = (state) => () => {
    if (onStateClick) {
      onStateClick(state);
    }
  };

  return (
    <MapContainer>
      <h3 style={{ color: 'white', marginBottom: '1rem' }}>🗺️ India Risk Map</h3>
      <ComposableMap
        projection="geoMercator"
        projectionConfig={{
          scale: 1000,
          center: [78.9629, 22.5937],
        }}
        style={{ width: '100%', height: '500px' }}
      >
        <Geographies geography="/india-states.json">
          {({ geographies }) =>
            geographies.map((geo) => {
              const stateName = geo.properties.name;
              const risk = stateRiskData?.[stateName] || Math.random() * 0.5;
              return (
                <Geography
                  key={geo.rsmKey}
                  geography={geo}
                  fill={getRiskColor(risk)}
                  stroke="#1a1a2e"
                  strokeWidth={0.5}
                  style={{
                    default: { outline: 'none' },
                    hover: { fill: '#ffffff', outline: 'none' },
                    pressed: { outline: 'none' },
                  }}
                  onMouseEnter={handleMouseEnter(stateName)}
                  onMouseLeave={handleMouseLeave}
                  onClick={handleClick(stateName)}
                />
              );
            })
          }
        </Geographies>
        {Object.entries(stateCoordinates).map(([state, coords]) => (
          <Marker key={state} coordinates={coords}>
            <circle
              r={4}
              fill="white"
              stroke="#1a1a2e"
              strokeWidth={2}
              onClick={handleClick(state)}
              style={{ cursor: 'pointer' }}
            />
          </Marker>
        ))}
      </ComposableMap>
      {tooltip.visible && (
        <Tooltip $top={tooltip.y} $left={tooltip.x}>
          {tooltip.content}
        </Tooltip>
      )}
    </MapContainer>
  );
};

export default IndiaMap;
