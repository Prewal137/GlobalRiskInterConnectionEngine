import React, { useRef } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import styled from 'styled-components';

const GraphContainer = styled.div`
  width: 100%;
  height: ${(props) => props.height || '500px'};
  background: #0a0e27;
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
`;

const getRiskColor = (risk) => {
  if (risk < 0.3) return '#4CAF50'; // Green
  if (risk < 0.6) return '#FFC107'; // Yellow
  return '#F44336'; // Red
};

const RiskGraph = ({ riskData, cascadeHistory, height }) => {
  const graphRef = useRef();

  const sectors = Object.keys(riskData || {});
  
  if (sectors.length === 0) {
    return <GraphContainer height={height}><div style={{ color: 'white', padding: '2rem' }}>No data available</div></GraphContainer>;
  }

  // Create nodes
  const nodes = sectors.map((sector) => ({
    id: sector,
    name: sector.charAt(0).toUpperCase() + sector.slice(1),
    risk: riskData[sector],
    color: getRiskColor(riskData[sector]),
  }));

  // Create edges (fully connected for interconnection)
  const links = [];
  sectors.forEach((source) => {
    sectors.forEach((target) => {
      if (source !== target) {
        links.push({
          source,
          target,
          value: 0.5, // Default weight
        });
      }
    });
  });

  const graphData = { nodes, links };

  return (
    <GraphContainer height={height}>
      <ForceGraph2D
        ref={graphRef}
        graphData={graphData}
        nodeLabel="name"
        nodeColor="color"
        nodeRelSize={8}
        linkWidth={2}
        linkColor={() => 'rgba(255,255,255,0.2)'}
        backgroundColor="#0a0e27"
        nodeCanvasObject={(node, ctx, globalScale) => {
          const label = node.name;
          const fontSize = 12 / globalScale;
          ctx.font = `${fontSize}px Sans-Serif`;
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          ctx.fillStyle = node.color;
          ctx.beginPath();
          ctx.arc(node.x, node.y, 15, 0, 2 * Math.PI, false);
          ctx.fill();
          ctx.fillStyle = 'white';
          ctx.fillText(label, node.x, node.y + 25);
          ctx.fillText(`Risk: ${node.risk.toFixed(2)}`, node.x, node.y + 40);
        }}
      />
    </GraphContainer>
  );
};

export default RiskGraph;
