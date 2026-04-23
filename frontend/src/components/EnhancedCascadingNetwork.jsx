import React, { useRef, useMemo, useEffect, useState, useCallback } from "react";
import ForceGraph2D from "react-force-graph-2d";
import * as d3 from "d3-force";

const defaultGraph = {
  nodes: [
    { id: "Climate", name: "Climate", baseRisk: 0.95 },
    { id: "Economy", name: "Economy", baseRisk: 0.82 },
    { id: "Trade", name: "Trade", baseRisk: 0.75 },
    { id: "Social Stability", name: "Social Stability", baseRisk: 0.65 },
    { id: "Urban Infrastructure", name: "Urban Infra", baseRisk: 0.45 },
    { id: "Migration", name: "Migration", baseRisk: 0.55 },
  ],
  links: [
    { source: "Climate", target: "Economy" },
    { source: "Economy", target: "Trade" },
    { source: "Economy", target: "Social Stability" },
    { source: "Social Stability", target: "Migration" },
    { source: "Urban Infrastructure", target: "Social Stability" },
  ]
};

export default function EnhancedCascadingNetwork({ graphData = defaultGraph, riskScores = {} }) {
  const fgRef = useRef();
  const containerRef = useRef();
  const [dimensions, setDimensions] = useState({ width: 0, height: 600 });
  const [hoverNode, setHoverNode] = useState(null);
  const [highlightNodes, setHighlightNodes] = useState(new Set());
  const [highlightLinks, setHighlightLinks] = useState(new Set());

  // Handle Resize to ensure canvas fills container
  useEffect(() => {
    if (containerRef.current) {
      setDimensions({
        width: containerRef.current.offsetWidth,
        height: containerRef.current.offsetHeight
      });
    }
    
    const handleResize = () => {
      if (containerRef.current) {
        setDimensions({
          width: containerRef.current.offsetWidth,
          height: containerRef.current.offsetHeight
        });
      }
    };

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const processedData = useMemo(() => {
    return {
      nodes: graphData.nodes.map(node => ({
        ...node,
        score: riskScores[node.id]?.score || node.baseRisk || 0.5
      })),
      links: graphData.links.map(link => ({
        ...link,
        source: typeof link.source === 'object' ? link.source.id : link.source,
        target: typeof link.target === 'object' ? link.target.id : link.target
      }))
    };
  }, [graphData, riskScores]);

  // Center the graph once the simulation starts/stops
  const handleEngineStop = () => {
    if (fgRef.current) {
      fgRef.current.zoomToFit(400, 100);
    }
  };

  const handleNodeHover = (node) => {
    const nextHighlightNodes = new Set();
    const nextHighlightLinks = new Set();
    
    if (node) {
      nextHighlightNodes.add(node.id);
      processedData.links.forEach(link => {
        const s = link.source.id || link.source;
        const t = link.target.id || link.target;
        if (s === node.id || t === node.id) {
          nextHighlightLinks.add(link);
          nextHighlightNodes.add(s);
          nextHighlightNodes.add(t);
        }
      });
    }
    
    setHoverNode(node || null);
    setHighlightNodes(nextHighlightNodes);
    setHighlightLinks(nextHighlightLinks);
  };

  const drawNode = useCallback((node, ctx, globalScale) => {
    const isHighlighted = highlightNodes.has(node.id) || highlightNodes.size === 0;
    const alpha = isHighlighted ? 1 : 0.1;
    const risk = node.score || 0.5;
    
    const radius = Math.max(12, risk * 35) / globalScale;
    const color = risk > 0.6 ? "#ff4d4d" : risk > 0.5 ? "#fbbf24" : "#22c55e";

    ctx.save();
    ctx.globalAlpha = alpha;
    
    // Shadow/Glow
    ctx.shadowColor = color;
    ctx.shadowBlur = 20 / globalScale;

    // Node Circle
    ctx.beginPath();
    ctx.arc(node.x, node.y, radius, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.fill();

    // Text Label
    ctx.shadowBlur = 0;
    ctx.font = `bold ${14 / globalScale}px Inter`;
    ctx.textAlign = "center";
    ctx.fillStyle = "#fff";
    ctx.fillText(node.name || node.id, node.x, node.y - radius - 8);

    // Score inside
    ctx.font = `bold ${11 / globalScale}px monospace`;
    ctx.fillStyle = "rgba(0,0,0,0.8)";
    ctx.fillText(risk.toFixed(2), node.x, node.y + 4);

    ctx.restore();
  }, [highlightNodes]);

  return (
    <div 
      ref={containerRef}
      className="w-full h-[600px] bg-[#050505] rounded-3xl border border-slate-800 relative overflow-hidden"
    >
      {/* Legend & HUD */}
      <div className="absolute top-6 left-6 z-20 pointer-events-none">
        <h2 className="text-xl font-bold text-white uppercase italic tracking-wider">Systemic Ripple Analysis</h2>
        <p className="text-slate-500 text-xs">Dynamic Inter-sector Dependency Map</p>
      </div>

      <ForceGraph2D
        ref={fgRef}
        width={dimensions.width}
        height={dimensions.height}
        graphData={processedData}
        nodeCanvasObject={drawNode}
        onNodeHover={handleNodeHover}
        onEngineStop={handleEngineStop}
        backgroundColor="rgba(0,0,0,0)"
        
        // Links
        linkDirectionalParticles={4}
        linkDirectionalParticleSpeed={0.01}
        linkColor={() => "rgba(255,255,255,0.1)"}
        linkWidth={1}
        
        // Forces to spread out the nodes
        d3VelocityDecay={0.3}
        cooldownTicks={100}
      />
    </div>
  );
}
