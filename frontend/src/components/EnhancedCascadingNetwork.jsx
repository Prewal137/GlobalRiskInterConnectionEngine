import React, { useRef, useMemo, useEffect, useState, useCallback } from "react";
import ForceGraph2D from "react-force-graph-2d";
import * as d3 from "d3-force";

const systemicLinks = [
  { source: "Climate", target: "Economy", label: "Yield Impact", weight: 0.8 },
  { source: "Climate", target: "Infrastructure", label: "Extreme Events", weight: 0.9 },
  { source: "Economy", target: "Social", label: "Cost of Living", weight: 0.75 },
  { source: "Economy", target: "Trade", label: "Market Volatility", weight: 0.85 },
  { source: "Geopolitics", target: "Trade", label: "Sanctions/Policy", weight: 0.9 },
  { source: "Geopolitics", target: "Migration", label: "Displaced Persons", weight: 0.95 },
  { source: "Social", target: "Geopolitics", label: "Civil Unrest", weight: 0.7 },
  { source: "Social", target: "Migration", label: "Quality of Life", weight: 0.6 },
  { source: "Infrastructure", target: "Economy", label: "Logistic Chain", weight: 0.8 },
  { source: "Trade", target: "Economy", label: "Forex/GDP", weight: 0.9 },
];

const sectorNodes = [
  { id: "Climate", name: "Climate", color: "#10b981" },
  { id: "Economy", name: "Economy", color: "#6366f1" },
  { id: "Trade", name: "Trade", color: "#8b5cf6" },
  { id: "Geopolitics", name: "Geopolitics", color: "#f43f5e" },
  { id: "Migration", name: "Migration", color: "#f59e0b" },
  { id: "Social", name: "Social Stability", color: "#ec4899" },
  { id: "Infrastructure", name: "Infrastructure", color: "#0ea5e9" },
];

export default function EnhancedCascadingNetwork({ graphData=null, riskScores = {} }) {
  const fgRef = useRef();
  const containerRef = useRef();
  const [dimensions, setDimensions] = useState({ width: 0, height: 600 });
  const [hoverNode, setHoverNode] = useState(null);
  const [highlightNodes, setHighlightNodes] = useState(new Set());
  const [highlightLinks, setHighlightLinks] = useState(new Set());

  // Handle Resize
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
    // If external graphData is provided (like in What-If), we use its nodes but our systemic links
    const nodesRaw = graphData?.nodes || sectorNodes;
    
    const nodes = nodesRaw.map(node => {
        const score = riskScores[node.id]?.score || node.baseRisk || (Math.random() * 0.4 + 0.2);
        const baseColor = sectorNodes.find(n => n.id === node.id)?.color || "#6366f1";
        return {
            ...node,
            score,
            color: score > 0.7 ? "#ef4444" : score > 0.5 ? "#f59e0b" : baseColor
        };
    });

    const links = systemicLinks.filter(l => 
        nodes.find(n => n.id === l.source) && nodes.find(n => n.id === l.target)
    ).map(l => ({ ...l }));

    return { nodes, links };
  }, [graphData, riskScores]);

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
    const alpha = isHighlighted ? 1 : 0.2;
    const risk = node.score || 0.5;
    
    const radius = Math.max(14, risk * 40) / globalScale;
    const color = node.color;

    ctx.save();
    ctx.globalAlpha = alpha;
    
    // Outer Glow
    ctx.shadowColor = color;
    ctx.shadowBlur = (isHighlighted ? 25 : 5) / globalScale;

    // Node Circle
    ctx.beginPath();
    ctx.arc(node.x, node.y, radius, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.fill();
    
    // Inner Circle for intensity
    ctx.beginPath();
    ctx.arc(node.x, node.y, radius * 0.7, 0, 2 * Math.PI);
    ctx.fillStyle = "rgba(255,255,255,0.2)";
    ctx.fill();

    // Text Label (Top)
    ctx.shadowBlur = 0;
    ctx.font = `black ${16 / globalScale}px Outfit, sans-serif`;
    ctx.textAlign = "center";
    ctx.fillStyle = "#fff";
    ctx.fillText((node.name || node.id).toUpperCase(), node.x, node.y - radius - 12);

    // Risk Meter Text (Bottom)
    ctx.font = `bold ${12 / globalScale}px JetBrains Mono, monospace`;
    ctx.fillStyle = "rgba(255,255,255,0.7)";
    ctx.fillText(`${(risk * 100).toFixed(0)}%`, node.x, node.y + radius + 15);

    ctx.restore();
  }, [highlightNodes]);

  const drawLink = useCallback((link, ctx, globalScale) => {
    const isHighlighted = highlightLinks.has(link) || highlightLinks.size === 0;
    const alpha = isHighlighted ? 0.4 : 0.05;
    
    ctx.save();
    ctx.globalAlpha = alpha;
    ctx.strokeStyle = "#fff";
    ctx.lineWidth = 2 / globalScale;
    
    // Draw the line
    ctx.beginPath();
    ctx.moveTo(link.source.x, link.source.y);
    ctx.lineTo(link.target.x, link.target.y);
    ctx.stroke();

    if (isHighlighted && link.label) {
        // Draw Link Label at midpoint
        const midX = (link.source.x + link.target.x) / 2;
        const midY = (link.source.y + link.target.y) / 2;
        
        ctx.font = `${10 / globalScale}px Inter`;
        ctx.fillStyle = "rgba(255,255,255,0.6)";
        ctx.textAlign = "center";
        ctx.fillText(link.label, midX, midY - 5);
    }

    ctx.restore();
  }, [highlightLinks]);

  return (
    <div 
      ref={containerRef}
      className="w-full h-full bg-[#050505] rounded-3xl border border-slate-800/40 relative overflow-hidden"
    >
      <ForceGraph2D
        ref={fgRef}
        width={dimensions.width}
        height={dimensions.height}
        graphData={processedData}
        nodeCanvasObject={drawNode}
        onNodeHover={handleNodeHover}
        onEngineStop={handleEngineStop}
        backgroundColor="rgba(0,0,0,0)"
        
        // Link Visualization
        linkCanvasObject={drawLink}
        linkDirectionalParticles={node => (highlightNodes.has(node.source.id) ? 8 : 2)}
        linkDirectionalParticleSpeed={0.005}
        linkDirectionalParticleWidth={2}
        linkDirectionalParticleColor={() => "#fff"}
        linkDirectionalArrowLength={6}
        linkDirectionalArrowRelPos={1}
        
        // Forces
        d3VelocityDecay={0.4}
        d3AlphaDecay={0.02}
        cooldownTicks={150}
      />
    </div>
  );
}

