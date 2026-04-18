import React, { useState } from "react";
import { ComposableMap, Geographies, Geography, Marker } from "react-simple-maps";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

const getRiskColor = (risk) => {
  if (risk < 0.3) return "#10b981"; // emerald-500
  if (risk < 0.6) return "#f59e0b"; // amber-500
  return "#ef4444"; // red-500
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

export default function IndiaMap({ stateRiskData, onStateClick }) {
  const [hoveredState, setHoveredState] = useState(null);

  const handleClick = (state) => {
    if (onStateClick) {
      onStateClick(state);
    }
  };

  return (
    <div className="relative w-full h-[500px] flex items-center justify-center bg-[#050505] rounded-3xl border border-white/5 overflow-hidden">
      {/* HUD Tooltip */}
      {hoveredState && (
        <div className="absolute top-6 right-6 z-20 glass-card p-4 animate-in fade-in zoom-in duration-200">
          <p className="text-[10px] font-bold uppercase tracking-widest text-slate-500">Region Focus</p>
          <p className="text-xl font-bold text-white font-display">{hoveredState.name}</p>
          <div className="mt-2 flex items-center gap-2">
            <div className={cn("h-2 w-2 rounded-full", hoveredState.risk > 0.6 ? "bg-red-500" : "bg-emerald-500")} />
            <span className="text-sm font-bold text-slate-300">Composite Risk: {(hoveredState.risk * 100).toFixed(1)}%</span>
          </div>
        </div>
      )}

      <ComposableMap
        projection="geoMercator"
        projectionConfig={{
          scale: 1000,
          center: [78.9629, 22.5937],
        }}
        className="w-full h-full"
      >
        <Geographies geography="https://raw.githubusercontent.com/lokesh-755/India-State-and-Country-Maps-JSON-TopoJSON/master/india.json">
          {({ geographies }) =>
            geographies.map((geo) => {
              const stateName = geo.properties.ST_NM || geo.properties.name;
              const risk = stateRiskData?.[stateName] || Math.random() * 0.6;
              const isHovered = hoveredState?.name === stateName;
              
              return (
                <Geography
                  key={geo.rsmKey}
                  geography={geo}
                  fill={isHovered ? "#6366f1" : getRiskColor(risk)}
                  stroke="#050505"
                  strokeWidth={0.5}
                  className="transition-all duration-200 outline-none cursor-pointer"
                  style={{
                    default: { outline: "none", opacity: 0.8 },
                    hover: { outline: "none", opacity: 1 },
                    pressed: { outline: "none" },
                  }}
                  onMouseEnter={() => setHoveredState({ name: stateName, risk })}
                  onMouseLeave={() => setHoveredState(null)}
                  onClick={() => handleClick(stateName)}
                />
              );
            })
          }
        </Geographies>
        
        {Object.entries(stateCoordinates).map(([state, coords]) => (
          <Marker key={state} coordinates={coords}>
            <circle
              r={isHoveredRegion(state) ? 6 : 3}
              fill="white"
              stroke="#6366f1"
              strokeWidth={2}
              className="cursor-pointer transition-all hover:r-6"
              onClick={() => handleClick(state)}
            />
          </Marker>
        ))}
      </ComposableMap>
    </div>
  );

  function isHoveredRegion(name) {
    return hoveredState?.name === name;
  }
}
