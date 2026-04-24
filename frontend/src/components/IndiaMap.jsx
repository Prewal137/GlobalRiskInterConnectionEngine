import React, { useState } from "react";
import { ComposableMap, Geographies, Geography, ZoomableGroup } from "react-simple-maps";

const INDIA_GEO_JSON = "https://raw.githubusercontent.com/geohacker/india/master/state/india_telengana.geojson";

// Helper to clean up state names for comparison
const normalize = (str) => str?.toLowerCase().trim();

const getRiskColor = (risk) => {
  if (risk < 0.2) return "#10b981"; // Emerald
  if (risk < 0.4) return "#3b82f6"; // Blue
  if (risk < 0.6) return "#f59e0b"; // Amber
  return "#ef4444"; // Red
};

export default function IndiaMap({ stateRiskData = {}, selectedState, onStateClick }) {
  const [hovered, setHovered] = useState(null);
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });

  return (
    <div
      className="relative w-full h-full bg-slate-950 rounded-2xl overflow-hidden"
      onMouseMove={(e) => setTooltipPos({ x: e.clientX + 15, y: e.clientY + 15 })}
    >
      <ComposableMap
        projection="geoMercator"
        projectionConfig={{ scale: 1000, center: [82, 22] }}
        className="w-full h-full cursor-crosshair"
      >
        <ZoomableGroup zoom={1} minZoom={1} maxZoom={4}>
          <Geographies geography={INDIA_GEO_JSON}>
            {({ geographies }) =>
              geographies.map((geo) => {
                // Robust extraction of state name
                const rawName = geo.properties.st_nm || geo.properties.NAME_1 || geo.properties.name || "Unknown";

                // Find matching risk data
                const matchedKey = Object.keys(stateRiskData).find(
                  (key) => normalize(key) === normalize(rawName)
                );

                const risk = matchedKey ? stateRiskData[matchedKey] : 0.3;
                const isSelected = normalize(selectedState) === normalize(rawName);
                const isHovered = normalize(hovered?.name) === normalize(rawName);

                return (
                  <Geography
                    key={geo.rsmKey}
                    geography={geo}
                    fill={isSelected ? "#ffffff" : isHovered ? "#6366f1" : getRiskColor(risk)}
                    stroke="#020617"
                    strokeWidth={isSelected ? 1 : 0.5}
                    onMouseEnter={() => setHovered({ name: rawName, risk })}
                    onMouseLeave={() => setHovered(null)}
                    onClick={() => onStateClick?.(rawName)}
                    style={{
                      default: { outline: "none", transition: "fill 0.3s" },
                      hover: { outline: "none", cursor: "pointer" },
                      pressed: { outline: "none" },
                    }}
                  />
                );
              })
            }
          </Geographies>
        </ZoomableGroup>
      </ComposableMap>

      {/* Cursor Tooltip */}
      {hovered && (
        <div
          className="fixed z-50 bg-black/90 backdrop-blur-md border border-white/10 p-3 rounded-lg w-44 pointer-events-none shadow-2xl"
          style={{ left: tooltipPos.x, top: tooltipPos.y }}
        >
          <p className="text-xs font-bold text-white mb-1">{hovered.name}</p>
          <div className="h-1.5 w-full bg-slate-800 rounded-full overflow-hidden">
            <div
              className="h-full transition-all duration-300"
              style={{ width: `${hovered.risk * 100}%`, background: getRiskColor(hovered.risk) }}
            />
          </div>
        </div>
      )}
    </div>
  );
}