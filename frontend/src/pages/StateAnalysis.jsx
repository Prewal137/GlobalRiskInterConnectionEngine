import React, { useState } from "react";
import { getStateRisk, getStateImpact } from "../services/api";
import IndiaMap from "../components/IndiaMap";
import EnhancedCascadingNetwork from "../components/EnhancedCascadingNetwork";
import { 
  MapPin, 
  Activity, 
  Layers, 
  MoreVertical,
  Zap
} from "lucide-react";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

export default function StateAnalysis() {
  const [selectedState, setSelectedState] = useState("Karnataka");
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
      console.error("Error fetching state data:", err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-8 pb-12">
      <div className="space-y-2">
        <div className="flex items-center gap-2 text-indigo-400">
          <MapPin className="h-4 w-4" />
          <span className="text-xs font-bold uppercase tracking-[0.2em]">Geospatial Intelligence</span>
        </div>
        <h1 className="text-4xl font-bold tracking-tight text-white font-display">
          State-Level <span className="text-indigo-400">Sub-Analysis</span>
        </h1>
        <p className="text-slate-400">Localized risk propagation models for specific economic zones.</p>
      </div>

      <div className="grid gap-8 lg:grid-cols-2">
        {/* Map Selection */}
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-bold text-white flex items-center gap-2">
              <Layers className="h-5 w-5 text-indigo-400" />
              Regional Selection
            </h2>
            <span className="text-xs font-bold text-indigo-400 bg-indigo-500/10 px-3 py-1 rounded-full uppercase">
              Current: {selectedState}
            </span>
          </div>
          <div className="glass-card min-h-[500px] overflow-hidden p-6 flex flex-col items-center justify-center">
            <IndiaMap stateRiskData={{}} onStateClick={handleStateClick} />
            <p className="mt-8 text-sm text-slate-500 italic">
              * Click on a state marker to initialize localized engine.
            </p>
          </div>
        </div>

        {/* Localized Cascade */}
        <div className="space-y-4">
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <Zap className="h-5 w-5 text-indigo-400" />
            Localized Propagation
          </h2>
          <div className="glass-card min-h-[500px] p-0 overflow-hidden">
            {loading ? (
              <div className="flex h-full items-center justify-center min-h-[500px]">
                <div className="h-8 w-8 animate-spin rounded-full border-4 border-indigo-500/20 border-t-indigo-500" />
              </div>
            ) : impactData ? (
              <EnhancedCascadingNetwork 
                graphData={{
                  nodes: Object.keys(impactData.final || {}).map(id => ({ id, name: id, baseRisk: impactData.initial?.[id] || 0.5 })),
                  links: []
                }}
                riskScores={Object.fromEntries(Object.entries(impactData.final || {}).map(([id, score]) => [id, { score }]))}
              />
            ) : (
              <div className="flex h-full flex-col items-center justify-center min-h-[500px] gap-4 p-12 text-center">
                <div className="h-16 w-16 rounded-full bg-white/5 flex items-center justify-center">
                  <Activity className="h-8 w-8 text-slate-600" />
                </div>
                <div className="space-y-1">
                  <h3 className="text-white font-bold">No Regional Probe Active</h3>
                  <p className="text-slate-500 text-sm">Select a state on the left to begin localized cascade simulation.</p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {impactData && (
        <div className="space-y-4 animate-in fade-in slide-in-from-bottom-4 duration-700">
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <MoreVertical className="h-5 w-5 text-indigo-400" />
            Regional Impact Vector
          </h2>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {Object.entries(impactData.final || {}).map(([sector, risk]) => {
              const initial = impactData.initial?.[sector] || 0;
              const change = risk - initial;
              return (
                <div key={sector} className="glass-card p-5 group flex flex-col justify-between transition-all hover:bg-white/10">
                  <div className="flex justify-between items-start">
                    <div>
                      <span className="text-[10px] font-bold uppercase tracking-widest text-indigo-400">{sector}</span>
                      <h3 className="text-2xl font-bold text-white">{(risk * 100).toFixed(1)}%</h3>
                    </div>
                    <div className={cn(
                      "px-2 py-0.5 rounded text-[10px] font-bold border",
                      change >= 0 ? "bg-red-500/10 text-red-400 border-red-500/20" : "bg-emerald-500/10 text-emerald-400 border-emerald-500/20"
                    )}>
                      {change >= 0 ? "+" : ""}{(change * 100).toFixed(1)}%
                    </div>
                  </div>
                  <div className="mt-4 h-1 w-full bg-white/5 rounded-full overflow-hidden">
                    <div 
                      className={cn("h-full", risk > 0.7 ? "bg-red-500" : "bg-indigo-500")}
                      style={{ width: `${risk * 100}%` }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
