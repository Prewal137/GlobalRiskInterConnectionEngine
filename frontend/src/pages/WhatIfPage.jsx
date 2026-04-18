import React, { useState } from "react";
import { runWhatIfSimulation } from "../services/api";
import EnhancedCascadingNetwork from "../components/EnhancedCascadingNetwork";
import IndiaMap from "../components/IndiaMap";
import RiskCard from "../components/RiskCard";
import { 
  Zap, 
  Settings2, 
  Play, 
  History,
  Activity,
  ArrowRight
} from "lucide-react";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

const sectorsList = ["climate", "economy", "trade", "geopolitics", "migration", "social", "infrastructure"];

export default function WhatIfPage() {
  const [sliders, setSliders] = useState(
    Object.fromEntries(sectorsList.map(s => [s, 0.3]))
  );

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
      console.error("Error running simulation:", err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-8 pb-12">
      <div className="space-y-2">
        <div className="flex items-center gap-2 text-indigo-400">
          <Zap className="h-4 w-4" />
          <span className="text-xs font-bold uppercase tracking-[0.2em]">Scenario Testing</span>
        </div>
        <h1 className="text-4xl font-bold tracking-tight text-white font-display">
          What-If <span className="text-indigo-400">Simulator</span>
        </h1>
        <p className="text-slate-400">Inject synthetic shocks into the system to observe long-term cascading stability.</p>
      </div>

      <div className="grid gap-8 lg:grid-cols-3">
        {/* Controls */}
        <div className="lg:col-span-1 space-y-6">
          <div className="flex items-center gap-2">
            <Settings2 className="h-5 w-5 text-indigo-400" />
            <h2 className="text-xl font-bold text-white">Shock Parameters</h2>
          </div>
          
          <div className="glass-card p-6 space-y-6">
            {Object.entries(sliders).map(([sector, value]) => (
              <div key={sector} className="space-y-3">
                <div className="flex justify-between items-center text-xs font-bold uppercase tracking-widest">
                  <span className="text-slate-400">{sector}</span>
                  <span className={cn(
                    "text-sm font-mono",
                    value > 0.6 ? "text-red-400" : value > 0.3 ? "text-orange-400" : "text-emerald-400"
                  )}>
                    {(value * 100).toFixed(0)}%
                  </span>
                </div>
                <input
                  type="range"
                  min="0"
                  max="1"
                  step="0.01"
                  value={value}
                  onChange={handleSliderChange(sector)}
                  className="w-full h-1.5 bg-white/5 rounded-full appearance-none cursor-pointer accent-indigo-500"
                />
              </div>
            ))}
            
            <button 
              onClick={handleRun}
              disabled={loading}
              className="w-full flex items-center justify-center gap-2 bg-indigo-500 hover:bg-indigo-600 disabled:opacity-50 text-white font-bold py-4 rounded-xl transition-all hover:shadow-lg hover:shadow-indigo-500/20 active:scale-95"
            >
              {loading ? (
                <div className="h-5 w-5 animate-spin rounded-full border-2 border-white/20 border-t-white" />
              ) : (
                <>
                  <Play className="h-5 w-5 fill-current" />
                  Initiate Shock Wave
                </>
              )}
            </button>
          </div>
        </div>

        {/* Visualization Output */}
        <div className="lg:col-span-2 space-y-6">
          <div className="flex items-center gap-2">
            <Activity className="h-5 w-5 text-indigo-400" />
            <h2 className="text-xl font-bold text-white">Propagation Result</h2>
          </div>
          
          <div className="glass-card min-h-[600px] flex items-center justify-center overflow-hidden">
            {!results && !loading ? (
              <div className="p-12 text-center space-y-4">
                <div className="h-20 w-20 rounded-full bg-white/5 mx-auto flex items-center justify-center">
                  <Zap className="h-10 w-10 text-slate-700" />
                </div>
                <div className="max-w-xs mx-auto">
                  <h3 className="text-white font-bold">Simulator Idle</h3>
                  <p className="text-slate-500 text-sm">Configure parameters on the left and engage the shock wave to see results.</p>
                </div>
              </div>
            ) : loading ? (
              <div className="p-12 text-center space-y-4 animate-pulse">
                <div className="h-20 w-20 rounded-full border-4 border-indigo-500/20 border-t-indigo-500 animate-spin mx-auto" />
                <p className="text-indigo-400 font-bold uppercase tracking-widest text-xs">Computing Inter-sector Influence...</p>
              </div>
            ) : (
              <div className="w-full h-full animate-in fade-in duration-1000">
                <EnhancedCascadingNetwork 
                  graphData={{
                    nodes: Object.keys(results.final || {}).map(id => ({ id, name: id, baseRisk: results.initial?.[id] || 0.5 })),
                    links: []
                  }}
                  riskScores={Object.fromEntries(Object.entries(results.final || {}).map(([id, score]) => [id, { score }]))}
                />
              </div>
            )}
          </div>
        </div>
      </div>

      {results && (
        <div className="space-y-6 animate-in slide-in-from-bottom-8 duration-700">
          <div className="flex items-center gap-2">
            <History className="h-5 w-5 text-indigo-400" />
            <h2 className="text-xl font-bold text-white">Delta Analysis</h2>
          </div>
          
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-6">
            {Object.entries(results.final).map(([sector, finalRisk]) => (
              <RiskCard
                key={sector}
                sector={sector}
                risk={finalRisk}
                initialRisk={results.initial?.[sector] || 0}
              />
            ))}
          </div>
          
          {results.steps && (
            <div className="glass-card p-6">
               <h3 className="text-sm font-bold uppercase tracking-widest text-slate-500 mb-6">Simulation Iterations</h3>
               <div className="flex items-center gap-4 overflow-x-auto pb-4 scrollbar-hide">
                  {results.steps.slice(0, 8).map((step, idx) => (
                    <div key={idx} className="flex items-center shrink-0">
                      <div className="glass-card p-4 flex flex-col items-center gap-2 border-indigo-500/10">
                        <span className="text-[10px] font-bold text-indigo-400">Step {idx + 1}</span>
                        <div className="flex -space-x-1">
                          {Object.values(step).slice(0, 3).map((val, i) => (
                            <div key={i} className={cn("h-2 w-2 rounded-full", val > 0.6 ? "bg-red-500" : "bg-emerald-500")} />
                          ))}
                        </div>
                      </div>
                      {idx < Math.min(results.steps.length, 8) - 1 && <ArrowRight className="h-4 w-4 text-slate-800 ml-4" />}
                    </div>
                  ))}
                  {results.steps.length > 8 && <span className="text-slate-700 font-bold ml-4">+{results.steps.length - 8} more</span>}
               </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
