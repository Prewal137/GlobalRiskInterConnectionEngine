import React, { useState, useEffect } from "react";
import { getLiveRisk } from "../services/api";
import RiskCard from "../components/RiskCard";
import EnhancedCascadingNetwork from "../components/EnhancedCascadingNetwork";
import { 
  AlertTriangle, 
  Activity, 
  RefreshCcw, 
  ArrowUpRight, 
  ChevronRight,
  Info
} from "lucide-react";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

export default function LiveDashboard() {
  const [loading, setLoading] = useState(true);
  const [initialRisk, setInitialRisk] = useState(null);
  const [finalRisk, setFinalRisk] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchRiskData();
  }, []);

  const fetchRiskData = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await getLiveRisk();
      setInitialRisk(response.initial_risk);
      setFinalRisk(response.final_risk);
      setLoading(false);
    } catch (err) {
      console.error("Error fetching risk data:", err);
      setError("System engine unreachable. Check backend connection.");
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex h-[60vh] flex-col items-center justify-center gap-4">
        <div className="h-12 w-12 animate-spin rounded-full border-4 border-indigo-500/20 border-t-indigo-500" />
        <p className="animate-pulse text-slate-400 font-medium">Calibrating global risk vectors...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-[60vh] flex-col items-center justify-center gap-6 rounded-3xl border border-red-500/20 bg-red-500/5 p-12 text-center">
        <div className="rounded-2xl bg-red-500/20 p-4">
          <AlertTriangle className="h-10 w-10 text-red-500" />
        </div>
        <div className="max-w-md space-y-2">
          <h2 className="text-2xl font-bold text-white">Initialization Failed</h2>
          <p className="text-slate-400">{error}</p>
        </div>
        <button 
          onClick={fetchRiskData}
          className="flex items-center gap-2 rounded-xl bg-white px-6 py-3 font-bold text-black transition-transform hover:scale-105 active:scale-95"
        >
          <RefreshCcw className="h-4 w-4" />
          Reinitialize System
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-8 pb-12">
      {/* Hero Header */}
      <div className="flex flex-col justify-between gap-6 md:flex-row md:items-end">
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-indigo-400">
            <Activity className="h-4 w-4" />
            <span className="text-xs font-bold uppercase tracking-[0.2em]">Live Monitoring Active</span>
          </div>
          <h1 className="text-4xl font-bold tracking-tight text-white md:text-5xl font-display">
            Global Risk <span className="text-indigo-400">InterConnection</span>
          </h1>
          <p className="text-lg text-slate-400 max-w-2xl">
            Multi-sector risk assessment engine calculating systemic ripple effects across core global infrastructures.
          </p>
        </div>
        
        <div className="flex items-center gap-3">
          <div className="text-right hidden sm:block">
            <p className="text-[10px] font-bold uppercase tracking-widest text-slate-500">Last Synced</p>
            <p className="text-sm font-medium text-white">{new Date().toLocaleTimeString()}</p>
          </div>
          <button 
            onClick={fetchRiskData}
            className="flex h-12 w-12 items-center justify-center rounded-xl bg-white/5 border border-white/10 text-slate-400 transition-all hover:bg-white/10 hover:text-white"
          >
            <RefreshCcw className="h-5 w-5" />
          </button>
        </div>
      </div>

      {/* Stats Quick View */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-6">
        {finalRisk && Object.entries(finalRisk).map(([sector, risk]) => (
          <RiskCard 
            key={sector} 
            sector={sector} 
            risk={risk} 
            initialRisk={initialRisk?.[sector]} 
          />
        ))}
      </div>

      {/* Main Analysis Grid */}
      <div className="grid gap-8 lg:grid-cols-3">
        {/* Network Visualization */}
        <div className="lg:col-span-2 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-bold text-white flex items-center gap-2">
              <div className="h-2 w-2 rounded-full bg-indigo-500 shadow-[0_0_8px_rgba(99,102,241,1)]" />
              Propagation Network
            </h2>
            <div className="flex gap-2">
              <span className="flex items-center gap-1.5 text-[10px] font-bold uppercase text-slate-500">
                <div className="h-1.5 w-1.5 rounded-full bg-red-500" /> High 
              </span>
              <span className="flex items-center gap-1.5 text-[10px] font-bold uppercase text-slate-500">
                <div className="h-1.5 w-1.5 rounded-full bg-emerald-500" /> Low
              </span>
            </div>
          </div>
          <div className="relative group">
            <div className="absolute -inset-0.5 rounded-3xl bg-gradient-to-b from-indigo-500/20 to-transparent opacity-0 transition-opacity group-hover:opacity-100" />
            <EnhancedCascadingNetwork 
              graphData={{
                nodes: Object.keys(finalRisk || {}).map(id => ({ id, name: id, baseRisk: initialRisk?.[id] || 0.5 })),
                links: [] // Ideally, edges come from backend influence matrix. We'll leave it for now or add some defaults.
              }}
              riskScores={Object.fromEntries(Object.entries(finalRisk || {}).map(([id, score]) => [id, { score }]))}
            />
          </div>
        </div>

        {/* Impact Analysis Table */}
        <div className="space-y-4">
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <ArrowUpRight className="h-5 w-5 text-indigo-400" />
            Inter-sector Impact
          </h2>
          
          <div className="glass-card overflow-hidden">
            <div className="max-h-[550px] overflow-y-auto">
              {Object.entries(initialRisk || {}).map(([sector, initial]) => {
                const final = finalRisk[sector];
                const change = final - initial;
                const percentChange = (change / initial) * 100;
                
                return (
                  <div key={sector} className="group border-b border-white/5 p-4 transition-colors hover:bg-white/5 last:border-0">
                    <div className="flex items-center justify-between gap-4">
                      <div className="flex items-center gap-3">
                        <div className={cn(
                          "flex h-8 w-8 items-center justify-center rounded-lg font-bold text-xs uppercase",
                          final >= 0.7 ? "bg-red-500/20 text-red-400" : "bg-white/5 text-slate-400"
                        )}>
                          {sector.substring(0, 2)}
                        </div>
                        <div>
                          <p className="text-sm font-bold text-white capitalize">{sector}</p>
                          <p className="text-[10px] text-slate-500 uppercase tracking-tight">Systemic Score: {final.toFixed(3)}</p>
                        </div>
                      </div>
                      
                      <div className="text-right">
                        <p className={cn(
                          "text-sm font-bold",
                          change > 0 ? "text-red-400" : change < 0 ? "text-emerald-400" : "text-white"
                        )}>
                          {change > 0 ? "+" : ""}{percentChange.toFixed(1)}%
                        </p>
                        <p className="text-[10px] uppercase font-bold text-slate-600">Cascade Shift</p>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
            
            <div className="bg-white/5 p-4">
              <button className="flex w-full items-center justify-between rounded-lg bg-indigo-500 px-4 py-2.5 text-sm font-bold text-white transition-opacity hover:opacity-90">
                View Detailed Analysis
                <ChevronRight className="h-4 w-4" />
              </button>
            </div>
          </div>
          
          <div className="rounded-2xl border border-indigo-500/10 bg-indigo-500/5 p-4 flex gap-3">
            <Info className="h-5 w-5 text-indigo-400 shrink-0" />
            <p className="text-xs text-slate-400 leading-relaxed">
              <span className="text-indigo-300 font-bold">Heuristic Note:</span> Scores reflect normalized risk probability after 5 iterations of the cascading engine. 
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
