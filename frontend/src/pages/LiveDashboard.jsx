import React, { useState, useEffect, useMemo } from "react";
import { 
  getLiveRisk, 
  getLiveClimate, 
  getLiveEconomy, 
  getLiveTrade, 
  getLiveGeopolitics, 
  getLiveMigration, 
  getLiveSocial, 
  getLiveInfrastructure 
} from "../services/api";
import RiskCard from "../components/RiskCard";
import EnhancedCascadingNetwork from "../components/EnhancedCascadingNetwork";
import { 
  AlertTriangle, 
  Activity, 
  RefreshCcw, 
  ArrowUpRight, 
  ChevronRight,
  Info,
  Globe,
  BarChart3,
  Waves,
  Zap
} from "lucide-react";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

const LIVE_SECTORS = [
  { id: "all", label: "Global Sync", icon: Globe, fetch: getLiveRisk },
  { id: "climate", label: "Climate", icon: Waves, fetch: getLiveClimate },
  { id: "economy", label: "Economy", icon: BarChart3, fetch: getLiveEconomy },
  { id: "trade", label: "Trade", icon: Activity, fetch: getLiveTrade },
  { id: "geopolitics", label: "Geopolitics", icon: Zap, fetch: getLiveGeopolitics },
];

export default function LiveDashboard() {
  const [activeTab, setActiveTab] = useState("all");
  const [loading, setLoading] = useState(true);
  const [initialRisk, setInitialRisk] = useState(null);
  const [finalRisk, setFinalRisk] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchRiskData();
  }, [activeTab]);

  const fetchRiskData = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const sector = LIVE_SECTORS.find(s => s.id === activeTab);
      const response = await sector.fetch();
      
      // Some endpoints return different structures, we normalize here
      setInitialRisk(response.initial_risk || response.initial || {});
      setFinalRisk(response.final_risk || response.final || response.data || {});
      
      setLoading(false);
    } catch (err) {
      console.error("Error fetching risk data:", err);
      setError("System engine unreachable. Check backend connection.");
      setLoading(false);
      setInitialRisk({});
      setFinalRisk({});
    }
  };

  const networkData = useMemo(() => {
    const nodes = Object.keys(finalRisk || {}).map(id => ({ 
      id, 
      name: id, 
      baseRisk: initialRisk?.[id] || 0.5 
    }));
    return { nodes, links: [] };
  }, [finalRisk, initialRisk]);

  if (loading && !finalRisk) {
    return (
      <div className="flex h-[70vh] flex-col items-center justify-center gap-6">
        <div className="relative h-20 w-20">
          <div className="absolute inset-0 animate-ping rounded-full bg-indigo-500/20" />
          <div className="relative flex h-20 w-20 items-center justify-center rounded-full border-4 border-indigo-500/20 border-t-indigo-500 animate-spin" />
        </div>
        <div className="text-center space-y-2">
          <p className="text-xl font-bold text-white">Calibrating Risk Vectors</p>
          <p className="text-slate-500 animate-pulse">Syncing with global monitoring stations...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-10 pb-20">
      {/* Hero Header */}
      <div className="relative overflow-hidden rounded-[2.5rem] bg-slate-900/40 p-10 border border-slate-800/60">
        <div className="absolute -right-20 -top-20 h-64 w-64 rounded-full bg-indigo-500/10 blur-[100px]" />
        
        <div className="relative flex flex-col justify-between gap-8 lg:flex-row lg:items-center">
          <div className="space-y-4">
            <div className="flex items-center gap-2 text-indigo-400">
              <div className="h-1.5 w-1.5 rounded-full bg-indigo-400 animate-pulse" />
              <span className="text-[10px] font-black uppercase tracking-[0.3em]">Neural Monitoring Active</span>
            </div>
            <h1 className="text-5xl font-black tracking-tight text-white md:text-6xl lg:max-w-xl leading-[1.1]">
              Live Risk <span className="bg-clip-text text-transparent bg-gradient-to-r from-indigo-400 to-cyan-400">Intelligence</span>
            </h1>
            <p className="text-lg text-slate-400 max-w-xl leading-relaxed">
              Real-time systemic ripple analysis across critical infrastructures and global geopolitical corridors.
            </p>
          </div>
          
          <div className="flex flex-col items-end gap-6">
             <div className="bg-black/40 backdrop-blur-md p-6 rounded-3xl border border-white/5 w-full lg:w-64">
                <p className="text-[10px] font-black uppercase tracking-widest text-slate-500 mb-1">Last Transmission</p>
                <p className="text-2xl font-mono font-bold text-white">{new Date().toLocaleTimeString()}</p>
                <div className="mt-4 h-1 w-full bg-slate-800 rounded-full overflow-hidden">
                   <div className="h-full bg-indigo-500 animate-[progress_10s_linear_infinite]" />
                </div>
             </div>
             <button 
               onClick={fetchRiskData}
               className="group flex items-center gap-3 rounded-2xl bg-white px-8 py-4 font-black text-black transition-all hover:bg-slate-200 hover:scale-[1.02] active:scale-[0.98]"
             >
               <RefreshCcw className={cn("h-5 w-5 transition-transform group-hover:rotate-180 duration-700", loading && "animate-spin")} />
               SYNC ENGINE
             </button>
          </div>
        </div>
      </div>

      {/* Tabs Selector */}
      <div className="flex gap-2 p-1.5 bg-slate-900/50 rounded-2xl border border-slate-800 w-fit mx-auto">
        {LIVE_SECTORS.map((tab) => {
          const Icon = tab.icon;
          const isActive = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={cn(
                "flex items-center gap-2 px-6 py-3 rounded-xl text-sm font-bold transition-all",
                isActive 
                  ? "bg-indigo-500 text-white shadow-lg shadow-indigo-500/20" 
                  : "text-slate-500 hover:text-slate-300 hover:bg-slate-800/50"
              )}
            >
              <Icon size={16} />
              {tab.label}
            </button>
          );
        })}
      </div>

      {error ? (
        <div className="flex flex-col items-center justify-center gap-6 rounded-3xl border border-red-500/20 bg-red-500/5 p-16 text-center">
          <div className="rounded-3xl bg-red-500/10 p-5 border border-red-500/20">
            <AlertTriangle className="h-10 w-10 text-red-500" />
          </div>
          <div className="max-w-md space-y-2">
            <h2 className="text-2xl font-bold text-white">Interface Interrupted</h2>
            <p className="text-slate-400">{error}</p>
          </div>
          <button 
            onClick={fetchRiskData}
            className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-8 py-3 font-bold text-white transition-all hover:bg-white/10"
          >
            Reconnect
          </button>
        </div>
      ) : (
        <>
          {/* Stats Quick View */}
          <div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-5">
            {finalRisk && Object.entries(finalRisk).filter(([v]) => typeof v === 'string').map(([sector, risk]) => (
              <RiskCard 
                key={sector} 
                sector={sector} 
                risk={typeof risk === 'number' ? risk : 0.5} 
                initialRisk={initialRisk?.[sector] || 0.4} 
              />
            ))}
          </div>

          {/* Main Analysis Grid */}
          <div className="grid gap-8 lg:grid-cols-3">
            {/* Network Visualization */}
            <div className="lg:col-span-2 space-y-6">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-2xl font-black text-white flex items-center gap-3">
                    Knowledge Graph
                  </h2>
                  <p className="text-slate-500 text-sm">Cross-sector influence and propagation likelihood</p>
                </div>
                <div className="p-1 px-4 rounded-full bg-slate-900/50 border border-slate-800 text-[10px] font-black uppercase text-indigo-400 tracking-widest">
                  Live Vector
                </div>
              </div>
              
              <div className="relative group bg-slate-900/20 rounded-[3rem] border border-slate-800/40 p-4 h-[650px] overflow-hidden">
                <EnhancedCascadingNetwork 
                  graphData={networkData}
                  riskScores={Object.fromEntries(Object.entries(finalRisk || {}).map(([id, score]) => [id, { score: typeof score === 'number' ? score : 0.5 }]))}
                />
              </div>
            </div>

            {/* Impact Analysis Table */}
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <h2 className="text-2xl font-black text-white flex items-center gap-3">
                  Pulse Log
                </h2>
                <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
                  <ArrowUpRight size={16} />
                </div>
              </div>
              
              <div className="bg-slate-900/40 backdrop-blur-md rounded-[2.5rem] border border-slate-800/60 overflow-hidden flex flex-col h-[650px]">
                <div className="p-8 pb-4">
                  <p className="text-sm text-slate-400">Systemic probability shifts detected in last 24h</p>
                </div>
                <div className="flex-1 overflow-y-auto custom-scrollbar px-4">
                  {Object.entries(finalRisk || {}).map(([sector, final]) => {
                    if (typeof final !== 'number') return null;
                    const initial = initialRisk?.[sector] || (final * 0.9);
                    const change = final - initial;
                    const percentChange = (change / (initial || 1)) * 100;
                    
                    return (
                      <div key={sector} className="group border-b border-white/5 p-5 transition-colors hover:bg-white/5 last:border-0 rounded-2xl">
                        <div className="flex items-center justify-between gap-4">
                          <div className="flex items-center gap-4">
                            <div className={cn(
                              "flex h-12 w-12 items-center justify-center rounded-2xl font-black text-xs uppercase tracking-tighter",
                              final >= 0.7 ? "bg-red-500/10 text-red-500 border border-red-500/20" : "bg-indigo-500/10 text-indigo-400 border border-indigo-500/20"
                            )}>
                              {sector.substring(0, 3)}
                            </div>
                            <div>
                              <p className="text-md font-black text-white capitalize">{sector}</p>
                              <div className="flex items-center gap-2">
                                <span className="text-[10px] text-slate-500 uppercase font-black">Score: {final.toFixed(3)}</span>
                                <div className="h-1 w-1 rounded-full bg-slate-700" />
                                <span className={cn(
                                  "text-[10px] font-black uppercase",
                                  final > 0.7 ? "text-red-500" : final > 0.4 ? "text-amber-500" : "text-emerald-500"
                                )}>
                                  {final > 0.7 ? "Critical" : final > 0.4 ? "Elevated" : "Nominal"}
                                </span>
                              </div>
                            </div>
                          </div>
                          
                          <div className="text-right">
                            <p className={cn(
                              "text-sm font-black font-mono",
                              change > 0 ? "text-red-500" : change < 0 ? "text-emerald-400" : "text-slate-500"
                            )}>
                              {change > 0 ? "▲" : change < 0 ? "▼" : "•"} {Math.abs(percentChange).toFixed(1)}%
                            </p>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
                
                <div className="p-8 bg-black/20">
                  <div className="flex gap-4 p-4 rounded-3xl bg-indigo-500/5 border border-indigo-500/10">
                    <Info className="h-5 w-5 text-indigo-400 shrink-0" />
                    <p className="text-[11px] text-slate-400 leading-relaxed font-medium">
                      Neural patterns indicate a high correlation between <span className="text-white">energy volatility</span> and <span className="text-white">regional stability</span> indices.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </>
      )}

      <style jsx>{`
        .custom-scrollbar::-webkit-scrollbar {
          width: 4px;
        }
        .custom-scrollbar::-webkit-scrollbar-track {
          background: transparent;
        }
        .custom-scrollbar::-webkit-scrollbar-thumb {
          background: #1e293b;
          border-radius: 10px;
        }
        @keyframes progress {
          from { width: 0%; }
          to { width: 100%; }
        }
      `}</style>
    </div>
  );
}

