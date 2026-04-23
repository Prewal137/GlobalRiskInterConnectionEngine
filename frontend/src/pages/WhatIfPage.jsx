import React, { useState, useMemo } from "react";
import { runWhatIfSimulation, simulateShock } from "../services/api";
import EnhancedCascadingNetwork from "../components/EnhancedCascadingNetwork";
import RiskCard from "../components/RiskCard";
import { 
  Zap, 
  Play, 
  RefreshCw, 
  Settings2, 
  Activity, 
  ShieldAlert, 
  Info,
  ChevronRight,
  Sparkles
} from "lucide-react";
import { 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer, 
  Cell 
} from "recharts";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

const SECTORS_LIST = [
  "climate",
  "economy",
  "trade",
  "geopolitics",
  "migration",
  "social",
  "infrastructure",
];

export default function WhatIfPage() {
  const [sliders, setSliders] = useState(
    Object.fromEntries(SECTORS_LIST.map((s) => [s, 0.3]))
  );

  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [activeSimulation, setActiveSimulation] = useState(null);

  const handleSliderChange = (sector) => (e) => {
    setSliders((prev) => ({
      ...prev,
      [sector]: parseFloat(e.target.value),
    }));
  };

  const handleRun = async () => {
    setLoading(true);
    setActiveSimulation("Custom Vector Simulation");
    try {
      const res = await runWhatIfSimulation(sliders);
      setResults(res);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleQuickShock = async (sector) => {
    setLoading(true);
    setActiveSimulation(`${sector.toUpperCase()} Systemic Shock`);
    try {
      const res = await simulateShock(sector, 0.85);
      setResults(res);
      // Update sliders to reflect the shock for visual consistency
      setSliders(prev => ({ ...prev, [sector]: 0.85 }));
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleReset = () => {
    setSliders(Object.fromEntries(SECTORS_LIST.map((s) => [s, 0.3])));
    setResults(null);
    setActiveSimulation(null);
  };

  const chartData = useMemo(() => {
    if (!results?.final) return [];
    return Object.entries(results.final).map(([sector, val]) => ({
      sector,
      risk: val,
    }));
  }, [results]);

  const networkData = useMemo(() => {
    if (!results?.final) return { nodes: [], links: [] };
    return {
      nodes: Object.keys(results.final).map((id) => ({
        id,
        name: id,
        baseRisk: results.initial?.[id] || 0.5,
      })),
      links: [],
    };
  }, [results]);

  const insights = useMemo(() => {
    if (!results?.final) return null;
    const sorted = Object.entries(results.final).sort((a, b) => b[1] - a[1]);
    return {
      highest: sorted[0],
      lowest: sorted[sorted.length - 1],
    };
  }, [results]);

  return (
    <div className="space-y-10 pb-20">
      {/* HEADER SECTION */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-amber-500">
             <Zap size={16} fill="currentColor" />
             <span className="text-[10px] font-black uppercase tracking-[0.3em]">Simulation Lab v2.4</span>
          </div>
          <h1 className="text-4xl font-black tracking-tight text-white bg-clip-text text-transparent bg-gradient-to-r from-amber-400 to-orange-500">
             What-If Simulator
          </h1>
          <p className="text-slate-400 max-w-xl">
             Stress-test global systems by injecting synthetic shocks into recursive risk models.
          </p>
        </div>
        
        <div className="flex gap-3">
           <button 
             onClick={handleReset}
             className="px-6 py-3 rounded-2xl bg-slate-900 border border-slate-800 text-slate-400 font-bold hover:bg-slate-800 transition-all flex items-center gap-2"
           >
             <RefreshCw size={18} />
             Reset Lab
           </button>
           <button 
             onClick={handleRun}
             disabled={loading}
             className="px-8 py-3 rounded-2xl bg-indigo-500 text-white font-black hover:bg-indigo-600 shadow-lg shadow-indigo-500/20 transition-all flex items-center gap-3 disabled:opacity-50"
           >
             <Play size={18} fill="currentColor" />
             EXE SIMULATION
           </button>
        </div>
      </div>

      {/* QUICK SHOCK STRIP */}
      <div className="p-1 px-4 bg-slate-900/40 rounded-3xl border border-slate-800/60 overflow-hidden">
        <div className="flex items-center gap-6 overflow-x-auto py-4 no-scrollbar">
           <span className="text-[10px] font-black uppercase text-slate-500 tracking-widest shrink-0">Systemic Shocks:</span>
           {SECTORS_LIST.map((s) => (
             <button
               key={s}
               onClick={() => handleQuickShock(s)}
               className="group flex items-center gap-2 px-4 py-2 bg-red-500/10 border border-red-500/20 text-red-500 rounded-xl whitespace-nowrap transition-all hover:bg-red-500 hover:text-white"
             >
               <ShieldAlert size={14} className="group-hover:animate-pulse" />
               <span className="text-xs font-black uppercase">{s}</span>
             </button>
           ))}
        </div>
      </div>

      {/* MAIN SIMULATION GRID */}
      <div className="grid xl:grid-cols-4 gap-8">
        
        {/* PARAMETER CONTROLS */}
        <div className="xl:col-span-1 bg-slate-900/40 backdrop-blur-md p-8 rounded-[2.5rem] border border-slate-800/60 flex flex-col h-[750px] group transition-all hover:bg-slate-900/60">
           <div className="flex items-center justify-between mb-8">
             <h2 className="text-xl font-bold text-white flex items-center gap-3">
               <Settings2 size={20} className="text-indigo-400" />
               Control Deck
             </h2>
             <div className="p-1 px-3 rounded-full bg-indigo-500/10 text-indigo-400 text-[10px] font-black uppercase">Active</div>
           </div>

           <div className="flex-1 space-y-8 overflow-y-auto pr-2 custom-scrollbar">
             {Object.entries(sliders).map(([sector, value]) => (
               <div key={sector} className="space-y-3 p-4 rounded-2xl bg-black/20 border border-white/5 transition-colors hover:border-white/10">
                 <div className="flex justify-between items-center">
                   <span className="text-xs font-black uppercase tracking-widest text-slate-400 capitalize">{sector}</span>
                   <span className={cn(
                     "text-sm font-mono font-bold",
                     value > 0.7 ? "text-red-500" : value > 0.4 ? "text-amber-500" : "text-emerald-500"
                   )}>{(value * 100).toFixed(0)}%</span>
                 </div>
                 <div className="relative flex items-center">
                    <input
                      type="range"
                      min="0"
                      max="1"
                      step="0.01"
                      value={value}
                      onChange={handleSliderChange(sector)}
                      className="w-full h-1.5 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-indigo-500 transition-all hover:accent-indigo-400"
                    />
                 </div>
               </div>
             ))}
           </div>
        </div>

        {/* RESULTS INTERFACE */}
        <div className="xl:col-span-3 space-y-8">
           <div className="grid lg:grid-cols-2 gap-8 h-[550px]">
              {/* PROPAGATION GRAPH */}
              <div className="bg-slate-900/40 backdrop-blur-md p-8 rounded-[2.5rem] border border-slate-800/60 flex flex-col group">
                 <div className="flex items-center justify-between mb-8">
                    <h2 className="text-xl font-bold text-white flex items-center gap-3">
                      <Activity size={20} className="text-emerald-400" />
                      Ripple Visualization
                    </h2>
                 </div>
                 <div className="flex-1 relative bg-black/20 rounded-[2rem] overflow-hidden">
                   {loading ? (
                     <div className="absolute inset-0 flex flex-col items-center justify-center space-y-4">
                        <div className="w-12 h-12 border-4 border-amber-500/10 border-t-amber-500 rounded-full animate-spin"></div>
                        <p className="text-amber-500 text-xs font-black animate-pulse">RECURSION ENGINE ACTIVE...</p>
                     </div>
                   ) : results ? (
                     <EnhancedCascadingNetwork
                       graphData={networkData}
                       riskScores={Object.fromEntries(
                         Object.entries(results.final).map(([k, v]) => [k, { score: v }])
                       )}
                     />
                   ) : (
                     <div className="absolute inset-0 flex flex-col items-center justify-center text-center p-12">
                        <div className="p-5 rounded-3xl bg-slate-800/50 text-slate-600 mb-4">
                           <Play size={32} />
                        </div>
                        <p className="text-slate-500 font-bold">Initiate simulation to witness systemic cascade effects.</p>
                     </div>
                   )}
                 </div>
              </div>

              {/* MAGNITUDE CHART */}
              <div className="bg-slate-900/40 backdrop-blur-md p-8 rounded-[2.5rem] border border-slate-800/60 flex flex-col">
                 <div className="flex items-center justify-between mb-8">
                    <h2 className="text-xl font-bold text-white flex items-center gap-3">
                      <BarChart size={20} className="text-purple-400" />
                      Magnitude Delta
                    </h2>
                 </div>
                 <div className="flex-1">
                   {results ? (
                     <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={chartData}>
                           <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                           <XAxis 
                             dataKey="sector" 
                             stroke="#475569" 
                             fontSize={10} 
                             tickLine={false} 
                             axisLine={false}
                           />
                           <YAxis 
                             stroke="#475569" 
                             fontSize={10} 
                             tickLine={false} 
                             axisLine={false}
                           />
                           <Tooltip 
                             cursor={{ fill: '#ffffff05' }}
                             contentStyle={{ 
                               backgroundColor: "#0f172a", 
                               border: "1px solid #1e293b",
                               borderRadius: "16px",
                             }}
                           />
                           <Bar dataKey="risk" radius={[8, 8, 8, 8]}>
                              {chartData.map((entry, index) => (
                                <Cell 
                                  key={`cell-${index}`} 
                                  fill={entry.risk > 0.7 ? '#ef4444' : entry.risk > 0.4 ? '#f59e0b' : '#6366f1'} 
                                  fillOpacity={0.8}
                                />
                              ))}
                           </Bar>
                        </BarChart>
                     </ResponsiveContainer>
                   ) : (
                     <div className="h-full flex items-center justify-center text-slate-700 italic border-2 border-dashed border-slate-800/50 rounded-[2rem]">
                        Awaiting data stream...
                     </div>
                   )}
                 </div>
              </div>
           </div>

           {/* DYNAMIC INSIGHTS & STATS */}
           <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              {/* INSIGHTS */}
              <div className="bg-slate-900/40 p-8 rounded-[2.5rem] border border-slate-800/60 relative overflow-hidden">
                 <div className="absolute top-0 right-0 p-8 opacity-10">
                    <Sparkles size={100} />
                 </div>
                 <h3 className="text-xl font-bold text-white mb-6">Simulation Insights</h3>
                 
                 {insights ? (
                    <div className="space-y-4">
                       <div className="flex items-start gap-4 p-4 bg-red-500/10 rounded-2xl border border-red-500/20">
                          <ShieldAlert className="text-red-500 shrink-0" size={20} />
                          <div>
                            <p className="text-[10px] font-black uppercase text-red-500/70 mb-1">Critical Failure Point</p>
                            <p className="text-white font-bold leading-none">The <span className="uppercase text-red-400">{insights.highest[0]}</span> sector hit a risk magnitude of {(insights.highest[1] * 100).toFixed(1)}%.</p>
                          </div>
                       </div>
                       <div className="flex items-start gap-4 p-4 bg-indigo-500/10 rounded-2xl border border-indigo-500/20">
                          <Info className="text-indigo-400 shrink-0" size={20} />
                          <div>
                            <p className="text-[10px] font-black uppercase text-indigo-400/70 mb-1">System Resilience</p>
                            <p className="text-white font-bold leading-none"><span className="uppercase text-indigo-300">{insights.lowest[0]}</span> remains the most stable anchor under current shock parameters.</p>
                          </div>
                       </div>
                    </div>
                 ) : (
                    <p className="text-slate-500 italic">No simulation data generated yet.</p>
                 )}
              </div>

              {/* CURRENT SIMULATION STATUS */}
              <div className="bg-gradient-to-br from-indigo-500 to-purple-600 p-8 rounded-[2.5rem] text-white flex flex-col justify-between shadow-xl shadow-indigo-500/20">
                 <div>
                    <p className="text-[10px] font-black uppercase text-indigo-100/60 tracking-widest mb-2 text-center">Engine Status</p>
                    <div className="flex items-center justify-center gap-6 mb-6">
                       <div className="flex flex-col items-center">
                          <span className="text-3xl font-black">{loading ? "BUSY" : activeSimulation ? "DONE" : "IDLE"}</span>
                          <span className="text-[10px] uppercase font-bold text-indigo-200">State</span>
                       </div>
                       <div className="h-10 w-[1px] bg-white/20" />
                       <div className="flex flex-col items-center text-center">
                          <span className="text-sm font-bold uppercase truncate max-w-[150px]">{activeSimulation || "N/A"}</span>
                          <span className="text-[10px] uppercase font-bold text-indigo-200">Active Test</span>
                       </div>
                    </div>
                 </div>
                 
                 <button className="w-full flex items-center justify-between p-4 bg-white/10 backdrop-blur-md rounded-2xl text-white font-black border border-white/20 transition-all hover:bg-white/20 group">
                    View Topology Map
                    <ChevronRight className="transition-transform group-hover:translate-x-1" />
                 </button>
              </div>
           </div>
        </div>
      </div>

      {/* DETAILED RESULTS CARDS (GRID) */}
      {results && (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-6">
          {Object.entries(results.final).map(([sector, val]) => (
            <RiskCard
              key={sector}
              sector={sector}
              risk={val}
              initialRisk={results.initial?.[sector]}
            />
          ))}
        </div>
      )}

      <style jsx>{`
        .no-scrollbar::-webkit-scrollbar { display: none; }
        .no-scrollbar { -ms-overflow-style: none; scrollbar-width: none; }
        .custom-scrollbar::-webkit-scrollbar { width: 4px; }
        .custom-scrollbar::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 10px; }
      `}</style>
    </div>
  );
}