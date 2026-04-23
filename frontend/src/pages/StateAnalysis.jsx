import React, { useState, useEffect, useMemo } from "react";
import { getStateRisk, getStateImpact } from "../services/api";
import IndiaMap from "../components/IndiaMap";
import EnhancedCascadingNetwork from "../components/EnhancedCascadingNetwork";

import {
  LineChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
} from "recharts";

import { MapPin, Activity, Zap, Shield, AlertCircle, RefreshCw, ChevronRight } from "lucide-react";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

export default function StateAnalysis() {
  const [selectedState, setSelectedState] = useState("Karnataka");
  const [impactData, setImpactData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    handleStateClick("Karnataka");
  }, []);

  const handleStateClick = async (stateName) => {
    setSelectedState(stateName);
    setLoading(true);
    setError(null);

    try {
      // We only use getStateImpact as it provides the most comprehensive data for the visualization
      const impact = await getStateImpact(stateName);
      setImpactData(impact);
    } catch (err) {
      console.error("State Fetch Error:", err);
      setError("Failed to fetch regional intelligence.");
    } finally {
      setLoading(false);
    }
  };

  const chartData = useMemo(() => {
    if (!impactData?.final) return [];
    return Object.entries(impactData.final).map(([key, val]) => ({
      name: key,
      risk: val,
    }));
  }, [impactData]);

  const networkData = useMemo(() => {
    if (!impactData?.final) return { nodes: [], links: [] };
    return {
      nodes: Object.keys(impactData.final).map((id) => ({
        id,
        name: id,
        baseRisk: impactData.initial?.[id] || 0.5,
      })),
      links: [],
    };
  }, [impactData]);

  return (
    <div className="space-y-10 pb-20">
      {/* HEADER SECTION */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div>
          <h1 className="text-4xl font-black tracking-tight text-white mb-2 bg-clip-text text-transparent bg-gradient-to-r from-indigo-400 to-emerald-400">
            Regional Analysis
          </h1>
          <p className="text-slate-400 max-w-xl">
             Deep-dive into localized risk vectors and systemic dependencies across the Indian subcontinent.
          </p>
        </div>
        
        <div className="flex items-center gap-4 bg-slate-900/50 p-2 pl-4 rounded-2xl border border-slate-800">
           <div className="flex flex-col">
              <span className="text-[10px] font-black text-slate-500 uppercase tracking-widest leading-none mb-1">Active Region</span>
              <span className="text-lg font-bold text-white">{selectedState}</span>
           </div>
           <button 
             onClick={() => handleStateClick(selectedState)}
             className="p-3 rounded-xl bg-indigo-500 text-white hover:bg-indigo-600 transition-colors"
           >
             <RefreshCw className={cn("w-4 h-4", loading && "animate-spin")} />
           </button>
        </div>
      </div>

      {/* TOP SECTION: MAP & NETWORK */}
      <div className="grid lg:grid-cols-2 gap-8">
        {/* GEOSPATIAL MAP */}
        <div className="bg-slate-900/40 backdrop-blur-md rounded-[2.5rem] border border-slate-800/60 p-8 flex flex-col h-[600px] group transition-all hover:bg-slate-900/60">
          <div className="flex items-center justify-between mb-8">
            <h2 className="text-xl font-bold text-white flex items-center gap-3">
              <div className="p-2 rounded-lg bg-indigo-500/10 text-indigo-400">
                <MapPin size={20} />
              </div>
              Geospatial Risk Mapping
            </h2>
            <div className="flex gap-2">
               <span className="flex items-center gap-1.5 text-[10px] font-black uppercase text-slate-500">
                 <div className="h-2 w-2 rounded-full bg-red-500" /> High 
               </span>
               <span className="flex items-center gap-1.5 text-[10px] font-black uppercase text-slate-500">
                 <div className="h-2 w-2 rounded-full bg-emerald-500" /> Low
               </span>
            </div>
          </div>

          <div className="flex-1 relative rounded-3xl overflow-hidden bg-black/20 p-4">
            <IndiaMap
              onStateClick={handleStateClick}
              stateRiskData={impactData?.final || {}}
            />
            
            <div className="absolute bottom-6 left-6 right-6 p-4 rounded-2xl bg-black/60 backdrop-blur-md border border-white/5 opacity-0 group-hover:opacity-100 transition-opacity">
               <p className="text-xs text-slate-400">Interactive: Click on any state to switch intelligence context.</p>
            </div>
          </div>
        </div>

        {/* CASCADE NETWORK */}
        <div className="bg-slate-900/40 backdrop-blur-md rounded-[2.5rem] border border-slate-800/60 p-8 flex flex-col h-[600px] overflow-hidden group">
          <div className="flex items-center justify-between mb-8">
             <h2 className="text-xl font-bold text-white flex items-center gap-3">
               <div className="p-2 rounded-lg bg-amber-500/10 text-amber-500">
                 <Zap size={20} />
               </div>
               Propagation Engine
             </h2>
             <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-white/5 text-slate-500">
                <Shield size={16} />
             </div>
          </div>

          <div className="flex-1 relative bg-black/20 rounded-3xl">
            {loading ? (
              <div className="absolute inset-0 flex flex-col items-center justify-center space-y-4">
                <div className="w-10 h-10 border-4 border-indigo-500/10 border-t-indigo-500 rounded-full animate-spin"></div>
                <p className="text-slate-500 text-xs animate-pulse">Recalculating Cascade...</p>
              </div>
            ) : (
              <EnhancedCascadingNetwork
                graphData={networkData}
                riskScores={Object.fromEntries(
                  Object.entries(impactData?.final || {}).map(([k, v]) => [
                    k,
                    { score: v },
                  ])
                )}
              />
            )}
          </div>
        </div>
      </div>

      {/* MID SECTION: SUMMARY CARDS */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4">
        {impactData?.final && Object.entries(impactData.final).map(([sector, value]) => (
          <div key={sector} className="group relative bg-slate-900/40 p-5 rounded-3xl border border-slate-800/60 hover:border-indigo-500/30 transition-all overflow-hidden">
            <div className={cn(
              "absolute -right-2 -top-2 w-12 h-12 rounded-full opacity-10",
              value > 0.7 ? "bg-red-500" : value > 0.4 ? "bg-amber-500" : "bg-emerald-500"
            )} />
            <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1 group-hover:text-slate-300">{sector}</p>
            <h2 className="text-2xl font-black text-white">
              {(value * 100).toFixed(1)}%
            </h2>
            <div className="mt-3 h-1 w-full bg-slate-800 rounded-full overflow-hidden">
               <div 
                 className={cn("h-full transition-all duration-1000", value > 0.7 ? "bg-red-500" : value > 0.4 ? "bg-amber-500" : "bg-emerald-500")}
                 style={{ width: `${value * 100}%` }}
               />
            </div>
          </div>
        ))}
      </div>

      {/* BOTTOM SECTION: ANALYTICS GRID */}
      <div className="grid lg:grid-cols-3 gap-8">
        {/* SECTOR COMPARISON CHART */}
        <div className="lg:col-span-2 bg-slate-900/40 backdrop-blur-md p-8 rounded-[2.5rem] border border-slate-800/60">
           <div className="flex items-center justify-between mb-8">
             <h2 className="text-xl font-bold text-white flex items-center gap-3">
               <Activity size={20} className="text-indigo-400" />
               Impact Spectrum
             </h2>
           </div>

           <div className="h-[350px]">
             {impactData && (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={chartData}>
                    <defs>
                      <linearGradient id="colorRisk" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#6366f1" stopOpacity={0.3}/>
                        <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                    <XAxis 
                      dataKey="name" 
                      stroke="#475569" 
                      fontSize={10} 
                      tickLine={false} 
                      axisLine={false}
                      interval={0}
                      tick={{ fill: "#94a3b8", fontWeight: 700 }}
                    />
                    <YAxis 
                      stroke="#475569" 
                      fontSize={10} 
                      tickLine={false} 
                      axisLine={false}
                      tickFormatter={(val) => (val * 100).toFixed(0) + "%"}
                    />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: "#0f172a", 
                        border: "1px solid #1e293b",
                        borderRadius: "16px",
                      }}
                    />
                    <Area
                      type="monotone"
                      dataKey="risk"
                      stroke="#6366f1"
                      strokeWidth={4}
                      fillOpacity={1}
                      fill="url(#colorRisk)"
                      animationDuration={1500}
                    />
                  </AreaChart>
                </ResponsiveContainer>
             )}
           </div>
        </div>

        {/* INSIGHT PANEL */}
        <div className="bg-gradient-to-br from-indigo-500 to-purple-600 p-8 rounded-[2.5rem] text-white flex flex-col justify-between shadow-xl shadow-indigo-500/20">
           <div>
              <div className="flex items-center gap-3 mb-6">
                <div className="p-2 rounded-xl bg-white/20">
                   <AlertCircle size={24} />
                </div>
                <h2 className="text-2xl font-black italic tracking-tighter">INTELLIGENCE</h2>
              </div>
              
              <div className="space-y-6">
                <div className="bg-black/20 backdrop-blur-md p-6 rounded-2xl border border-white/10">
                   <p className="text-[10px] font-black uppercase text-indigo-200 mb-2 opacity-60">Critical Path Detected</p>
                   <p className="text-xl font-bold capitalize">
                     {impactData?.final && Object.entries(impactData.final).sort((a,b) => b[1]-a[1])[0][0]} Sector
                   </p>
                   <p className="text-sm text-indigo-100/70 mt-1 leading-relaxed">
                     Shows the highest susceptibility to recursive failures in {selectedState}.
                   </p>
                </div>

                <div className="bg-black/20 backdrop-blur-md p-6 rounded-2xl border border-white/10">
                   <p className="text-[10px] font-black uppercase text-indigo-200 mb-2 opacity-60">Stability Zone</p>
                   <p className="text-xl font-bold capitalize">
                     {impactData?.final && Object.entries(impactData.final).sort((a,b) => a[1]-b[1])[0][0]} Sector
                   </p>
                   <p className="text-sm text-indigo-100/70 mt-1 leading-relaxed">
                     Demonstrates highest resilience against systemic shocks.
                   </p>
                </div>
              </div>
           </div>

           <button className="mt-8 group w-full flex items-center justify-between p-4 bg-white rounded-2xl text-black font-black transition-all hover:bg-slate-100">
              DOWNLOAD REPORT
              <div className="h-8 w-8 rounded-lg bg-indigo-500 flex items-center justify-center transition-transform group-hover:translate-x-1">
                 <ChevronRight className="text-white" size={18} />
              </div>
           </button>
        </div>
      </div>
    </div>
  );
}