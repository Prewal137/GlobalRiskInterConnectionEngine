import React, { useState, useEffect } from "react";
import { 
  getTradeSummary, 
  getEconomySummary, 
  getGeopoliticsGlobalSummary, 
  getMigrationSummary, 
  getInterconnectionSummary,
  getInfraSummary 
} from "../services/api";
import { 
  Binary, 
  TrendingDown, 
  ShieldAlert, 
  PlaneLanding, 
  Construction, 
  Zap, 
  Globe, 
  ChevronRight,
  TrendingUp,
  AlertTriangle,
  Activity,
  ArrowRight
} from "lucide-react";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

const SectorStat = ({ title, value, status, icon: Icon, colorClass }) => (
  <div className="glass-card p-8 group relative overflow-hidden transition-all hover:scale-[1.02]">
    <div className={cn("absolute -right-4 -top-4 w-24 h-24 rounded-full opacity-5 group-hover:opacity-10 transition-opacity", colorClass)} />
    <div className="flex items-center justify-between mb-6">
      <div className={cn("p-4 rounded-2xl", colorClass + "/10", colorClass.replace('bg-', 'text-'))}>
        <Icon size={24} />
      </div>
      <div className={cn("px-4 py-1.5 rounded-full text-[10px] font-black uppercase tracking-widest border", 
        status === "High" ? "bg-red-500/10 text-red-500 border-red-500/20" : "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
      )}>
        {status} RISK
      </div>
    </div>
    <p className="text-[10px] font-black text-slate-500 uppercase tracking-[0.2em] mb-1">{title}</p>
    <div className="flex items-baseline gap-2">
      <h3 className="text-4xl font-black text-white">{(value * 100).toFixed(1)}%</h3>
      <span className="text-xs font-bold text-slate-500">Systemic Load</span>
    </div>
    <div className="mt-8 flex items-center justify-between text-xs font-bold">
      <span className="text-slate-500">Stability Index</span>
      <span className="text-white">{(100 - value * 100).toFixed(0)}pts</span>
    </div>
    <div className="mt-2 h-1.5 w-full bg-slate-800 rounded-full overflow-hidden">
      <div 
        className={cn("h-full transition-all duration-1000", value > 0.7 ? "bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.5)]" : "bg-indigo-500 shadow-[0_0_8px_rgba(99,102,241,0.5)]")} 
        style={{ width: `${value * 100}%` }}
      />
    </div>
  </div>
);

export default function SystemAudit() {
  const [summaries, setSummaries] = useState({
    trade: 0.45,
    economy: 0.32,
    geopolitics: 0.68,
    migration: 0.25,
    interconnection: 0.55,
    infra: 0.42
  });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchAllSummaries();
  }, []);

  const fetchAllSummaries = async () => {
    setLoading(true);
    try {
      const [t, e, g, m, i, inf] = await Promise.all([
        getTradeSummary().catch(err => { console.error("Trade API Error:", err); return {}; }),
        getEconomySummary("IND").catch(err => { console.error("Economy API Error:", err); return {}; }),
        getGeopoliticsGlobalSummary().catch(err => { console.error("Geopolitics API Error:", err); return {}; }),
        getMigrationSummary("IND").catch(err => { console.error("Migration API Error:", err); return {}; }),
        getInterconnectionSummary().catch(err => { console.error("Interconnection API Error:", err); return {}; }),
        getInfraSummary().catch(err => { console.error("Infra API Error:", err); return {}; })
      ]);

      setSummaries({
        trade: t?.mean_trade_risk || 0.45,
        economy: e?.summary?.risk_statistics?.mean_predicted_risk || 0.32,
        geopolitics: g?.mean_risk || 0.68,
        migration: 0.25, 
        interconnection: i?.mean_composite_risk || 0.55,
        infra: 0.42 
      });
    } catch (err) {
      console.error("Audit Fetch Error:", err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-12 pb-32">
      {/* HUD Header */}
      <div className="relative overflow-hidden rounded-[3rem] bg-indigo-600 p-12 transition-all group">
        <div className="absolute top-0 right-0 p-12 opacity-20 transition-transform group-hover:scale-110 group-hover:rotate-12">
            <Globe size={300} />
        </div>
        <div className="relative z-10 space-y-6">
          <div className="flex items-center gap-2 text-indigo-200">
            <div className="h-2 w-2 rounded-full bg-indigo-200 animate-pulse" />
            <span className="text-[10px] font-black uppercase tracking-[0.4em]">Integrated Risk Sync v1.0</span>
          </div>
          <h1 className="text-6xl font-black tracking-tighter text-white lg:max-w-xl leading-[0.9]">
            Systemic <span className="text-indigo-900/40">Audit</span> Terminal
          </h1>
          <p className="text-xl text-indigo-100/80 max-w-xl font-medium leading-relaxed">
            Consolidated intelligence across global sectors. Real-time verification of neural prediction vectors.
          </p>
          <div className="flex gap-4 pt-4">
            <button className="px-8 py-4 bg-white text-black font-black rounded-2xl flex items-center gap-3 transition-transform hover:-translate-y-1">
                GENERATE GLOBAL REPORT <ArrowRight size={20} />
            </button>
            <button className="px-8 py-4 bg-indigo-500/20 backdrop-blur-md border border-white/10 text-white font-black rounded-2xl hover:bg-white/10 transition-all">
                EXPORT RAW VECTORS
            </button>
          </div>
        </div>
      </div>

      {loading ? (
        <div className="py-40 flex flex-col items-center justify-center gap-6">
           <div className="h-20 w-20 border-8 border-white/5 border-t-indigo-500 rounded-full animate-spin" />
           <p className="text-[10px] font-black uppercase text-slate-500 tracking-[0.5em] animate-pulse">Aggregating Signal Data...</p>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
            <SectorStat 
              title="Trade Dynamics" 
              value={summaries.trade} 
              status={summaries.trade > 0.6 ? "High" : "Low"} 
              icon={Binary} 
              colorClass="bg-indigo-500"
            />
            <SectorStat 
              title="Economic Resilience" 
              value={summaries.economy} 
              status={summaries.economy > 0.6 ? "High" : "Low"} 
              icon={TrendingDown} 
              colorClass="bg-emerald-500"
            />
            <SectorStat 
              title="Geopolitical Flux" 
              value={summaries.geopolitics} 
              status={summaries.geopolitics > 0.6 ? "High" : "Low"} 
              icon={ShieldAlert} 
              colorClass="bg-rose-500"
            />
            <SectorStat 
              title="Migration Pressure" 
              value={summaries.migration} 
              status={summaries.migration > 0.6 ? "High" : "Low"} 
              icon={PlaneLanding} 
              colorClass="bg-amber-500"
            />
            <SectorStat 
              title="Critical Infrastructure" 
              value={summaries.infra} 
              status={summaries.infra > 0.6 ? "High" : "Low"} 
              icon={Construction} 
              colorClass="bg-cyan-500"
            />
            <SectorStat 
              title="Systemic Overload" 
              value={summaries.interconnection} 
              status={summaries.interconnection > 0.6 ? "High" : "Low"} 
              icon={Zap} 
              colorClass="bg-purple-500"
            />
          </div>

          {/* LOWER ANALYSIS SECTION */}
          <div className="grid lg:grid-cols-2 gap-12 pt-12">
             <div className="bg-slate-900/40 p-12 rounded-[3.5rem] border border-white/5 space-y-8">
                <div className="flex items-center justify-between">
                   <h2 className="text-3xl font-black text-white">Global Anomalies</h2>
                   <div className="p-3 rounded-2xl bg-amber-500/10 text-amber-500">
                      <AlertTriangle size={24} />
                   </div>
                </div>
                <div className="space-y-4">
                   {[
                     { label: "Energy Supply Chain", impact: "Critical", trend: "Up" },
                     { label: "Cyber-Sovereignty Index", impact: "Moderate", trend: "Stable" },
                     { label: "Oceanic Trade Routes", impact: "Elevated", trend: "Up" },
                     { label: "Agricultural Stability", impact: "Nominal", trend: "Down" }
                   ].map((item, idx) => (
                     <div key={idx} className="flex items-center justify-between p-6 bg-white/5 rounded-3xl group hover:bg-white/10 transition-all cursor-pointer">
                        <div className="flex items-center gap-6">
                           <div className="h-12 w-12 flex items-center justify-center rounded-2xl bg-slate-800 text-slate-400 group-hover:text-white transition-colors">
                              <Activity size={20} />
                           </div>
                           <div>
                              <p className="text-lg font-bold text-white leading-none">{item.label}</p>
                              <p className="text-xs text-slate-500 mt-1 uppercase font-black tracking-widest">{item.impact} Impact</p>
                           </div>
                        </div>
                        <div className={cn("h-10 w-10 flex items-center justify-center rounded-xl", 
                          item.trend === "Up" ? "bg-red-500/10 text-red-500" : "bg-emerald-500/10 text-emerald-500"
                        )}>
                           {item.trend === "Up" ? <TrendingUp size={18} /> : <TrendingDown size={18} />}
                        </div>
                     </div>
                   ))}
                </div>
             </div>

             <div className="bg-gradient-to-br from-slate-900 to-black p-12 rounded-[3.5rem] border border-white/10 flex flex-col justify-between">
                <div className="space-y-8">
                   <div className="space-y-4">
                      <h2 className="text-4xl font-black text-white italic">Neural Insight</h2>
                      <div className="h-1.5 w-24 bg-indigo-500 rounded-full" />
                   </div>
                   <p className="text-2xl font-bold text-slate-400 leading-relaxed">
                      "Systemic cascades are currently trending towards <span className="text-white underline decoration-rose-500 underline-offset-8">Geopolitical Consolidation</span> over trade-based cooperation, indicating a shift in global risk anchors."
                   </p>
                   <div className="flex gap-4">
                      <div className="p-4 rounded-3xl bg-indigo-500/10 border border-indigo-500/20 text-indigo-400">
                         <span className="text-3xl font-black">0.84</span>
                         <p className="text-[10px] font-bold uppercase tracking-widest mt-1">Confidence</p>
                      </div>
                      <div className="p-4 rounded-3xl bg-purple-500/10 border border-purple-500/20 text-purple-400">
                        <span className="text-3xl font-black">1.2ms</span>
                        <p className="text-[10px] font-bold uppercase tracking-widest mt-1">Latency</p>
                      </div>
                   </div>
                </div>
                <button className="group mt-12 flex items-center justify-between p-6 bg-white rounded-3xl text-black font-black text-xl transition-all hover:bg-slate-100">
                   EXPLORE TOPOLOGY
                   <div className="h-12 w-12 bg-black rounded-2xl flex items-center justify-center text-white transition-transform group-hover:translate-x-2">
                      <ChevronRight size={24} />
                   </div>
                </button>
             </div>
          </div>
        </>
      )}
    </div>
  );
}
