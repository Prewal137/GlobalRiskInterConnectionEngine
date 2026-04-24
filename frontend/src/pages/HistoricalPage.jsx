import React, { useState, useEffect, useMemo } from "react";
import {
  getHistoricalSector,
  getClimateAllStates,
  getEconomyCountries,
  getSocialStates,
  getInfraAvailableStates,
  getGeopoliticsCountries,
} from "../services/api";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  AreaChart,
  Area,
} from "recharts";
import { 
  Globe, 
  TrendingUp, 
  Users, 
  Briefcase, 
  ShieldCheck, 
  CloudRain, 
  Truck, 
  Activity,
  Calendar,
  MapPin,
  RefreshCw,
  AlertCircle
} from "lucide-react";

/**
 * SECTORS CONFIGURATION
 */
const SECTORS = [
  { id: "global", label: "Global", icon: Globe, color: "#818cf8" },
  { id: "economy", label: "Economy", icon: Briefcase, color: "#f87171" },
  { id: "social", label: "Social", icon: Users, color: "#fbbf24" },
  { id: "migration", label: "Migration", icon: TrendingUp, color: "#34d399" },
  { id: "infrastructure", label: "Infrastructure", icon: ShieldCheck, color: "#60a5fa" },
  { id: "trade", label: "Trade", icon: Truck, color: "#c084fc" },
  { id: "climate", label: "Climate", icon: CloudRain, color: "#2dd4bf" },
  { id: "geopolitics", label: "Geopolitics", icon: Activity, color: "#fb7185" },
];

const YEARS = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];

export default function HistoricalPage() {
  const [selectedYear, setSelectedYear] = useState(2023);
  const [selectedSector, setSelectedSector] = useState("global");
  const [country, setCountry] = useState("IND");
  const [state, setState] = useState("Karnataka");
  
  const [trendData, setTrendData] = useState([]);
  const [historicalData, setHistoricalData] = useState([]);
  const [availableStates, setAvailableStates] = useState([]);
  const [availableCountries, setAvailableCountries] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [activeDataPoint, setActiveDataPoint] = useState(null);

  useEffect(() => {
    fetchAvailableEntities();
  }, []);

  const fetchAvailableEntities = async () => {
    try {
      const [statesRes, infraStatesRes, socialStatesRes, economyCountriesRes, geopoliticsCountriesRes] = await Promise.all([
        getClimateAllStates(),
        getInfraAvailableStates(),
        getSocialStates(),
        getEconomyCountries(),
        getGeopoliticsCountries()
      ]);

      const allStates = [...new Set([
        ...(statesRes?.map(s => s.state) || []),
        ...(infraStatesRes?.states || []),
        ...(socialStatesRes?.states || [])
      ])].sort();

      const allCountries = [...new Set([
        "IND", "USA", "CHN", "GBR", "FRA", "DEU", "JPN",
        ...(economyCountriesRes?.countries || []),
        ...(geopoliticsCountriesRes || [])
      ])].sort();

      setAvailableStates(allStates);
      setAvailableCountries(allCountries);
    } catch (err) {
      console.error("Failed to fetch entities:", err);
    }
  };

  useEffect(() => {
    fetchData();
  }, [selectedYear, selectedSector, country, state]);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);

      // Fetch historical breakdown for the selected year and sector (for the table)
      const historicalRes = await getHistoricalSector(selectedSector, {
        country,
        state,
        year: selectedYear
      });
      setHistoricalData(Array.isArray(historicalRes.data.data) ? historicalRes.data.data : []);

      // Fetch full trend for the chart
      const trendRes = await getHistoricalSector(selectedSector, {
        country,
        state
      });
      const data = Array.isArray(trendRes.data.data) ? trendRes.data.data : [];

      // Ensure data is always an array to prevent "slice is not a function" errors in Recharts
      setTrendData(Array.isArray(data) ? data : []);
      
    } catch (err) {
      console.error("Fetch Error:", err);
      setError("Failed to fetch historical data. Please check your connection.");
      setTrendData([]);
    } finally {
      setLoading(false);
    }
  };

  const currentSectorConfig = useMemo(() => 
    SECTORS.find(s => s.id === selectedSector), 
    [selectedSector]
  );

  const getLines = () => {
    const colorMap = {
      global: "#6366f1",
      economy: "#ef4444",
      social: "#f59e0b",
      migration: "#10b981",
      infrastructure: "#60a5fa",
      trade: "#c084fc",
      climate: "#2dd4bf",
      geopolitics: "#fb7185",
    };

    return [{ 
      key: "risk_score", 
      name: `${selectedSector.charAt(0).toUpperCase() + selectedSector.slice(1)} Risk`, 
      color: colorMap[selectedSector] || "#818cf8" 
    }];
  };

  const getActiveRiskScore = (row) => {
    if (!row) return 0;
    return row.risk_score || 0;
  };

  return (
    <div className="min-h-screen bg-[#030712] text-slate-200 pb-20">
      {/* HEADER */}
      <div className="max-w-7xl mx-auto px-6 pt-10">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-6 mb-10">
          <div>
            <h1 className="text-4xl font-black tracking-tight text-white mb-2 bg-clip-text text-transparent bg-gradient-to-r from-indigo-400 to-cyan-400">
              Historical Intelligence
            </h1>
            <p className="text-slate-400 max-w-2xl">
              Analyze multi-dimensional risk trends across 15+ years of geopolitical, economic, and environmental data.
            </p>
          </div>
          
          <div className="flex flex-wrap items-center gap-4">
            <div className="flex flex-col">
              <span className="text-[10px] font-black text-indigo-400 uppercase tracking-widest mb-1">Country</span>
              <select 
                value={country} 
                onChange={(e) => setCountry(e.target.value)}
                className="bg-slate-900 border border-slate-800 text-white text-xs font-bold rounded-xl px-4 py-2 opacity-80 hover:opacity-100 transition-opacity focus:outline-none focus:border-indigo-500"
              >
                {availableCountries.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>

            <div className="flex flex-col">
              <span className="text-[10px] font-black text-cyan-400 uppercase tracking-widest mb-1">Region / State</span>
              <select 
                value={state} 
                onChange={(e) => setState(e.target.value)}
                className="bg-slate-900 border border-slate-800 text-white text-xs font-bold rounded-xl px-4 py-2 opacity-80 hover:opacity-100 transition-opacity focus:outline-none focus:border-cyan-500"
              >
                {availableStates.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </div>

            <div className="h-10 w-[1px] bg-slate-800 mx-2 hidden md:block"></div>

            <button 
              onClick={fetchData}
              className="p-3 rounded-xl bg-indigo-500 text-white hover:bg-indigo-600 transition-all shadow-lg shadow-indigo-500/20"
              title="Sync Intelligence"
            >
              <RefreshCw className={`w-5 h-5 ${loading ? "animate-spin" : ""}`} />
            </button>
          </div>
        </div>

        {/* SECTOR TABS */}
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-3 mb-12">
          {SECTORS.map((s) => {
            const Icon = s.icon;
            const isActive = selectedSector === s.id;
            return (
              <button
                key={s.id}
                onClick={() => setSelectedSector(s.id)}
                className={`flex flex-col items-center justify-center p-4 rounded-2xl border transition-all duration-300 group ${
                  isActive 
                    ? "bg-slate-800/80 border-indigo-500 shadow-[0_0_20px_rgba(99,102,241,0.15)]" 
                    : "bg-slate-900/40 border-slate-800 hover:border-slate-700"
                }`}
              >
                <div className={`p-2 rounded-xl mb-3 transition-colors ${isActive ? "bg-indigo-500 text-white" : "bg-slate-800 text-slate-400 group-hover:text-slate-200"}`}>
                  <Icon className="w-5 h-5" />
                </div>
                <span className={`text-xs font-bold tracking-wide uppercase ${isActive ? "text-white" : "text-slate-500 group-hover:text-slate-400"}`}>
                  {s.label}
                </span>
              </button>
            );
          })}
        </div>

        {/* YEAR SELECTOR */}
        <div className="flex flex-wrap items-center gap-4 mb-8 bg-slate-900/50 p-2 rounded-2xl border border-slate-800 w-fit">
           <div className="flex items-center gap-2 px-4 py-2 text-slate-400">
             <Calendar className="w-4 h-4" />
             <span className="text-xs font-bold uppercase tracking-tight">Timeline</span>
           </div>
           <div className="flex gap-1 overflow-x-auto pb-1 sm:pb-0">
            {YEARS.map((y) => (
              <button
                key={y}
                onClick={() => setSelectedYear(y)}
                className={`px-4 py-2 rounded-xl text-sm font-bold transition-all ${
                  selectedYear === y 
                    ? "bg-white text-black shadow-lg shadow-white/10" 
                    : "text-slate-500 hover:text-slate-300 hover:bg-slate-800"
                }`}
              >
                {y}
              </button>
            ))}
          </div>
        </div>

        {/* MAIN CONTENT GRID */}
        <div className="grid grid-cols-1 xl:grid-cols-3 gap-8">
          
          {/* CHART AREA */}
          <div className="xl:col-span-2 space-y-6">
            <div className="bg-slate-900/40 backdrop-blur-md rounded-3xl border border-slate-800/60 p-8 h-[550px] relative overflow-hidden group">
              <div className="absolute inset-0 bg-gradient-to-br from-indigo-500/5 to-transparent pointer-events-none"></div>
              
              <div className="flex items-center justify-between mb-10 relative">
                <div>
                  <h3 className="text-xl font-bold text-white flex items-center gap-2">
                    {currentSectorConfig?.label || "Sector"} Trend Analysis
                    <span className="text-xs font-medium px-2 py-0.5 rounded-full bg-slate-800 text-slate-400 uppercase">
                      Predictive
                    </span>
                  </h3>
                  <p className="text-sm text-slate-500 capitalize">Historical data for {country} through {YEARS[YEARS.length - 1]}</p>
                </div>
              </div>

              {loading ? (
                <div className="h-full w-full flex flex-col items-center justify-center space-y-4">
                  <div className="w-12 h-12 border-4 border-indigo-500/20 border-t-indigo-500 rounded-full animate-spin"></div>
                  <p className="text-slate-500 text-sm animate-pulse">Processing metadata...</p>
                </div>
              ) : error ? (
                <div className="h-full w-full flex flex-col items-center justify-center text-center p-10">
                  <div className="p-4 rounded-full bg-red-500/10 text-red-500 mb-4">
                    <AlertCircle className="w-8 h-8" />
                  </div>
                  <h4 className="text-white font-bold mb-2">Sync Error</h4>
                  <p className="text-slate-500 text-sm max-w-xs">{error}</p>
                  <button onClick={fetchData} className="mt-6 px-6 py-2 bg-slate-800 rounded-full text-xs font-bold hover:bg-slate-700 transition-colors">Retry</button>
                </div>
              ) : trendData.length === 0 ? (
                <div className="h-full w-full flex flex-col items-center justify-center text-slate-500 italic">
                  No trend data available for this configuration.
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="90%" minHeight={400}>
                  <AreaChart 
                    data={trendData}
                    onClick={(data) => {
                      if (data && data.activePayload) {
                        setActiveDataPoint(data.activePayload[0].payload);
                      }
                    }}
                  >
                    <defs>
                      {getLines().map((line) => (
                        <linearGradient key={`grad-${line.key}`} id={`color-${line.key}`} x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor={line.color} stopOpacity={0.3}/>
                          <stop offset="95%" stopColor={line.color} stopOpacity={0}/>
                        </linearGradient>
                      ))}
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                    <XAxis 
                      dataKey="year" 
                      stroke="#475569" 
                      fontSize={12} 
                      tickLine={false}
                      axisLine={false}
                      dy={10}
                    />
                    <YAxis 
                       stroke="#475569" 
                       fontSize={12} 
                       tickLine={false}
                       axisLine={false}
                       tickFormatter={(val) => (val * 100).toFixed(0) + "%"}
                    />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: "#0f172a", 
                        border: "1px solid #1e293b",
                        borderRadius: "12px",
                        boxShadow: "0 10px 15px -3px rgba(0, 0, 0, 0.4)"
                      }}
                      itemStyle={{ fontSize: "12px", fontWeight: "bold" }}
                    />
                    <Legend iconType="circle" />

                    {getLines().map((line) => (
                      <Area
                        key={line.key}
                        type="monotone"
                        dataKey={line.key}
                        stroke={line.color}
                        strokeWidth={3}
                        fillOpacity={1}
                        fill={`url(#color-${line.key})`}
                        animationDuration={1500}
                      />
                    ))}
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>

            {/* QUICK STATS - Placeholder or removal of extra div */}
            

            {/* DETAIL PANEL (SHOWS ON CLICK) */}
            {activeDataPoint && (
              <div className="bg-gradient-to-r from-indigo-500 to-cyan-500 p-[1px] rounded-3xl animate-in fade-in slide-in-from-bottom-4 duration-500">
                <div className="bg-[#030712] p-8 rounded-[23px] flex flex-col md:flex-row items-center justify-between gap-8">
                  <div className="space-y-2">
                    <h4 className="text-sm font-black text-indigo-400 uppercase tracking-widest">Temporal Point Analysis</h4>
                    <p className="text-4xl font-black text-white">Year: {activeDataPoint.year || activeDataPoint.Year}</p>
                    <p className="text-slate-400">Granular metrics for the selected timestamp in {country}/{state}.</p>
                  </div>

                  <div className="flex flex-wrap gap-4">
                    {getLines().map(line => (
                      <div key={line.key} className="bg-slate-900 border border-slate-800 p-4 rounded-2xl min-w-[140px]">
                        <p className="text-[10px] font-bold text-slate-500 uppercase mb-1">{line.name}</p>
                        <p className="text-2xl font-black text-white">
                          {((activeDataPoint[line.key] || 0) * 100).toFixed(2)}%
                        </p>
                      </div>
                    ))}
                    <button 
                      onClick={() => setActiveDataPoint(null)}
                      className="px-6 py-2 bg-white text-black font-black rounded-xl text-xs hover:bg-slate-200 transition-colors"
                    >
                      CLEAR
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* TABLE AREA */}
          <div className="bg-slate-900/40 backdrop-blur-md rounded-3xl border border-slate-800/60 flex flex-col overflow-hidden">
            <div className="p-8 pb-4">
               <h3 className="text-xl font-bold text-white flex items-center gap-2 mb-1">
                 Risk Breakdown
               </h3>
               <p className="text-sm text-slate-500">Monthly granularity for {selectedYear}</p>
            </div>
            
            <div className="flex-1 overflow-auto custom-scrollbar">
              {historicalData.length > 0 ? (
                <table className="w-full text-left">
                  <thead className="sticky top-0 bg-slate-900/90 backdrop-blur-sm z-10 border-b border-slate-800">
                    <tr>
                      <th className="px-8 py-4 text-[10px] font-black uppercase tracking-widest text-slate-500">Period</th>
                      <th className="px-8 py-4 text-[10px] font-black uppercase tracking-widest text-slate-500">Risk Score</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/50">
                    {historicalData.map((row, i) => (
                      <tr key={i} className="hover:bg-slate-800/30 transition-colors group">
                        <td className="px-8 py-4">
                          <div className="flex flex-col">
                            <span className="text-white font-bold">{row.month || "N/A"}</span>
                            <span className="text-xs text-slate-500">{row.year}</span>
                          </div>
                        </td>
                        <td className="px-8 py-4">
                          <div className="flex items-center gap-3">
                            <div className="flex-1 h-1.5 bg-slate-800 rounded-full overflow-hidden min-w-[60px]">
                              <div 
                                className="h-full rounded-full transition-all duration-1000" 
                                style={{ 
                                  width: `${getActiveRiskScore(row) * 100}%`,
                                  backgroundColor: getActiveRiskScore(row) > 0.7 ? "#ef4444" : getActiveRiskScore(row) > 0.4 ? "#f59e0b" : "#10b981"
                                }}
                              ></div>
                            </div>
                            <span className="text-sm font-mono font-bold text-white">
                              {(getActiveRiskScore(row) * 100).toFixed(1)}%
                            </span>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <div className="h-full flex flex-col items-center justify-center p-10 text-center">
                   <div className="p-4 rounded-3xl bg-slate-800 text-slate-600 mb-4">
                     <Calendar className="w-8 h-8" />
                   </div>
                   <p className="text-slate-500 text-sm">Select a timeline with available historical data.</p>
                </div>
              )}
            </div>
            
            <div className="p-6 bg-slate-800/20 border-t border-slate-800">
               <div className="flex items-center gap-2 text-[10px] font-bold text-slate-500 uppercase tracking-tight">
                 <MapPin className="w-3 h-3 text-indigo-500" />
                 Last updated: {new Date().toLocaleTimeString()}
               </div>
            </div>
          </div>

        </div>
      </div>

      <style>{`
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
        .custom-scrollbar::-webkit-scrollbar-thumb:hover {
          background: #334155;
        }
      `}</style>
    </div>
  );
}