import React, { useState, useEffect } from "react";
import { getHistoricalRisk, getTrendData } from "../services/api";
import EnhancedCascadingNetwork from "../components/EnhancedCascadingNetwork";
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer 
} from "recharts";
import { Calendar, PlayCircle, Table, Activity, TrendingUp } from "lucide-react";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

const years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];

export default function HistoricalPage() {
  const [loading, setLoading] = useState(true);
  const [selectedYear, setSelectedYear] = useState(2020);
  const [historicalData, setHistoricalData] = useState(null);
  const [trendData, setTrendData] = useState([]);
  const [error, setError] = useState(null);
  const [isAnimating, setIsAnimating] = useState(false);

  useEffect(() => {
    fetchData();
  }, [selectedYear]);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);
      const [historical, trend] = await Promise.all([
        getHistoricalRisk(selectedYear),
        getTrendData(),
      ]);
      setHistoricalData(historical);
      setTrendData(trend.trend || []);
      setLoading(false);
    } catch (err) {
      console.error("Error fetching historical data:", err);
      setError("Failed to load historical archives.");
      setLoading(false);
    }
  };

  const handleAnimate = () => {
    if (isAnimating) return;
    setIsAnimating(true);
    let index = years.indexOf(selectedYear);
    const interval = setInterval(() => {
      index++;
      if (index >= years.length) {
        clearInterval(interval);
        setIsAnimating(false);
        return;
      }
      setSelectedYear(years[index]);
    }, 1500);
  };

  if (loading && !isAnimating) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="h-10 w-10 animate-spin rounded-full border-4 border-indigo-500/20 border-t-indigo-500" />
      </div>
    );
  }

  return (
    <div className="space-y-8 pb-12">
      <div className="flex flex-col justify-between gap-6 md:flex-row md:items-end">
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-indigo-400">
            <Calendar className="h-4 w-4" />
            <span className="text-xs font-bold uppercase tracking-[0.2em]">Historical Repository</span>
          </div>
          <h1 className="text-4xl font-bold tracking-tight text-white font-display">
            Temporal <span className="text-indigo-400">Analysis</span>
          </h1>
          <p className="text-slate-400">Reviewing systemic risk evolution across the last decade.</p>
        </div>

        <div className="flex flex-wrap items-center gap-4">
          <div className="flex items-center gap-2 rounded-xl bg-white/5 p-1 border border-white/10">
            {years.map((year) => (
              <button
                key={year}
                onClick={() => setSelectedYear(year)}
                className={cn(
                  "px-3 py-1.5 text-sm font-bold rounded-lg transition-all",
                  selectedYear === year 
                    ? "bg-indigo-500 text-white shadow-lg shadow-indigo-500/20" 
                    : "text-slate-500 hover:text-slate-300"
                )}
              >
                {year}
              </button>
            ))}
          </div>
          <button 
            onClick={handleAnimate}
            disabled={isAnimating}
            className={cn(
              "flex items-center gap-2 rounded-xl px-5 py-2.5 font-bold transition-all",
              isAnimating 
                ? "bg-indigo-500/20 text-indigo-400 cursor-not-allowed" 
                : "bg-white text-black hover:scale-105 active:scale-95"
            )}
          >
            <PlayCircle className={cn("h-4 w-4", isAnimating && "animate-pulse")} />
            {isAnimating ? "Animating..." : "Animate"}
          </button>
        </div>
      </div>

      <div className="grid gap-8 lg:grid-cols-2">
        <div className="space-y-4">
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <Activity className="h-5 w-5 text-indigo-400" />
            System State ({selectedYear})
          </h2>
          <EnhancedCascadingNetwork 
            graphData={{
              nodes: [
                { id: "Climate", name: "Climate", baseRisk: 0.5 },
                { id: "Economy", name: "Economy", baseRisk: 0.5 },
                { id: "Trade", name: "Trade", baseRisk: 0.5 },
                { id: "Geopolitics", name: "Geopolitics", baseRisk: 0.5 },
                { id: "Migration", name: "Migration", baseRisk: 0.5 },
                { id: "Social", name: "Social", baseRisk: 0.5 },
              ],
              links: [] 
            }}
          />
        </div>

        <div className="space-y-4">
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-indigo-400" />
            Multi-Year Trends
          </h2>
          <div className="glass-card h-[600px] p-6">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={trendData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#ffffff05" vertical={false} />
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
                  tickFormatter={(val) => `${(val * 100).toFixed(0)}%`}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: "#0f172a", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "12px" }}
                  itemStyle={{ fontWeight: "bold" }}
                />
                <Legend iconType="circle" wrapperStyle={{ paddingTop: "20px" }} />
                <Line 
                  type="monotone" 
                  dataKey="global_risk" 
                  stroke="#6366f1" 
                  strokeWidth={3} 
                  dot={{ r: 4, fill: "#6366f1", strokeWidth: 0 }}
                  activeDot={{ r: 6, strokeWidth: 0 }}
                  name="Global Index"
                />
                <Line 
                  type="monotone" 
                  dataKey="economic_risk" 
                  stroke="#ef4444" 
                  strokeWidth={2} 
                  dot={false}
                  name="Economic"
                />
                <Line 
                  type="monotone" 
                  dataKey="social_risk" 
                  stroke="#f59e0b" 
                  strokeWidth={2} 
                  dot={false}
                  name="Social"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {historicalData?.data && (
        <div className="space-y-4">
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <Table className="h-5 w-5 text-indigo-400" />
            Raw Signal Data
          </h2>
          <div className="glass-card overflow-hidden">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-white/5 bg-white/5">
                  <th className="p-4 text-xs font-bold uppercase tracking-widest text-slate-500">Year/Month</th>
                  <th className="p-4 text-xs font-bold uppercase tracking-widest text-slate-500">Climate</th>
                  <th className="p-4 text-xs font-bold uppercase tracking-widest text-slate-500">Economy</th>
                  <th className="p-4 text-xs font-bold uppercase tracking-widest text-slate-500">Trade</th>
                  <th className="p-4 text-xs font-bold uppercase tracking-widest text-slate-500">Geopolitics</th>
                  <th className="p-4 text-xs font-bold uppercase tracking-widest text-slate-500">Social</th>
                  <th className="p-4 text-xs font-bold uppercase tracking-widest text-slate-500">Global Risk</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {historicalData.data.slice(0, 12).map((row, index) => (
                  <tr key={index} className="transition-colors hover:bg-white/5">
                    <td className="p-4 text-sm font-bold text-white">{row.Year} - {row.Month}</td>
                    <td className="p-4 text-sm text-slate-400">{(row.climate_risk * 100).toFixed(1)}%</td>
                    <td className="p-4 text-sm text-slate-400">{(row.economic_risk * 100).toFixed(1)}%</td>
                    <td className="p-4 text-sm text-slate-400">{(row.trade_risk * 100).toFixed(1)}%</td>
                    <td className="p-4 text-sm text-slate-400">{(row.geopolitical_risk * 100).toFixed(1)}%</td>
                    <td className="p-4 text-sm text-slate-400">{(row.social_risk * 100).toFixed(1)}%</td>
                    <td className="p-4">
                      <span className={cn(
                        "rounded-full px-3 py-1 text-xs font-bold",
                        row.global_risk >= 0.7 ? "bg-red-500/20 text-red-400" : "bg-indigo-500/20 text-indigo-400"
                      )}>
                        {(row.global_risk * 100).toFixed(1)}%
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
