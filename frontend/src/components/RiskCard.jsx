import React from "react";
import { TrendingUp, TrendingDown, AlertCircle, ShieldAlert, Activity } from "lucide-react";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

export default function RiskCard({ sector, risk, initialRisk }) {
  const change = risk - initialRisk;
  const isIncreasing = change > 0;
  
  const getRiskColor = (val) => {
    if (val >= 0.8) return "text-red-500";
    if (val >= 0.6) return "text-orange-500";
    if (val >= 0.3) return "text-amber-500";
    return "text-emerald-500";
  };

  const getRiskBg = (val) => {
    if (val >= 0.8) return "bg-red-500/10 border-red-500/20";
    if (val >= 0.6) return "bg-orange-500/10 border-orange-500/20";
    if (val >= 0.3) return "bg-amber-500/10 border-amber-500/20";
    return "bg-emerald-500/10 border-emerald-500/20";
  };

  const getIcon = (val) => {
    if (val >= 0.8) return <ShieldAlert className="h-5 w-5 text-red-500" />;
    if (val >= 0.5) return <AlertCircle className="h-5 w-5 text-orange-500" />;
    return <Activity className="h-5 w-5 text-emerald-500" />;
  };

  return (
    <div className={cn(
      "glass-card p-5 group flex flex-col justify-between min-h-[160px] transition-all duration-300",
      "hover:ring-1 hover:ring-indigo-500/30"
    )}>
      <div className="flex items-start justify-between">
        <div className="flex flex-col gap-1">
          <span className="text-xs font-bold uppercase tracking-widest text-slate-500">{sector}</span>
          <div className="flex items-center gap-2">
            <h3 className="text-2xl font-bold text-white">{(risk * 100).toFixed(1)}%</h3>
            {getIcon(risk)}
          </div>
        </div>
        
        <div className={cn(
          "flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-bold",
          isIncreasing ? "bg-red-500/20 text-red-400" : "bg-emerald-500/20 text-emerald-400"
        )}>
          {isIncreasing ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
          {Math.abs(change * 100).toFixed(1)}%
        </div>
      </div>

      <div className="mt-4 space-y-2">
        <div className="flex justify-between text-[10px] uppercase tracking-tighter text-slate-400">
          <span>Current Exposure</span>
          <span>Target {"<"} 30%</span>
        </div>
        <div className="h-1.5 w-full overflow-hidden rounded-full bg-white/5">
          <div 
            className={cn(
              "h-full transition-all duration-1000 ease-out",
              risk >= 0.8 ? "bg-red-500" : risk >= 0.6 ? "bg-orange-500" : risk >= 0.3 ? "bg-amber-500" : "bg-emerald-500"
            )}
            style={{ width: `${risk * 100}%` }}
          />
        </div>
      </div>
    </div>
  );
}
