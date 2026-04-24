import React, { useState, useEffect, useMemo } from "react";
import { getStateImpact, getClimateAllDistricts } from "../services/api";
import IndiaMap from "../components/IndiaMap";

import {
  MapPin,
  AlertCircle,
  RefreshCw,
  LayoutGrid,
} from "lucide-react";

import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

export default function StateAnalysis() {
  const [selectedState, setSelectedState] = useState("Karnataka");
  const [impactData, setImpactData] = useState(null);
  const [districts, setDistricts] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    handleStateClick("Karnataka");
    fetchDistricts();
  }, []);

  const fetchDistricts = async () => {
    try {
      const data = await getClimateAllDistricts();
      setDistricts(data);
    } catch (err) {
      console.error(err);
    }
  };

  // ✅ FIXED CRASH
  const currentDistricts = useMemo(() => {
    return districts
      .filter(
        (d) =>
          d?.state &&
          d.state.toLowerCase() === selectedState.toLowerCase()
      )
      .sort((a, b) => b.climate_risk_score - a.climate_risk_score);
  }, [districts, selectedState]);

  const handleStateClick = async (stateName) => {
    setSelectedState(stateName);
    setLoading(true);

    try {
      const impact = await getStateImpact(stateName);
      setImpactData(impact);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-black via-slate-950 to-black p-6 space-y-10">

      {/* HEADER */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-4xl font-black text-white">
            Regional Analysis
          </h1>
          <p className="text-slate-400 text-sm mt-1">
            Interactive risk intelligence dashboard
          </p>
        </div>

        <div className="flex items-center gap-3 bg-slate-900/60 border border-white/10 px-4 py-2 rounded-xl">
          <div>
            <p className="text-xs text-slate-400">Active</p>
            <p className="text-white font-bold">{selectedState}</p>
          </div>

          <button
            onClick={() => handleStateClick(selectedState)}
            className="p-2 bg-indigo-500 rounded-lg hover:bg-indigo-600 transition"
          >
            <RefreshCw
              className={cn("w-4 h-4 text-white", loading && "animate-spin")}
            />
          </button>
        </div>
      </div>

      {/* 🔥 MAIN GRID */}
      <div className="grid lg:grid-cols-3 gap-8">

        {/* MAP (MAIN HERO) */}
        <div className="lg:col-span-2 bg-slate-900/60 border border-white/10 rounded-2xl p-4 shadow-xl">

          <div className="flex justify-between mb-3">
            <h2 className="text-white font-bold flex gap-2 items-center">
              <MapPin className="text-indigo-400" size={18} />
              Risk Map
            </h2>
            <span className="text-xs text-slate-400">
              Click to explore
            </span>
          </div>

          <div className="h-[500px] rounded-xl overflow-hidden">
            <IndiaMap
              selectedState={selectedState}
              onStateClick={handleStateClick}
              stateRiskData={impactData?.final || {}}
            />
          </div>
        </div>

        {/* RIGHT PANEL */}
        <div className="bg-slate-900/60 border border-white/10 rounded-2xl p-6 flex flex-col gap-6">

          <div>
            <p className="text-xs text-indigo-400 uppercase">
              Selected
            </p>
            <h2 className="text-2xl font-bold text-white">
              {selectedState}
            </h2>
          </div>

          {/* TOP SECTORS */}
          <div className="space-y-3">
            {impactData?.final &&
              Object.entries(impactData.final)
                .slice(0, 5)
                .map(([sector, value]) => (
                  <div key={sector}>
                    <div className="flex justify-between text-xs text-slate-400 mb-1">
                      <span>{sector}</span>
                      <span>{(value * 100).toFixed(0)}%</span>
                    </div>

                    <div className="h-2 bg-white/10 rounded-full overflow-hidden">
                      <div
                        className="h-full transition-all duration-500"
                        style={{
                          width: `${value * 100}%`,
                          background:
                            value > 0.7
                              ? "#ef4444"
                              : value > 0.4
                                ? "#f59e0b"
                                : "#10b981",
                        }}
                      />
                    </div>
                  </div>
                ))}
          </div>
        </div>
      </div>

      {/* DISTRICT SECTION */}
      <div className="bg-slate-900/60 border border-white/10 rounded-2xl p-6">

        <div className="flex justify-between items-center mb-6">
          <h2 className="text-xl font-bold text-white flex gap-2 items-center">
            <LayoutGrid className="text-emerald-400" />
            Districts: {selectedState}
          </h2>

          <span className="text-xs text-emerald-400">
            {currentDistricts.length} active
          </span>
        </div>

        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4">

          {currentDistricts.map((d, i) => (
            <div
              key={i}
              className="bg-black/40 border border-white/10 rounded-xl p-4 hover:border-indigo-500/30 transition"
            >
              <p className="text-xs text-slate-400">{d.district}</p>

              <div className="mt-2 flex justify-between text-sm">
                <span className="text-white font-bold">
                  {(d.climate_risk_score * 100).toFixed(1)}%
                </span>

                <span
                  className={cn(
                    "text-xs font-bold",
                    d.risk_level === "VERY HIGH"
                      ? "text-red-500"
                      : d.risk_level === "HIGH"
                        ? "text-orange-400"
                        : "text-emerald-400"
                  )}
                >
                  {d.risk_level}
                </span>
              </div>

              <div className="mt-2 h-1 bg-white/10 rounded-full">
                <div
                  className="h-full"
                  style={{
                    width: `${d.climate_risk_score * 100}%`,
                    background:
                      d.risk_level === "VERY HIGH"
                        ? "#ef4444"
                        : d.risk_level === "HIGH"
                          ? "#f59e0b"
                          : "#10b981",
                  }}
                />
              </div>
            </div>
          ))}

          {currentDistricts.length === 0 && (
            <div className="col-span-full text-center py-10 text-slate-500">
              <AlertCircle className="mx-auto mb-2" />
              No district data
            </div>
          )}
        </div>
      </div>

      {/* SECTOR SUMMARY */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4">
        {impactData?.final &&
          Object.entries(impactData.final).map(([sector, value]) => (
            <div
              key={sector}
              className="bg-slate-900/60 border border-white/10 rounded-xl p-4 text-center"
            >
              <p className="text-xs text-slate-400">{sector}</p>
              <p className="text-lg font-bold text-white">
                {(value * 100).toFixed(1)}%
              </p>
            </div>
          ))}
      </div>
    </div>
  );
}