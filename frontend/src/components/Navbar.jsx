import React from "react";
import { Link, useLocation } from "react-router-dom";
import { Shield, BarChart3, History, Map, Zap, Settings } from "lucide-react";
import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs) {
  return twMerge(clsx(inputs));
}

const navItems = [
  { name: "Live Stream", path: "/", icon: BarChart3 },
  // { name: "System Audit", path: "/audit", icon: Shield },
  { name: "History", path: "/history", icon: History },
  { name: "Regional Lab", path: "/state", icon: Map },
  { name: "What-If Lab", path: "/whatif", icon: Zap },
];

export default function Navbar() {
  const location = useLocation();

  return (
    <nav className="sticky top-0 z-50 w-full border-b border-white/5 bg-[#050505]/80 backdrop-blur-xl">
      <div className="mx-auto flex h-20 max-w-7xl items-center justify-between px-6">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 shadow-lg shadow-indigo-500/20">
            <Shield className="h-6 w-6 text-white" />
          </div>
          <span className="text-xl font-bold tracking-tight text-white font-display">
            RISK<span className="text-indigo-500">INTEL</span>
          </span>
        </div>

        <div className="hidden items-center gap-1 md:flex">
          {navItems.map((item) => {
            const isActive = location.pathname === item.path;
            const Icon = item.icon;
            return (
              <Link
                key={item.path}
                to={item.path}
                className={cn(
                  "group relative flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-all duration-200",
                  isActive
                    ? "text-white"
                    : "text-slate-400 hover:text-white hover:bg-white/5"
                )}
              >
                <Icon className={cn("h-4 w-4 transition-transform group-hover:scale-110", isActive ? "text-indigo-400" : "text-slate-500")} />
                {item.name}
                {isActive && (
                  <span className="absolute -bottom-[21px] left-0 h-[2px] w-full bg-indigo-500 shadow-[0_0_12px_rgba(99,102,241,0.8)]" />
                )}
              </Link>
            );
          })}
        </div>

        {/* <div className="flex items-center gap-4">
          <button className="flex h-10 w-10 items-center justify-center rounded-full border border-white/5 bg-white/5 text-slate-400 transition-colors hover:bg-white/10 hover:text-white">
            <Settings className="h-5 w-5" />
          </button>
          <div className="h-10 w-10 rounded-full border-2 border-indigo-500/30 bg-gradient-to-tr from-slate-800 to-slate-900 p-0.5">
            <img
              src="https://api.dicebear.com/7.x/avataaars/svg?seed=risk"
              alt="Avatar"
              className="h-full w-full rounded-full"
            />
          </div>
        </div> */}
      </div>
    </nav>
  );
}
