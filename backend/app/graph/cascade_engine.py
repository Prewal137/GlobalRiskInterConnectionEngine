"""
🌊 Cascade Engine for Graph Interconnection System

Simulates cascading risk propagation through the learned graph.
Risk flows from source sectors to target sectors based on learned weights.

Author: Global Risk Platform
Date: 2026-04-05
"""

import networkx as nx
import numpy as np
from typing import Dict, List, Tuple, Optional
import pandas as pd


def run_cascade(graph: nx.DiGraph, 
                risk_dict: Dict[str, float], 
                steps: int = 5,
                damping: float = 0.8) -> Tuple[Dict[str, float], List[Dict[str, float]]]:
    """
    Run cascading risk simulation through the graph.
    
    For each step:
        new_risk[target] += source_risk * learned_weight * damping
    
    Args:
        graph: NetworkX DiGraph with learned edge weights
        risk_dict: Initial risk values {sector: risk}
        steps: Number of cascade steps (default: 5)
        damping: Damping factor to prevent infinite growth (default: 0.8)
        
    Returns:
        Tuple of (final_risk_dict, history_list)
    """
    print("\n" + "="*70)
    print("🌊 RUNNING CASCADE SIMULATION")
    print("="*70)
    
    # Initialize risk values
    current_risk = risk_dict.copy()
    
    # Ensure all nodes have risk values
    for node in graph.nodes():
        if node not in current_risk:
            current_risk[node] = 0.0
    
    # Store history
    history = [current_risk.copy()]
    
    print(f"   Initial risk values:")
    for sector, risk in sorted(current_risk.items()):
        print(f"      {sector:20s}: {risk:.4f}")
    
    print(f"\n   Cascade parameters:")
    print(f"      Steps: {steps}")
    print(f"      Damping: {damping}")
    
    # Run cascade
    for step in range(1, steps + 1):
        new_risk = current_risk.copy()
        
        # For each node, accumulate risk from its predecessors
        for target in graph.nodes():
            predecessors = list(graph.predecessors(target))
            
            for source in predecessors:
                if source in current_risk:
                    weight = graph[source][target].get('weight', 0.0)
                    # Risk propagation: source_risk * weight * damping
                    risk_transfer = current_risk[source] * weight * damping
                    new_risk[target] += risk_transfer
        
        # Normalize to prevent unbounded growth
        max_risk = max(new_risk.values())
        if max_risk > 1.0:
            for sector in new_risk:
                new_risk[sector] /= max_risk
        
        # Ensure non-negative
        for sector in new_risk:
            new_risk[sector] = max(0.0, new_risk[sector])
        
        current_risk = new_risk
        history.append(current_risk.copy())
        
        # Print step summary
        total_risk = sum(current_risk.values())
        max_sector = max(current_risk, key=current_risk.get)
        print(f"\n   Step {step}/{steps}:")
        print(f"      Total risk: {total_risk:.4f}")
        print(f"      Highest risk: {max_sector} ({current_risk[max_sector]:.4f})")
    
    print(f"\n✅ Cascade simulation complete ({steps} steps)")
    
    return current_risk, history


def print_cascade_summary(initial_risk: Dict[str, float], 
                         final_risk: Dict[str, float],
                         history: List[Dict[str, float]]):
    """
    Print a comprehensive summary of cascade effects.
    
    Args:
        initial_risk: Risk values before cascade
        final_risk: Risk values after cascade
        history: Full history of risk values
    """
    print("\n" + "="*70)
    print("📊 CASCADE SIMULATION SUMMARY")
    print("="*70)
    
    # Calculate changes
    changes = {}
    for sector in initial_risk:
        initial = initial_risk.get(sector, 0.0)
        final = final_risk.get(sector, 0.0)
        change = final - initial
        change_pct = (change / initial * 100) if initial > 0 else float('inf') if change > 0 else 0
        changes[sector] = {
            'initial': initial,
            'final': final,
            'absolute_change': change,
            'percent_change': change_pct
        }
    
    # Print table
    print(f"\n{'Sector':<20s} {'Initial':>10s} {'Final':>10s} {'Change':>10s} {'Change %':>10s}")
    print("-" * 65)
    
    for sector in sorted(changes.keys()):
        data = changes[sector]
        print(f"{sector:<20s} {data['initial']:>10.4f} {data['final']:>10.4f} "
              f"{data['absolute_change']:>+10.4f} {data['percent_change']:>+9.2f}%")
    
    # Find most affected sectors
    sorted_by_change = sorted(changes.items(), key=lambda x: abs(x[1]['percent_change']), reverse=True)
    
    print(f"\n🔝 Most Affected Sectors (by % change):")
    for i, (sector, data) in enumerate(sorted_by_change[:5], 1):
        print(f"   {i:2d}. {sector:20s}: {data['percent_change']:+.2f}%")
    
    print("="*70)


def get_cascade_pathways(graph: nx.DiGraph, 
                        source_sector: str, 
                        top_k: int = 5) -> List[Tuple[List[str], float]]:
    """
    Find the strongest cascade pathways from a source sector.
    
    Args:
        graph: NetworkX DiGraph
        source_sector: Starting sector
        top_k: Number of top pathways to return
        
    Returns:
        List of (pathway, pathway_weight) tuples
    """
    pathways = []
    
    # Find all paths from source
    for target in graph.nodes():
        if target != source_sector:
            try:
                # Find shortest path
                path = nx.shortest_path(graph, source=source_sector, target=target)
                
                # Calculate path weight (product of edge weights)
                path_weight = 1.0
                for i in range(len(path) - 1):
                    path_weight *= graph[path[i]][path[i+1]].get('weight', 0.0)
                
                pathways.append((path, path_weight))
            except nx.NetworkXNoPath:
                continue
    
    # Sort by weight
    pathways.sort(key=lambda x: x[1], reverse=True)
    
    return pathways[:top_k]


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from graph.risk_loader import load_latest_risk
    from graph.weight_learner import learn_weights
    from graph.graph_builder import build_graph
    
    # Load data and build graph
    risk_dict = load_latest_risk()
    from graph.risk_loader import load_risk_timeseries
    df = load_risk_timeseries()
    weights = learn_weights(df, method='regression')
    G = build_graph(weights)
    
    # Run cascade
    final_risk, history = run_cascade(G, risk_dict, steps=5, damping=0.8)
    
    # Print summary
    print_cascade_summary(risk_dict, final_risk, history)
