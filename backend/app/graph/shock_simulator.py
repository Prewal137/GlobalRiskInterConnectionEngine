"""
💥 Shock Simulator for Graph Interconnection System

Simulates external shocks to individual sectors and analyzes cascading impacts.
Compares baseline vs shocked scenarios to quantify systemic risk.

Author: Global Risk Platform
Date: 2026-04-05
"""

import networkx as nx
import numpy as np
from typing import Dict, List, Tuple, Optional
from .cascade_engine import run_cascade, print_cascade_summary


def simulate_shock(graph: nx.DiGraph,
                   risk_dict: Dict[str, float],
                   shocked_sector: str,
                   shock_value: float,
                   steps: int = 5,
                   damping: float = 0.8) -> Dict:
    """
    Simulate a shock to a specific sector and analyze cascading effects.
    
    Args:
        graph: NetworkX DiGraph with learned edge weights
        risk_dict: Baseline risk values {sector: risk}
        shocked_sector: Sector to shock
        shock_value: New risk value for the shocked sector (0-1 scale)
        steps: Number of cascade steps
        damping: Damping factor
        
    Returns:
        Dictionary with shock analysis results
    """
    print("\n" + "="*70)
    print(f"💥 SIMULATING SHOCK: {shocked_sector.upper()} → {shock_value:.4f}")
    print("="*70)
    
    # Validate inputs
    if shocked_sector not in risk_dict:
        raise ValueError(f"Sector '{shocked_sector}' not found in risk data")
    
    if not (0.0 <= shock_value <= 1.0):
        raise ValueError(f"Shock value must be between 0 and 1, got {shock_value}")
    
    # Create shocked scenario
    shocked_risk = risk_dict.copy()
    original_value = shocked_risk[shocked_sector]
    shocked_risk[shocked_sector] = shock_value
    
    print(f"\n   Shock Details:")
    print(f"      Sector: {shocked_sector}")
    print(f"      Original risk: {original_value:.4f}")
    print(f"      Shocked risk: {shock_value:.4f}")
    print(f"      Change: {shock_value - original_value:+.4f}")
    
    # Run cascade on baseline
    print(f"\n📊 Running baseline cascade...")
    baseline_final, baseline_history = run_cascade(graph, risk_dict, steps, damping)
    
    # Run cascade on shocked scenario
    print(f"\n📊 Running shocked cascade...")
    shocked_final, shocked_history = run_cascade(graph, shocked_risk, steps, damping)
    
    # Compare results
    comparison = compare_scenarios(baseline_final, shocked_final, risk_dict, shocked_risk)
    
    # Add metadata
    comparison['shocked_sector'] = shocked_sector
    comparison['shock_value'] = shock_value
    comparison['original_value'] = original_value
    comparison['steps'] = steps
    comparison['damping'] = damping
    
    print("\n" + "="*70)
    print("💥 SHOCK SIMULATION COMPLETE")
    print("="*70)
    
    return comparison


def compare_scenarios(baseline_final: Dict[str, float],
                     shocked_final: Dict[str, float],
                     baseline_initial: Dict[str, float],
                     shocked_initial: Dict[str, float]) -> Dict:
    """
    Compare baseline and shocked scenarios.
    
    Args:
        baseline_final: Final risk values (baseline)
        shocked_final: Final risk values (shocked)
        baseline_initial: Initial risk values (baseline)
        shocked_initial: Initial risk values (shocked)
        
    Returns:
        Dictionary with comparison metrics
    """
    print("\n📈 SCENARIO COMPARISON:")
    print(f"\n{'Sector':<20s} {'Base Init':>10s} {'Base Final':>10s} {'Shock Init':>10s} {'Shock Final':>10s} {'Impact':>10s}")
    print("-" * 75)
    
    impact_metrics = {}
    total_impact = 0.0
    
    for sector in sorted(baseline_final.keys()):
        base_init = baseline_initial.get(sector, 0.0)
        base_final = baseline_final.get(sector, 0.0)
        shock_init = shocked_initial.get(sector, 0.0)
        shock_final = shocked_final.get(sector, 0.0)
        
        # Impact = difference in final states
        impact = shock_final - base_final
        total_impact += abs(impact)
        
        impact_metrics[sector] = {
            'baseline_initial': base_init,
            'baseline_final': base_final,
            'shocked_initial': shock_init,
            'shocked_final': shock_final,
            'impact': impact
        }
        
        print(f"{sector:<20s} {base_init:>10.4f} {base_final:>10.4f} "
              f"{shock_init:>10.4f} {shock_final:>10.4f} {impact:>+10.4f}")
    
    # Find most impacted sectors
    sorted_impacts = sorted(impact_metrics.items(), 
                           key=lambda x: abs(x[1]['impact']), 
                           reverse=True)
    
    print(f"\n🔝 Most Impacted Sectors:")
    for i, (sector, metrics) in enumerate(sorted_impacts[:5], 1):
        print(f"   {i:2d}. {sector:20s}: {metrics['impact']:+.4f}")
    
    return {
        'impact_metrics': impact_metrics,
        'total_system_impact': total_impact,
        'baseline_final': baseline_final,
        'shocked_final': shocked_final
    }


def analyze_sector_vulnerability(graph: nx.DiGraph,
                                risk_dict: Dict[str, float],
                                shock_value: float = 0.9,
                                steps: int = 5) -> Dict[str, float]:
    """
    Analyze how vulnerable the system is to shocks in each sector.
    
    Shocks each sector individually and measures total system impact.
    
    Args:
        graph: NetworkX DiGraph
        risk_dict: Baseline risk values
        shock_value: Shock value to apply (default: 0.9 = high risk)
        steps: Number of cascade steps
        
    Returns:
        Dictionary: {sector: total_system_impact}
    """
    print("\n" + "="*70)
    print("🎯 SECTOR VULNERABILITY ANALYSIS")
    print("="*70)
    print(f"   Shock value: {shock_value}")
    print(f"   Cascade steps: {steps}")
    
    vulnerability_scores = {}
    
    for sector in graph.nodes():
        if sector in risk_dict:
            result = simulate_shock(graph, risk_dict, sector, shock_value, steps)
            vulnerability_scores[sector] = result['total_system_impact']
    
    # Print summary
    print(f"\n📊 Vulnerability Scores (higher = more systemic risk):")
    sorted_vuln = sorted(vulnerability_scores.items(), key=lambda x: x[1], reverse=True)
    
    for i, (sector, score) in enumerate(sorted_vuln, 1):
        print(f"   {i:2d}. {sector:20s}: {score:.4f}")
    
    print("="*70)
    
    return vulnerability_scores


def simulate_multiple_shocks(graph: nx.DiGraph,
                            risk_dict: Dict[str, float],
                            shocks: List[Tuple[str, float]],
                            steps: int = 5) -> Dict:
    """
    Simulate multiple simultaneous shocks.
    
    Args:
        graph: NetworkX DiGraph
        risk_dict: Baseline risk values
        shocks: List of (sector, shock_value) tuples
        steps: Number of cascade steps
        
    Returns:
        Dictionary with multi-shock analysis results
    """
    print("\n" + "="*70)
    print("💥 SIMULATING MULTIPLE SIMULTANEOUS SHOCKS")
    print("="*70)
    
    # Create shocked scenario
    shocked_risk = risk_dict.copy()
    
    print(f"\n   Shocks applied:")
    for sector, value in shocks:
        if sector in shocked_risk:
            original = shocked_risk[sector]
            shocked_risk[sector] = value
            print(f"      {sector:20s}: {original:.4f} → {value:.4f}")
        else:
            print(f"      ⚠️  Sector '{sector}' not found, skipping")
    
    # Run cascades
    print(f"\n📊 Running baseline cascade...")
    baseline_final, baseline_history = run_cascade(graph, risk_dict, steps)
    
    print(f"\n📊 Running multi-shock cascade...")
    shocked_final, shocked_history = run_cascade(graph, shocked_risk, steps)
    
    # Compare
    comparison = compare_scenarios(baseline_final, shocked_final, risk_dict, shocked_risk)
    comparison['shocks'] = shocks
    
    print("\n" + "="*70)
    print("💥 MULTI-SHOCK SIMULATION COMPLETE")
    print("="*70)
    
    return comparison


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from graph.risk_loader import load_latest_risk, load_risk_timeseries
    from graph.weight_learner import learn_weights
    from graph.graph_builder import build_graph
    
    # Load data and build graph
    risk_dict = load_latest_risk()
    df = load_risk_timeseries()
    weights = learn_weights(df, method='regression')
    G = build_graph(weights)
    
    # Simulate shock to each sector
    print("\n" + "="*70)
    print("🎯 TESTING INDIVIDUAL SECTOR SHOCKS")
    print("="*70)
    
    for sector in sorted(risk_dict.keys()):
        result = simulate_shock(G, risk_dict, sector, shock_value=0.9, steps=5)
        print(f"\n   Total system impact from {sector} shock: {result['total_system_impact']:.4f}")
    
    # Analyze vulnerability
    vulnerability = analyze_sector_vulnerability(G, risk_dict, shock_value=0.9, steps=5)
