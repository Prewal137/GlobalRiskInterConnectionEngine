"""
⚡ Dynamic Interconnection Engine

Main orchestrator for the learning-based graph interconnection system.
Integrates all components:
1. Load risk time series
2. Learn edge weights from data (NO fixed weights!)
3. Build interconnection graph
4. Run cascade simulation
5. Save results

Author: Global Risk Platform
Date: 2026-04-05
"""

import pandas as pd
import numpy as np
import networkx as nx
import os
from typing import Dict, Optional, Tuple
from datetime import datetime

from .risk_loader import load_risk_timeseries, load_latest_risk
from .weight_learner import learn_weights
from .graph_builder import build_graph
from .cascade_engine import run_cascade, print_cascade_summary
from .shock_simulator import simulate_shock, analyze_sector_vulnerability


# Output path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "interconnection", "dynamic_risk_learned.csv")


def run_dynamic_system(method: str = 'regression',
                       cascade_steps: int = 5,
                       damping: float = 0.8,
                       save_results: bool = True,
                       run_shock_analysis: bool = True) -> Dict:
    """
    Main function to run the complete dynamic interconnection system.
    
    Args:
        method: Weight learning method ('correlation' or 'regression')
        cascade_steps: Number of cascade simulation steps
        damping: Damping factor for cascade
        save_results: Whether to save results to CSV
        run_shock_analysis: Whether to run shock vulnerability analysis
        
    Returns:
        Dictionary with complete system results
    """
    print("\n" + "="*70)
    print("⚡ DYNAMIC INTERCONNECTION ENGINE")
    print("="*70)
    print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Weight learning method: {method}")
    print(f"   Cascade steps: {cascade_steps}")
    print(f"   Damping factor: {damping}")
    print("="*70)
    
    results = {}
    
    # ========================================================================
    # STEP 1: Load Risk Time Series
    # ========================================================================
    print("\n" + "="*70)
    print("📊 STEP 1: LOADING RISK TIME SERIES")
    print("="*70)
    
    df = load_risk_timeseries()
    results['time_series'] = df
    
    # ========================================================================
    # STEP 2: Learn Edge Weights
    # ========================================================================
    print("\n" + "="*70)
    print("🔥 STEP 2: LEARNING EDGE WEIGHTS FROM DATA")
    print("="*70)
    
    weights = learn_weights(df, method=method)
    results['weights'] = weights
    
    # ========================================================================
    # STEP 3: Build Interconnection Graph
    # ========================================================================
    print("\n" + "="*70)
    print("🕸️  STEP 3: BUILDING INTERCONNECTION GRAPH")
    print("="*70)
    
    G = build_graph(weights)
    results['graph'] = G
    
    # ========================================================================
    # STEP 4: Load Latest Risk Values
    # ========================================================================
    print("\n" + "="*70)
    print("📍 STEP 4: LOADING LATEST RISK VALUES")
    print("="*70)
    
    risk_dict = load_latest_risk()
    results['initial_risk'] = risk_dict
    
    # ========================================================================
    # STEP 5: Run Cascade Simulation
    # ========================================================================
    print("\n" + "="*70)
    print("🌊 STEP 5: RUNNING CASCADE SIMULATION")
    print("="*70)
    
    final_risk, cascade_history = run_cascade(G, risk_dict, steps=cascade_steps, damping=damping)
    results['final_risk'] = final_risk
    results['cascade_history'] = cascade_history
    
    # Print cascade summary
    print_cascade_summary(risk_dict, final_risk, cascade_history)
    
    # ========================================================================
    # STEP 6: Save Results
    # ========================================================================
    if save_results:
        print("\n" + "="*70)
        print("💾 STEP 6: SAVING RESULTS")
        print("="*70)
        
        # Create output DataFrame
        output_data = []
        
        for step_idx, step_risk in enumerate(cascade_history):
            row = {
                'cascade_step': step_idx,
                'timestamp': datetime.now().isoformat()
            }
            row.update(step_risk)
            output_data.append(row)
        
        output_df = pd.DataFrame(output_data)
        
        # Also add initial vs final comparison
        comparison_row = {
            'cascade_step': 'summary',
            'timestamp': datetime.now().isoformat()
        }
        
        for sector in risk_dict:
            initial = risk_dict.get(sector, 0.0)
            final = final_risk.get(sector, 0.0)
            comparison_row[f'{sector}_initial'] = initial
            comparison_row[f'{sector}_final'] = final
            comparison_row[f'{sector}_change'] = final - initial
        
        # Save to CSV
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        output_df.to_csv(OUTPUT_FILE, index=False)
        
        print(f"   ✅ Saved cascade history to: {OUTPUT_FILE}")
        print(f"      Rows: {len(output_df)}")
        print(f"      Columns: {len(output_df.columns)}")
        
        results['output_file'] = OUTPUT_FILE
        results['output_dataframe'] = output_df
    
    # ========================================================================
    # STEP 7: Shock Vulnerability Analysis (Optional)
    # ========================================================================
    if run_shock_analysis:
        print("\n" + "="*70)
        print("💥 STEP 7: SHOCK VULNERABILITY ANALYSIS")
        print("="*70)
        
        vulnerability_scores = analyze_sector_vulnerability(
            G, risk_dict, shock_value=0.9, steps=cascade_steps
        )
        results['vulnerability_scores'] = vulnerability_scores
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print("\n" + "="*70)
    print("✅ DYNAMIC INTERCONNECTION ENGINE COMPLETE")
    print("="*70)
    
    print(f"\n📊 LEARNED WEIGHTS:")
    print(f"   Total edges: {len(weights)}")
    if weights:
        sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
        print(f"   Top 5 strongest edges:")
        for i, ((source, target), weight) in enumerate(sorted_weights[:5], 1):
            print(f"      {i}. {source} → {target}: {weight:.4f}")
    
    print(f"\n📈 RISK TRANSFORMATION:")
    print(f"   {'Sector':<20s} {'Initial':>10s} {'Final':>10s} {'Change':>10s}")
    print("   " + "-" * 55)
    
    for sector in sorted(risk_dict.keys()):
        initial = risk_dict.get(sector, 0.0)
        final = final_risk.get(sector, 0.0)
        change = final - initial
        print(f"   {sector:<20s} {initial:>10.4f} {final:>10.4f} {change:>+10.4f}")
    
    if run_shock_analysis and 'vulnerability_scores' in results:
        print(f"\n💥 SECTOR VULNERABILITY (systemic risk from shock):")
        sorted_vuln = sorted(vulnerability_scores.items(), key=lambda x: x[1], reverse=True)
        for i, (sector, score) in enumerate(sorted_vuln, 1):
            print(f"   {i}. {sector:<20s}: {score:.4f}")
    
    print("="*70)
    
    return results


def run_shock_scenario(graph: nx.DiGraph,
                       risk_dict: Dict[str, float],
                       shocked_sector: str,
                       shock_value: float = 0.9,
                       cascade_steps: int = 5) -> Dict:
    """
    Convenience function to run a specific shock scenario.
    
    Args:
        graph: NetworkX DiGraph
        risk_dict: Baseline risk values
        shocked_sector: Sector to shock
        shock_value: Shock value (0-1)
        cascade_steps: Number of cascade steps
        
    Returns:
        Shock simulation results
    """
    return simulate_shock(graph, risk_dict, shocked_sector, shock_value, cascade_steps)


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    # Run the complete dynamic system
    results = run_dynamic_system(
        method='regression',
        cascade_steps=5,
        damping=0.8,
        save_results=True,
        run_shock_analysis=True
    )
    
    print("\n🎯 SYSTEM READY FOR API INTEGRATION")
    print(f"   Output file: {results.get('output_file', 'N/A')}")
    print(f"   Graph nodes: {results['graph'].number_of_nodes()}")
    print(f"   Graph edges: {results['graph'].number_of_edges()}")
