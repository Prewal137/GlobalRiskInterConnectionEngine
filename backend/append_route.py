import textwrap

code = textwrap.dedent("""
@router.post("/monte-carlo")
async def run_monte_carlo_endpoint(payload: dict):
    \"\"\"
    Run continuous-time Monte Carlo risk simulation using Student-t Copula dependency.
    Calculates Value-at-Risk (VaR) and Expected Shortfall (CVaR).
    \"\"\"
    try:
        from app.graph.risk_loader import load_latest_risk, load_risk_timeseries
        from app.graph.weight_learner import learn_weights
        from app.graph.graph_builder import build_graph
        from app.graph.monte_carlo_engine import run_monte_carlo_cascade
        from app.graph.statistical_tests import run_tier2_tests
        import numpy as np
        
        # Load baseline
        risk = load_latest_risk()
        df = load_risk_timeseries()
        
        # Build graph and correlation matrix
        weights = learn_weights(df, method='regression')
        graph = build_graph(weights)
        
        # Compute correlation matrix for Copula
        sector_cols = [col for col in df.columns if col not in ['Year', 'Month']]
        corr_matrix = df[sector_cols].corr()
        
        # Apply what-if shocks from payload
        for key, value in payload.items():
            if key in risk:
                risk[key] = float(value)
            else:
                for node in graph.nodes():
                    if node.lower() == key.lower():
                        risk[node] = float(value)
                        break
        
        # Run Monte Carlo
        mc_results = run_monte_carlo_cascade(
            graph=graph, 
            risk_dict=risk, 
            corr_matrix=corr_matrix, 
            n_sim=1000, 
            steps=5, 
            dt=0.1
        )
        
        # Run statistical tests
        stats = run_tier2_tests(mc_results['total_loss_distribution'])
        
        hist, bins = np.histogram(mc_results['total_loss_distribution'], bins=50)
        
        return {
            "mode": "monte-carlo",
            "input": payload,
            "var_95": mc_results['var_95'],
            "cvar_95": mc_results['cvar_95'],
            "final_mean_state": {k: round(float(v), 4) for k, v in mc_results['final_mean_state'].items()},
            "statistical_tests": stats,
            "histogram": hist.tolist(),
            "bin_edges": bins.tolist(),
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Error running Monte Carlo simulation: {str(e)}")
""")

with open('app/routes/interconnection.py', 'a', encoding='utf-8') as f:
    f.write(code)
