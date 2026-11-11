"""
🎲 Monte Carlo Engine for Graph Interconnection System

Implements a continuous-time (Euler-Maruyama) Monte Carlo simulation
with Student-t Copula dependency to avoid statistical artifacts.

Features:
- PCG64 Independent random generators
- Student-t Copula for heavy-tail dependency (non-zero lambda_U)
- Continuous ODE-like state propagation without boolean leaks
- Rigorous VaR and CVaR calculations
"""

import networkx as nx
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Any
from scipy.stats import t as student_t, norm
from scipy.linalg import cholesky

def t_copula_sample(corr_matrix: np.ndarray, nu: float, n_sim: int, rng: np.random.Generator) -> np.ndarray:
    """
    Generate correlated uniform samples using a Student-t Copula.
    This preserves tail dependencies between extreme events.
    
    Args:
        corr_matrix: Correlation matrix between assets/sectors
        nu: Degrees of freedom for the t-distribution
        n_sim: Number of simulations
        rng: Random number generator (PCG64)
        
    Returns:
        np.ndarray of shape (n_sim, n_assets) with values in (0, 1)
    """
    n_assets = corr_matrix.shape[0]
    
    # Ensure positive semi-definite
    eigenvalues = np.linalg.eigvalsh(corr_matrix)
    if np.any(eigenvalues < -1e-10):
        # Regularize to PSD
        min_eig = np.min(eigenvalues)
        corr_matrix = corr_matrix - min_eig * np.eye(n_assets)
        # Re-normalize to correlation matrix
        d = np.sqrt(np.diag(corr_matrix))
        corr_matrix = corr_matrix / np.outer(d, d)
        
    L = cholesky(corr_matrix, lower=True)
    
    # Z ~ N(0, I)
    Z = rng.standard_normal((n_sim, n_assets))
    
    # chi2 ~ Chi-Square(nu)
    chi2 = rng.chisquare(nu, size=n_sim)
    
    # W ~ N(0, Sigma)
    W = Z @ L.T
    
    # T = W / sqrt(chi2 / nu)
    T = W / np.sqrt(chi2[:, None] / nu)
    
    # Transform to uniform using t-CDF
    U = student_t.cdf(T, df=nu)
    
    return U

def compute_var_cvar(losses: np.ndarray, alpha: float = 0.95) -> Tuple[float, float]:
    """
    Compute Value at Risk (VaR) and Expected Shortfall (CVaR).
    
    Args:
        losses: Array of simulated final losses
        alpha: Confidence level
        
    Returns:
        Tuple of (VaR, CVaR)
    """
    sorted_losses = np.sort(losses)
    idx = int(alpha * len(sorted_losses))
    var = sorted_losses[idx]
    
    cvar = np.mean(sorted_losses[idx:]) if idx < len(sorted_losses) else var
    return var, cvar

def run_monte_carlo_cascade(graph: nx.DiGraph, 
                            risk_dict: Dict[str, float], 
                            corr_matrix: pd.DataFrame,
                            n_sim: int = 10000,
                            steps: int = 5,
                            dt: float = 0.1,
                            nu: float = 5.0,
                            seed: int = 42) -> Dict[str, Any]:
    """
    Run Monte Carlo simulation of risk cascade using Euler-Maruyama integration.
    
    Args:
        graph: NetworkX DiGraph with learned weights
        risk_dict: Initial risk state
        corr_matrix: Correlation matrix of sectors
        n_sim: Number of paths to simulate
        steps: Number of integration steps
        dt: Time step size
        nu: Degrees of freedom for t-Copula
        seed: Random seed
        
    Returns:
        Dictionary with simulation results
    """
    sectors = list(corr_matrix.columns)
    
    # Initialize PCG64 Random Number Generator
    base_rng = np.random.default_rng(seed)
    
    # Extract adjacency matrix for fast propagation
    adj_matrix = nx.to_numpy_array(graph, nodelist=sectors, weight='weight')
    
    # Initial state (shape: n_sim x n_sectors)
    initial_risks = np.array([risk_dict.get(s, 0.0) for s in sectors])
    
    # Expand to n_sim
    state = np.tile(initial_risks, (n_sim, 1))
    
    history_mean = []
    
    # Damping factor: decay rate to prevent infinite growth
    damping_factor = 0.2
    
    for step in range(steps):
        # Continuous cascade formulation:
        # dx = (A * x) dt + sigma * dW
        
        # Drift component (deterministic cascade)
        drift = state @ adj_matrix
        decay = -damping_factor * state
        
        # Generate t-copula sample for this step to maintain heavy-tail joint shocks
        U_step = t_copula_sample(corr_matrix.values, nu, n_sim, base_rng)
        
        # Transform uniform samples to standard normal shocks scaled by dt (Brownian increment)
        dW_step = norm.ppf(U_step) * 0.05 * np.sqrt(dt)
        
        # Update state using Euler-Maruyama
        state = state + (drift + decay) * dt + dW_step
        
        # Reviewer warning: "Clipping then normalizing loses tail structure"
        # We do not clip the raw states to preserve the true uncompressed distribution tails
        
        history_mean.append(np.mean(state, axis=0))
        
    # Final total system loss distribution (sum across sectors)
    total_loss_distribution = np.sum(state, axis=1)
    
    # Compute metrics
    var_95, cvar_95 = compute_var_cvar(total_loss_distribution, 0.95)
    
    return {
        'total_loss_distribution': total_loss_distribution,
        'var_95': float(var_95),
        'cvar_95': float(cvar_95),
        'final_mean_state': dict(zip(sectors, np.mean(state, axis=0))),
        'history_mean': history_mean,
        'sectors': sectors
    }
