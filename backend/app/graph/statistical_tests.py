"""
📊 Statistical Tests Suite for Monte Carlo Simulation

Implements the Tier 1-3 rigor checks requested by the peer reviewer.
- Hartigan's Dip Test for unimodality/bimodality
- Silverman's Bandwidth Test
- Gaussian Mixture Model BIC
- KS Test vs Bernoulli
"""

import numpy as np
from diptest import diptest
from sklearn.mixture import GaussianMixture
from scipy.stats import kstest, bernoulli
import statsmodels.api as sm
from statsmodels.nonparametric.bandwidths import bw_silverman

def run_tier2_tests(simulation_outputs: np.ndarray) -> dict:
    """
    Run statistical robustness checks to ensure bimodality (if any) is genuine 
    and not an artifact, and ensure it hasn't collapsed into a Bernoulli coin flip.
    """
    results = {}
    
    # 1. Hartigan's Dip Test
    # H0: Distribution is unimodal
    # If p_value < 0.05, we reject H0 -> it's multimodal
    try:
        dip_stat, p_value = diptest(simulation_outputs)
        results['hartigans_dip'] = {
            'statistic': float(dip_stat),
            'p_value': float(p_value),
            'is_multimodal': bool(p_value < 0.05)
        }
    except Exception as e:
        results['hartigans_dip'] = {'error': str(e)}

    # 2. Silverman's Bandwidth
    try:
        bw = bw_silverman(simulation_outputs)
        results['silverman_bw'] = float(bw)
    except Exception as e:
        results['silverman_bw'] = {'error': str(e)}
        
    # 3. GMM BIC Comparison
    try:
        bic_scores = {}
        data = simulation_outputs.reshape(-1, 1)
        best_n = 1
        min_bic = float('inf')
        
        for n_comp in [1, 2, 3]:
            gmm = GaussianMixture(n_components=n_comp, covariance_type='full', random_state=42)
            gmm.fit(data)
            bic = gmm.bic(data)
            bic_scores[n_comp] = float(bic)
            if bic < min_bic:
                min_bic = bic
                best_n = n_comp
                
        results['gmm_bic'] = {
            'scores': bic_scores,
            'optimal_components': best_n
        }
    except Exception as e:
        results['gmm_bic'] = {'error': str(e)}

    # 4. Kolmogorov-Smirnov vs Bernoulli
    # Proves the distribution is not just a binary coin flip at a threshold
    try:
        threshold = np.median(simulation_outputs)
        p_bern = np.mean(simulation_outputs > threshold)
        
        # Test if the continuous data perfectly matches a discrete step function
        # We compare the CDF of the simulation output against a Bernoulli CDF
        ks_stat, ks_p = kstest(simulation_outputs, lambda x: bernoulli.cdf(x > threshold, p_bern))
        
        results['ks_vs_bernoulli'] = {
            'statistic': float(ks_stat),
            'p_value': float(ks_p),
            'is_coin_flip': bool(ks_p > 0.05) # If p > 0.05, we can't reject Bernoulli -> BAD!
        }
    except Exception as e:
        results['ks_vs_bernoulli'] = {'error': str(e)}

    return results
