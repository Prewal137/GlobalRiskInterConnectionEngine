"""
🕸️ Graph Builder for Graph Interconnection System

Builds a NetworkX directed graph where:
- Nodes = sectors
- Edges = learned relationship weights

Author: Global Risk Platform
Date: 2026-04-05
"""

import networkx as nx
import matplotlib.pyplot as plt
from typing import Dict, Tuple, Optional, List
import os


def build_graph(weights: Dict[Tuple[str, str], float], 
                threshold: float = 0.0) -> nx.DiGraph:
    """
    Build a directed graph from learned weights.
    
    Args:
        weights: Dictionary {(source, target): weight}
        threshold: Minimum weight threshold for edges (default: 0.0)
        
    Returns:
        NetworkX DiGraph with sectors as nodes and weights as edges
    """
    print("\n" + "="*70)
    print("🕸️  BUILDING INTERCONNECTION GRAPH")
    print("="*70)
    
    # Create directed graph
    G = nx.DiGraph()
    
    # Add nodes (sectors)
    sectors = set()
    for (source, target) in weights.keys():
        sectors.add(source)
        sectors.add(target)
    
    for sector in sectors:
        G.add_node(sector)
    
    print(f"   Added {G.number_of_nodes()} nodes: {sorted(G.nodes())}")
    
    # Add edges with weights (filtered by threshold)
    edge_count = 0
    for (source, target), weight in weights.items():
        if weight >= threshold:
            G.add_edge(source, target, weight=weight)
            edge_count += 1
    
    print(f"   Added {edge_count} edges (threshold: {threshold})")
    
    # Graph statistics
    print(f"\n📊 Graph Statistics:")
    print(f"   Nodes: {G.number_of_nodes()}")
    print(f"   Edges: {G.number_of_edges()}")
    print(f"   Density: {nx.density(G):.4f}")
    
    if G.number_of_edges() > 0:
        print(f"   Average weight: {nx.get_edge_attributes(G, 'weight').values().__iter__().__next__():.4f}")
        
        # In-degree and out-degree
        in_degrees = dict(G.in_degree())
        out_degrees = dict(G.out_degree())
        
        print(f"\n   📥 In-Degree (sectors most influenced):")
        for sector in sorted(in_degrees, key=in_degrees.get, reverse=True):
            print(f"      {sector:20s}: {in_degrees[sector]}")
        
        print(f"\n   📤 Out-Degree (sectors with most influence):")
        for sector in sorted(out_degrees, key=out_degrees.get, reverse=True):
            print(f"      {sector:20s}: {out_degrees[sector]}")
    
    print("="*70)
    
    return G


def visualize_graph(G: nx.DiGraph, save_path: Optional[str] = None):
    """
    Visualize the interconnection graph.
    
    Args:
        G: NetworkX DiGraph
        save_path: Path to save visualization (optional)
    """
    print("\n📊 Generating graph visualization...")
    
    # Create figure
    plt.figure(figsize=(12, 10))
    
    # Get edge weights for visualization
    edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
    
    # Normalize edge weights for width
    if edge_weights:
        min_w = min(edge_weights)
        max_w = max(edge_weights)
        if max_w > min_w:
            widths = [2 + 8 * (w - min_w) / (max_w - min_w) for w in edge_weights]
        else:
            widths = [2] * len(edge_weights)
    else:
        widths = [2]
    
    # Use spring layout
    pos = nx.spring_layout(G, seed=42, k=2)
    
    # Draw nodes
    nx.draw_networkx_nodes(G, pos, 
                          node_color='lightblue', 
                          node_size=2000, 
                          edgecolors='navy',
                          alpha=0.8)
    
    # Draw edges with varying widths
    nx.draw_networkx_edges(G, pos, 
                          width=widths,
                          edge_color='gray',
                          arrows=True,
                          arrowsize=20,
                          alpha=0.6)
    
    # Draw edge labels (weights)
    edge_labels = {(u, v): f"{d['weight']:.2f}" for u, v, d in G.edges(data=True)}
    nx.draw_networkx_edge_labels(G, pos, 
                                edge_labels=edge_labels,
                                font_size=8,
                                alpha=0.7)
    
    # Draw node labels
    nx.draw_networkx_labels(G, pos, 
                           font_size=11, 
                           font_weight='bold')
    
    plt.title('Sector Risk Interconnection Graph\n(Learned Weights)', 
             fontsize=14, fontweight='bold', pad=20)
    plt.axis('off')
    plt.tight_layout()
    
    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"   ✅ Saved visualization to: {save_path}")
    else:
        plt.show()
    
    plt.close()


def get_strongest_connections(G: nx.DiGraph, top_n: int = 10) -> List[Tuple[str, str, float]]:
    """
    Get the strongest connections in the graph.
    
    Args:
        G: NetworkX DiGraph
        top_n: Number of top connections to return
        
    Returns:
        List of (source, target, weight) tuples
    """
    edge_weights = nx.get_edge_attributes(G, 'weight')
    sorted_edges = sorted(edge_weights.items(), key=lambda x: x[1], reverse=True)
    
    result = []
    for (source, target), weight in sorted_edges[:top_n]:
        result.append((source, target, weight))
    
    return result


def get_sector_influence(G: nx.DiGraph) -> Dict[str, Dict[str, float]]:
    """
    Calculate influence metrics for each sector.
    
    Returns:
        Dictionary with influence metrics per sector
    """
    influence_metrics = {}
    
    for sector in G.nodes():
        # Out-strength: how much this sector influences others
        out_strength = sum(G[sector][neighbor]['weight'] 
                          for neighbor in G.successors(sector))
        
        # In-strength: how much this sector is influenced by others
        in_strength = sum(G[predecessor][sector]['weight'] 
                         for predecessor in G.predecessors(sector))
        
        # Net influence
        net_influence = out_strength - in_strength
        
        influence_metrics[sector] = {
            'out_strength': out_strength,
            'in_strength': in_strength,
            'net_influence': net_influence
        }
    
    return influence_metrics


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from graph.risk_loader import load_risk_timeseries
    from graph.weight_learner import learn_weights
    
    # Load data and learn weights
    df = load_risk_timeseries()
    weights = learn_weights(df, method='regression')
    
    # Build graph
    G = build_graph(weights)
    
    # Get strongest connections
    print("\n🔝 Top 10 Strongest Connections:")
    for i, (source, target, weight) in enumerate(get_strongest_connections(G, 10), 1):
        print(f"   {i:2d}. {source:20s} → {target:20s} = {weight:.4f}")
    
    # Get influence metrics
    print("\n📊 Sector Influence Metrics:")
    metrics = get_sector_influence(G)
    for sector, metric in sorted(metrics.items(), key=lambda x: x[1]['net_influence'], reverse=True):
        print(f"   {sector:20s}: out={metric['out_strength']:.4f}, in={metric['in_strength']:.4f}, net={metric['net_influence']:+.4f}")
