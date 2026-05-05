# Ecosystem Theme: datacenter

Input database: `usa_close_change.db`
Raw report directory: `analysis_output/similar_stocks_report`
Residual report directory: `analysis_output/residual_similar_stocks_2024_2026`
Output directory: `analysis_output/ecosystem_graph_2024_2026`

Theme name: `datacenter`
Seed tickers (30): AMAT, AMD, ANET, APH, ASML, AVGO, CEG, CIEN, DELL, DLR, EMR, EQIX, ETN, GEV, GLW, HPE, KLAC, LRCX, MPWR, MRVL, NEE, NTAP, NVDA, NVT, PWR, SMCI, TSM, TT, VRT, VST

## Thresholds

- Min raw correlation: `0.5`
- Min raw rolling mean: `0.4`
- Min residual correlation: `0.3`
- Min residual rolling mean: `0.2`
- Min combined score: `0.3`
- Min component size: `3`
- Min / max clique size: `3` / `6`
- Max seed distance: `2`

## Summary Statistics

- Graph nodes: `230`
- Graph edges: `429`
- Connected components (total): `56`
- Seed components: `2`
- Cliques found (size 3+): `67`
- Cliques written: `67`
- Direct seed edges: `12`
- Seed expansion candidates: `6`
- Cross-sector edges: `6`
- Seed tickers found in graph: `6`

## Graph Construction

Nodes are individual stock tickers. Edges are statistical similarity relationships
derived from daily close-change percentage residual correlations (V4) and raw
Pearson correlations (V3). An edge exists when the pair passes filtering thresholds
on raw correlation, rolling correlation mean, residual correlation, and/or seed adjacency.

## Combined Score

Each edge has a `combined_score` computed as a weighted average of up to four
metrics: raw_correlation (0.25), raw_rolling_corr_mean (0.20), residual_correlation (0.35),
residual_rolling_corr_mean (0.20). Missing metrics are removed and remaining weights
are rescaled to sum to 1.

## Connected Components

Connected components group all tickers reachable from each other via the similarity
edge graph. A component may span multiple sectors if cross-sector edges exist.
Component size is the number of tickers. Only components with size >= min_component_size
are reported in ecosystem_components.csv.

## Cliques

A clique is a group of tickers where every pair has a direct similarity edge.
Cliques of size >= min_clique_size are reported. Large cliques indicate groups
with strong mutual statistical co-movement.

## Seed Expansion

Seed tickers define the theme (e.g. datacenter ecosystem). The seed expansion
report lists non-seed tickers within max_seed_distance graph hops from any seed.
Distance 1 = direct edge to a seed. Distance 2 = connected via one intermediate node.

## Datacenter Ecosystem Context

The datacenter theme includes chips, semiconductor equipment, networking,
optical/connectivity, electrical equipment, power infrastructure, utilities,
datacenter REITs, cooling, and construction/engineering. These sectors are
economically linked by the build-out of AI and cloud infrastructure.

## Important Caveats

Graph connections are statistical similarity measures, not proof of business
relationship or causality. Two stocks may have high correlation for reasons
unrelated to their business connection: shared macro sensitivity, sector rotation,
or statistical coincidence.

Datacenter ecosystem interpretation requires business validation. Not all stocks
in the graph are directly linked to the datacenter theme. The graph reflects
co-movement patterns over the analyzed period, which may not persist.
