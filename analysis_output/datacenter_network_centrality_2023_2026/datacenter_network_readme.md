# Datacenter Network Centrality Outputs

This directory contains V7 centrality/connectivity reports generated from V6b + V6c outputs.

## Files
- datacenter_network_node_scores.csv: full node-level network metrics for all graph nodes.
- datacenter_top_network_connectors.csv: top nodes by network importance score.
- datacenter_top_non_seed_connectors.csv: top non-seed nodes by network importance score.
- datacenter_multi_seed_connectors.csv: nodes connected to multiple seed tickers.
- datacenter_cross_subtheme_bridges.csv: nodes bridging multiple neighbor subthemes.
- datacenter_cross_sector_network_bridges.csv: nodes with strong cross-sector connectivity.
- datacenter_clique_connectors.csv: nodes with meaningful clique participation.
- datacenter_subtheme_network_summary.csv: subtheme-level centrality summary.
- datacenter_edge_filtered_strong_network.csv: filtered strong edge list for manual review.
- datacenter_network_validation_shortlist.csv: centrality-aware manual validation shortlist.
- datacenter_top_seed_anchors.csv: ranked seed anchor nodes with deterministic reason tags.
- datacenter_top_operating_non_seed_connectors.csv: ranked operating non-seed datacenter connectors.
- datacenter_broad_beta_network_connectors.csv: ranked broad beta/financial/statistical connectors.
- datacenter_operating_connector_by_subtheme.csv: operating connector summary grouped by subtheme.
- datacenter_final_manual_review_queue.csv: combined manual review queue across seed/core/high-signal broad-beta.
- datacenter_network_report.md: compact markdown summary with top sections.
- datacenter_network_readme.md: this file.
