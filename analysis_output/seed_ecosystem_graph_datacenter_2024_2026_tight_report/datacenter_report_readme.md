# Datacenter Focused Report Outputs

This folder contains V6c focused reports generated from V6b graph outputs.

## Files
- top_seed_neighbors_by_seed.csv: top direct non-seed neighbors for each seed ticker.
- multi_seed_connected_candidates.csv: non-seed first-hop candidates connected to multiple seeds.
- datacenter_cross_sector_bridges_top.csv: top cross-sector bridge edges involving seed/first-hop endpoints.
- datacenter_priority_candidates.csv: ranked first-hop/second-hop non-seed candidates using deterministic priority_score.
- datacenter_subtheme_groups.csv: grouped summary by deterministic subtheme_guess.
- datacenter_seed_summary_compact.csv: compact summary of each seed and top direct neighbors.
- datacenter_cliques_top.csv: strongest cliques containing at least one seed or first-hop ticker.
- datacenter_validation_shortlist.csv: concise manual validation list with reason tags.
- datacenter_core_validation_shortlist.csv: clean operating-company shortlist for manual validation.
- datacenter_broad_beta_candidates.csv: broad beta/statistical candidates separated from the core list.
- datacenter_focused_report.md: compact markdown summary (top 10 sections).
- datacenter_report_readme.md: this file.
