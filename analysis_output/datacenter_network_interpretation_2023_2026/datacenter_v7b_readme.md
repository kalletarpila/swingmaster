# Datacenter Network Interpretation Outputs (V7b)

V7b reads V7 (network centrality) outputs and produces interpretable prioritization reports.

## Output Files

### datacenter_operating_relevance_scores.csv
Full node table with V7b metrics: operating relevance score, seed subtheme connectivity, ecosystem bridge scores.
One row per ticker.

### datacenter_top_operating_relevance.csv
Top nodes by operating relevance score, filtered by threshold.
Includes both seeds and non-seeds.

### datacenter_top_non_seed_operating_relevance.csv
Top non-seed nodes by operating relevance score.
Best candidates for new operating datacenter exposure.

### datacenter_top_connectors_by_subtheme.csv
Top 25 connectors within each subtheme.
Helps identify best-in-class companies per operational focus area.

### datacenter_seed_subtheme_connectivity.csv
Companies connected to multiple seed subthemes.
Bridges across different parts of the operating datacenter ecosystem.

### datacenter_best_ecosystem_bridges.csv
Companies with high ecosystem bridge scores.
Key nodes for understanding how different ecosystem areas connect.

### datacenter_interpretation_flags.csv
Summary of companies by interpretation flag (OPERATING_CORE, ECOSYSTEM_BRIDGE, BROAD_BETA_RISK, etc.).
Helps identify and quantify different roles in the ecosystem.

### datacenter_v7b_final_research_queue.csv
Clean research queue combining seeds, operating core, ecosystem bridges, and high-relevance candidates.
Primary output for manual business validation workflows.

### datacenter_v7b_summary.md
Executive summary with key metrics, top lists, and scoring explanations.

### datacenter_v7b_readme.md
This file.
