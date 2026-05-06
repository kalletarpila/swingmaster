# Datacenter Focused Ecosystem Report

## Run Context
- input_dir: analysis_output/seed_ecosystem_graph_datacenter_2023_2026_tight
- output_dir: analysis_output/seed_ecosystem_graph_datacenter_2023_2026_tight_report
- theme_name: datacenter
- thresholds used:
  - min_best_seed_edge_score: 0.35
  - min_seed_neighbor_count: 2
  - min_weighted_degree: 1.0
  - top_neighbors_per_seed: 30
  - top_priority_candidates: 300
  - top_cross_sector_bridges: 300
  - top_multi_seed_candidates: 300

## Counts
- seeds_found_in_graph: 109
- priority_candidates: 300
- multi_seed_candidates: 300
- cross_sector_bridges: 300
- subthemes: 10
- core_validation_candidates: 52
- broad_beta_candidates: 248

## Top 10 Priority Candidates
| ticker | subtheme_guess | priority_score | priority_tier | seed_status | seed_neighbor_count |
| --- | --- | --- | --- | --- | --- |
| RVT | OTHER_OR_UNCLASSIFIED | 0.8918423586335279 | HIGH | FIRST_HOP | 95 |
| LFUS | NETWORKING_OPTICAL_CONNECTIVITY | 0.8914156171911403 | HIGH | FIRST_HOP | 94 |
| TKR | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.889179254968784 | HIGH | FIRST_HOP | 73 |
| KLIC | SEMICONDUCTOR_EQUIPMENT | 0.888354828133788 | HIGH | FIRST_HOP | 90 |
| RMT | OTHER_OR_UNCLASSIFIED | 0.8869376003498314 | HIGH | FIRST_HOP | 99 |
| DIOD | SEMICONDUCTORS_AI_CHIPS | 0.8867542632478249 | HIGH | FIRST_HOP | 77 |
| LECO | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.8836972034265113 | HIGH | FIRST_HOP | 56 |
| AIT | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.8833838357879209 | HIGH | FIRST_HOP | 65 |
| SYNA | SEMICONDUCTORS_AI_CHIPS | 0.8824069556806045 | HIGH | FIRST_HOP | 79 |
| BTX | OTHER_OR_UNCLASSIFIED | 0.8822195785592452 | HIGH | FIRST_HOP | 101 |

## Top 10 Multi-Seed Candidates
| ticker | subtheme_guess | seed_neighbor_count | best_seed_edge_score | priority_score |
| --- | --- | --- | --- | --- |
| BTX | OTHER_OR_UNCLASSIFIED | 101 | 0.6634845101692721 | 0.8822195785592452 |
| ASG | OTHER_OR_UNCLASSIFIED | 101 | 0.6447158382883343 | 0.875650543400917 |
| BSTZ | OTHER_OR_UNCLASSIFIED | 100 | 0.6467704907045689 | 0.8763696717465991 |
| RMT | OTHER_OR_UNCLASSIFIED | 99 | 0.6769645724280899 | 0.8869376003498314 |
| BST | OTHER_OR_UNCLASSIFIED | 98 | 0.6159791992928813 | 0.8655927197525084 |
| EOS | OTHER_OR_UNCLASSIFIED | 98 | 0.5959455331712745 | 0.858580936609946 |
| GDV | OTHER_OR_UNCLASSIFIED | 96 | 0.6470579798991813 | 0.8764702929647135 |
| AOD | OTHER_OR_UNCLASSIFIED | 96 | 0.5865996491828782 | 0.8553098772140073 |
| CHW | OTHER_OR_UNCLASSIFIED | 96 | 0.5799348294820166 | 0.8529771903187058 |
| RVT | OTHER_OR_UNCLASSIFIED | 95 | 0.6909781675243657 | 0.8918423586335279 |

## Top 10 Cross-Sector Bridges
| ticker_1 | ticker_2 | combined_score | ticker_1_subtheme_guess | ticker_2_subtheme_guess |
| --- | --- | --- | --- | --- |
| CCS | LEN | 0.8238522672597172 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| CCS | DHI | 0.7976834343845596 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| CCS | TPH | 0.7734174226609026 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| BLD | TOL | 0.7585429529463911 | ENGINEERING_CONSTRUCTION_INFRA | OTHER_OR_UNCLASSIFIED |
| FBIN | MHK | 0.739569665124263 | ELECTRICAL_POWER_EQUIPMENT | OTHER_OR_UNCLASSIFIED |
| CCS | GRBK | 0.7386638994792832 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| FBIN | UFPI | 0.7348945473697177 | ELECTRICAL_POWER_EQUIPMENT | OTHER_OR_UNCLASSIFIED |
| CCS | HOV | 0.7314835763958288 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| BLD | PHM | 0.731336278939655 | ENGINEERING_CONSTRUCTION_INFRA | OTHER_OR_UNCLASSIFIED |
| MAS | UFPI | 0.7266630368753698 | ELECTRICAL_POWER_EQUIPMENT | OTHER_OR_UNCLASSIFIED |

## Subtheme Summary Counts
| subtheme_guess | candidate_count | high_priority_count | medium_priority_count | low_priority_count |
| --- | --- | --- | --- | --- |
| OTHER_OR_UNCLASSIFIED | 184 | 184 | 0 | 0 |
| BROAD_TECH_OR_INDUSTRIAL_BETA | 57 | 57 | 0 | 0 |
| ELECTRICAL_POWER_EQUIPMENT | 25 | 25 | 0 | 0 |
| SEMICONDUCTORS_AI_CHIPS | 12 | 12 | 0 | 0 |
| NETWORKING_OPTICAL_CONNECTIVITY | 7 | 7 | 0 | 0 |
| SOFTWARE_PLATFORM_ADJACENT | 7 | 7 | 0 | 0 |
| SEMICONDUCTOR_EQUIPMENT | 4 | 4 | 0 | 0 |
| ENGINEERING_CONSTRUCTION_INFRA | 2 | 2 | 0 | 0 |
| DATACENTER_REIT | 1 | 1 | 0 | 0 |
| SERVER_STORAGE_HARDWARE | 1 | 1 | 0 | 0 |

## Top 10 Core Validation Candidates
| ticker | subtheme_guess | priority_score | priority_tier | seed_status | seed_neighbor_count |
| --- | --- | --- | --- | --- | --- |
| LFUS | NETWORKING_OPTICAL_CONNECTIVITY | 0.8914156171911403 | HIGH | FIRST_HOP | 94 |
| KLIC | SEMICONDUCTOR_EQUIPMENT | 0.888354828133788 | HIGH | FIRST_HOP | 90 |
| DIOD | SEMICONDUCTORS_AI_CHIPS | 0.8867542632478249 | HIGH | FIRST_HOP | 77 |
| SYNA | SEMICONDUCTORS_AI_CHIPS | 0.8824069556806045 | HIGH | FIRST_HOP | 79 |
| VSH | SEMICONDUCTORS_AI_CHIPS | 0.875095642230978 | HIGH | FIRST_HOP | 84 |
| DCI | ELECTRICAL_POWER_EQUIPMENT | 0.8741256436911703 | HIGH | FIRST_HOP | 70 |
| KN | NETWORKING_OPTICAL_CONNECTIVITY | 0.8740570958576569 | HIGH | FIRST_HOP | 87 |
| APG | ENGINEERING_CONSTRUCTION_INFRA | 0.8740022291857193 | HIGH | FIRST_HOP | 89 |
| ITW | ELECTRICAL_POWER_EQUIPMENT | 0.8735893243665501 | HIGH | FIRST_HOP | 43 |
| ALGM | SEMICONDUCTORS_AI_CHIPS | 0.8726136615244916 | HIGH | FIRST_HOP | 72 |

## Top 10 Broad Beta Candidates
| ticker | subtheme_guess | priority_score | priority_tier | seed_status | seed_neighbor_count |
| --- | --- | --- | --- | --- | --- |
| RVT | OTHER_OR_UNCLASSIFIED | 0.8918423586335279 | HIGH | FIRST_HOP | 95 |
| TKR | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.889179254968784 | HIGH | FIRST_HOP | 73 |
| RMT | OTHER_OR_UNCLASSIFIED | 0.8869376003498314 | HIGH | FIRST_HOP | 99 |
| LECO | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.8836972034265113 | HIGH | FIRST_HOP | 56 |
| AIT | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.8833838357879209 | HIGH | FIRST_HOP | 65 |
| BTX | OTHER_OR_UNCLASSIFIED | 0.8822195785592452 | HIGH | FIRST_HOP | 101 |
| BC | OTHER_OR_UNCLASSIFIED | 0.8805626669308557 | HIGH | FIRST_HOP | 50 |
| CAT | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.8781225560443097 | HIGH | FIRST_HOP | 86 |
| EVT | OTHER_OR_UNCLASSIFIED | 0.8775924563720583 | HIGH | FIRST_HOP | 83 |
| FTV | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.8775551531850972 | HIGH | FIRST_HOP | 67 |

## Important Notes
- These outputs are statistical candidate signals, not proof of business relationships.
- All candidates require manual business validation before investment use.
- datacenter_core_validation_shortlist.csv is the preferred manual validation starting point.
- datacenter_broad_beta_candidates.csv contains statistically connected but less directly operational candidates.
- Financial Services and fund-like/market-beta candidates are intentionally separated from the operating datacenter ecosystem list.
