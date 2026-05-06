# Datacenter Focused Ecosystem Report

## Run Context
- input_dir: analysis_output/seed_ecosystem_graph_datacenter_2024_2026_wide
- output_dir: analysis_output/seed_ecosystem_graph_datacenter_2024_2026_report
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
- seeds_found_in_graph: 30
- priority_candidates: 300
- multi_seed_candidates: 300
- cross_sector_bridges: 300
- subthemes: 10

## Top 10 Priority Candidates
| ticker | subtheme_guess | priority_score | priority_tier | seed_status | seed_neighbor_count |
| --- | --- | --- | --- | --- | --- |
| MKSI | SEMICONDUCTOR_EQUIPMENT | 0.9218145004104947 | HIGH | FIRST_HOP | 29 |
| NVMI | SEMICONDUCTOR_EQUIPMENT | 0.9144351128019337 | HIGH | FIRST_HOP | 28 |
| NRG | POWER_GENERATION_UTILITIES | 0.9052710562273778 | HIGH | FIRST_HOP | 28 |
| MU | SEMICONDUCTOR_EQUIPMENT | 0.9045175136179548 | HIGH | FIRST_HOP | 28 |
| ASX | SEMICONDUCTOR_EQUIPMENT | 0.9036085511106621 | HIGH | FIRST_HOP | 28 |
| ONTO | SEMICONDUCTOR_EQUIPMENT | 0.9005242236707988 | HIGH | FIRST_HOP | 28 |
| ENTG | SEMICONDUCTOR_EQUIPMENT | 0.8929519560398688 | HIGH | FIRST_HOP | 28 |
| CAMT | SEMICONDUCTOR_EQUIPMENT | 0.8928916422736582 | HIGH | FIRST_HOP | 28 |
| UCTT | SEMICONDUCTOR_EQUIPMENT | 0.8925297136761494 | HIGH | FIRST_HOP | 28 |
| VECO | SEMICONDUCTOR_EQUIPMENT | 0.8915762700661345 | HIGH | FIRST_HOP | 28 |

## Top 10 Multi-Seed Candidates
| ticker | subtheme_guess | seed_neighbor_count | best_seed_edge_score | priority_score |
| --- | --- | --- | --- | --- |
| MKSI | SEMICONDUCTOR_EQUIPMENT | 29 | 0.7766128583156994 | 0.9218145004104947 |
| FIX | ENGINEERING_CONSTRUCTION_INFRA | 29 | 0.6728480988903438 | 0.8854968346116203 |
| RMT | OTHER_OR_UNCLASSIFIED | 29 | 0.6728025302545927 | 0.8854808855891074 |
| MTSI | SEMICONDUCTOR_EQUIPMENT | 29 | 0.6400064551014625 | 0.8740022592855118 |
| EME | ENGINEERING_CONSTRUCTION_INFRA | 29 | 0.6288277268210002 | 0.87008970438735 |
| KN | NETWORKING_OPTICAL_CONNECTIVITY | 29 | 0.6234047865475602 | 0.868191675291646 |
| HUBB | ELECTRICAL_POWER_EQUIPMENT | 29 | 0.5962628623882353 | 0.8586920018358823 |
| SITM | SEMICONDUCTOR_EQUIPMENT | 29 | 0.5569517524861741 | 0.8449331133701609 |
| GDV | OTHER_OR_UNCLASSIFIED | 29 | 0.5338081641172432 | 0.8368328574410351 |
| BSTZ | OTHER_OR_UNCLASSIFIED | 29 | 0.5334481927459598 | 0.8367068674610859 |

## Top 10 Cross-Sector Bridges
| ticker_1 | ticker_2 | combined_score | ticker_1_subtheme_guess | ticker_2_subtheme_guess |
| --- | --- | --- | --- | --- |
| CIFR | RIOT | 0.7127292740706654 | SOFTWARE_PLATFORM_ADJACENT | OTHER_OR_UNCLASSIFIED |
| CCS | LGIH | 0.7062459882728599 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| BLD | IBP | 0.6830431832097473 | ENGINEERING_CONSTRUCTION_INFRA | OTHER_OR_UNCLASSIFIED |
| EMR | RMT | 0.6728025302545927 | ELECTRICAL_POWER_EQUIPMENT | OTHER_OR_UNCLASSIFIED |
| MAR | PK | 0.6699749585893396 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| BC | GGG | 0.6670897419201007 | OTHER_OR_UNCLASSIFIED | ELECTRICAL_POWER_EQUIPMENT |
| ITT | RVT | 0.6668496345617069 | ELECTRICAL_POWER_EQUIPMENT | OTHER_OR_UNCLASSIFIED |
| PEB | RVT | 0.6614479508366163 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| RVT | XHR | 0.6599420080064665 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| MAR | XHR | 0.6594686221859006 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |

## Subtheme Summary Counts
| subtheme_guess | candidate_count | high_priority_count | medium_priority_count | low_priority_count |
| --- | --- | --- | --- | --- |
| OTHER_OR_UNCLASSIFIED | 94 | 94 | 0 | 0 |
| SEMICONDUCTOR_EQUIPMENT | 48 | 48 | 0 | 0 |
| BROAD_TECH_OR_INDUSTRIAL_BETA | 47 | 47 | 0 | 0 |
| SOFTWARE_PLATFORM_ADJACENT | 37 | 37 | 0 | 0 |
| ELECTRICAL_POWER_EQUIPMENT | 28 | 28 | 0 | 0 |
| NETWORKING_OPTICAL_CONNECTIVITY | 28 | 28 | 0 | 0 |
| ENGINEERING_CONSTRUCTION_INFRA | 10 | 10 | 0 | 0 |
| SERVER_STORAGE_HARDWARE | 5 | 5 | 0 | 0 |
| POWER_GENERATION_UTILITIES | 2 | 2 | 0 | 0 |
| DATACENTER_REIT | 1 | 1 | 0 | 0 |

## Important Notes
- These outputs are statistical candidate signals, not proof of business relationships.
- All candidates require manual business validation before investment use.
