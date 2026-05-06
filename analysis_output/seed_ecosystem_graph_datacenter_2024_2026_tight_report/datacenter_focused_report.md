# Datacenter Focused Ecosystem Report

## Run Context
- input_dir: analysis_output/seed_ecosystem_graph_datacenter_2024_2026_tight
- output_dir: analysis_output/seed_ecosystem_graph_datacenter_2024_2026_tight_report
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
- core_validation_candidates: 103
- broad_beta_candidates: 197

## Top 10 Priority Candidates
| ticker | subtheme_guess | priority_score | priority_tier | seed_status | seed_neighbor_count |
| --- | --- | --- | --- | --- | --- |
| MKSI | SEMICONDUCTOR_EQUIPMENT | 0.9218145004104947 | HIGH | FIRST_HOP | 28 |
| ASX | SEMICONDUCTORS_AI_CHIPS | 0.9036085511106621 | HIGH | FIRST_HOP | 26 |
| NVMI | SEMICONDUCTOR_EQUIPMENT | 0.9020713199042778 | HIGH | FIRST_HOP | 26 |
| ONTO | SEMICONDUCTOR_EQUIPMENT | 0.9005242236707988 | HIGH | FIRST_HOP | 27 |
| ENTG | SEMICONDUCTOR_EQUIPMENT | 0.8929519560398688 | HIGH | FIRST_HOP | 25 |
| DOV | ELECTRICAL_POWER_EQUIPMENT | 0.8866311623334422 | HIGH | FIRST_HOP | 16 |
| RMT | OTHER_OR_UNCLASSIFIED | 0.8854808855891074 | HIGH | FIRST_HOP | 26 |
| AMKR | SEMICONDUCTOR_EQUIPMENT | 0.8844293170061575 | HIGH | FIRST_HOP | 24 |
| UCTT | SEMICONDUCTOR_EQUIPMENT | 0.8825297136761494 | HIGH | FIRST_HOP | 25 |
| TEL | NETWORKING_OPTICAL_CONNECTIVITY | 0.8824650670967401 | HIGH | FIRST_HOP | 24 |

## Top 10 Multi-Seed Candidates
| ticker | subtheme_guess | seed_neighbor_count | best_seed_edge_score | priority_score |
| --- | --- | --- | --- | --- |
| MKSI | SEMICONDUCTOR_EQUIPMENT | 28 | 0.7766128583156994 | 0.9218145004104947 |
| BST | OTHER_OR_UNCLASSIFIED | 28 | 0.6293346501405829 | 0.870267127549204 |
| BSTZ | OTHER_OR_UNCLASSIFIED | 28 | 0.6057543870158397 | 0.8620140354555439 |
| COHR | NETWORKING_OPTICAL_CONNECTIVITY | 28 | 0.6059303875444011 | 0.8414197548825365 |
| ONTO | SEMICONDUCTOR_EQUIPMENT | 27 | 0.7157834962022825 | 0.9005242236707988 |
| FLEX | NETWORKING_OPTICAL_CONNECTIVITY | 27 | 0.6566149851277316 | 0.879815244794706 |
| ASG | OTHER_OR_UNCLASSIFIED | 27 | 0.6429019365189272 | 0.8750156777816245 |
| MTSI | SEMICONDUCTORS_AI_CHIPS | 27 | 0.6400064551014625 | 0.8740022592855118 |
| AEIS | ELECTRICAL_POWER_EQUIPMENT | 27 | 0.6348416628364567 | 0.8721945819927598 |
| BTX | OTHER_OR_UNCLASSIFIED | 27 | 0.6093502312319816 | 0.8632725809311935 |

## Top 10 Cross-Sector Bridges
| ticker_1 | ticker_2 | combined_score | ticker_1_subtheme_guess | ticker_2_subtheme_guess |
| --- | --- | --- | --- | --- |
| BC | TKR | 0.6844782657783846 | OTHER_OR_UNCLASSIFIED | BROAD_TECH_OR_INDUSTRIAL_BETA |
| AVNT | BC | 0.6787302155120066 | OTHER_OR_UNCLASSIFIED | OTHER_OR_UNCLASSIFIED |
| NOVT | RVT | 0.6752005113227487 | BROAD_TECH_OR_INDUSTRIAL_BETA | OTHER_OR_UNCLASSIFIED |
| BC | NDSN | 0.674862070229732 | OTHER_OR_UNCLASSIFIED | ELECTRICAL_POWER_EQUIPMENT |
| EMR | RMT | 0.6728025302545927 | ELECTRICAL_POWER_EQUIPMENT | OTHER_OR_UNCLASSIFIED |
| APH | ETN | 0.6701096055754292 | NETWORKING_OPTICAL_CONNECTIVITY | ELECTRICAL_POWER_EQUIPMENT |
| FUL | ITW | 0.669247383002816 | OTHER_OR_UNCLASSIFIED | ELECTRICAL_POWER_EQUIPMENT |
| FUL | GGG | 0.6690608698089304 | OTHER_OR_UNCLASSIFIED | ELECTRICAL_POWER_EQUIPMENT |
| NOVT | RMT | 0.6683994125648295 | BROAD_TECH_OR_INDUSTRIAL_BETA | OTHER_OR_UNCLASSIFIED |
| AIT | RUSHA | 0.6676799118496484 | BROAD_TECH_OR_INDUSTRIAL_BETA | OTHER_OR_UNCLASSIFIED |

## Subtheme Summary Counts
| subtheme_guess | candidate_count | high_priority_count | medium_priority_count | low_priority_count |
| --- | --- | --- | --- | --- |
| OTHER_OR_UNCLASSIFIED | 137 | 105 | 32 | 0 |
| BROAD_TECH_OR_INDUSTRIAL_BETA | 33 | 29 | 4 | 0 |
| ELECTRICAL_POWER_EQUIPMENT | 27 | 26 | 1 | 0 |
| SEMICONDUCTORS_AI_CHIPS | 24 | 23 | 1 | 0 |
| NETWORKING_OPTICAL_CONNECTIVITY | 22 | 21 | 1 | 0 |
| SEMICONDUCTOR_EQUIPMENT | 16 | 16 | 0 | 0 |
| SOFTWARE_PLATFORM_ADJACENT | 27 | 13 | 14 | 0 |
| ENGINEERING_CONSTRUCTION_INFRA | 7 | 7 | 0 | 0 |
| SERVER_STORAGE_HARDWARE | 5 | 5 | 0 | 0 |
| POWER_GENERATION_UTILITIES | 2 | 2 | 0 | 0 |

## Top 10 Core Validation Candidates
| ticker | subtheme_guess | priority_score | priority_tier | seed_status | seed_neighbor_count |
| --- | --- | --- | --- | --- | --- |
| MKSI | SEMICONDUCTOR_EQUIPMENT | 0.9218145004104947 | HIGH | FIRST_HOP | 28 |
| ASX | SEMICONDUCTORS_AI_CHIPS | 0.9036085511106621 | HIGH | FIRST_HOP | 26 |
| NVMI | SEMICONDUCTOR_EQUIPMENT | 0.9020713199042778 | HIGH | FIRST_HOP | 26 |
| ONTO | SEMICONDUCTOR_EQUIPMENT | 0.9005242236707988 | HIGH | FIRST_HOP | 27 |
| ENTG | SEMICONDUCTOR_EQUIPMENT | 0.8929519560398688 | HIGH | FIRST_HOP | 25 |
| DOV | ELECTRICAL_POWER_EQUIPMENT | 0.8866311623334422 | HIGH | FIRST_HOP | 16 |
| AMKR | SEMICONDUCTOR_EQUIPMENT | 0.8844293170061575 | HIGH | FIRST_HOP | 24 |
| UCTT | SEMICONDUCTOR_EQUIPMENT | 0.8825297136761494 | HIGH | FIRST_HOP | 25 |
| TEL | NETWORKING_OPTICAL_CONNECTIVITY | 0.8824650670967401 | HIGH | FIRST_HOP | 24 |
| FLEX | NETWORKING_OPTICAL_CONNECTIVITY | 0.879815244794706 | HIGH | FIRST_HOP | 27 |

## Top 10 Broad Beta Candidates
| ticker | subtheme_guess | priority_score | priority_tier | seed_status | seed_neighbor_count |
| --- | --- | --- | --- | --- | --- |
| RMT | OTHER_OR_UNCLASSIFIED | 0.8854808855891074 | HIGH | FIRST_HOP | 26 |
| RVT | OTHER_OR_UNCLASSIFIED | 0.8765501797466644 | HIGH | FIRST_HOP | 24 |
| CAT | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.8763347464294151 | HIGH | FIRST_HOP | 22 |
| EVR | OTHER_OR_UNCLASSIFIED | 0.8754058723864934 | HIGH | FIRST_HOP | 19 |
| AIT | BROAD_TECH_OR_INDUSTRIAL_BETA | 0.8753268830317692 | HIGH | FIRST_HOP | 12 |
| ASG | OTHER_OR_UNCLASSIFIED | 0.8750156777816245 | HIGH | FIRST_HOP | 27 |
| CG | OTHER_OR_UNCLASSIFIED | 0.8716182890133755 | HIGH | FIRST_HOP | 18 |
| BN | OTHER_OR_UNCLASSIFIED | 0.8711604072886834 | HIGH | FIRST_HOP | 23 |
| BST | OTHER_OR_UNCLASSIFIED | 0.870267127549204 | HIGH | FIRST_HOP | 28 |
| EVT | OTHER_OR_UNCLASSIFIED | 0.8677179087166137 | HIGH | FIRST_HOP | 19 |

## Important Notes
- These outputs are statistical candidate signals, not proof of business relationships.
- All candidates require manual business validation before investment use.
- datacenter_core_validation_shortlist.csv is the preferred manual validation starting point.
- datacenter_broad_beta_candidates.csv contains statistically connected but less directly operational candidates.
- Financial Services and fund-like/market-beta candidates are intentionally separated from the operating datacenter ecosystem list.
