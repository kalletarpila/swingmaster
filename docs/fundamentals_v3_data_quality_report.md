# Fundamentals V3 Data Quality Report

## Headline Dashboard

| Metric | Clean / Available | Total | % |
| --- | ---: | ---: | ---: |
| Full canonical fiscal identity clean | 66030 | 72713 | 90.8091 |
| 2024+ fiscal identity clean | 20461 | 22128 | 92.4666 |
| 2025+ fiscal identity clean | 13330 | 14410 | 92.5052 |
| Latest 8Q rows clean | 18599 | 19728 | 94.2772 |
| Latest 4Q rows clean | 9362 | 9880 | 94.7571 |
| Latest quarter active tickers clean | 2269 | 2470 | 91.8623 |
| Active tickers with all latest4Q clean | 2191 | 2470 | 88.7045 |
| Active tickers with all latest8Q clean | 2053 | 2470 | 83.1174 |
| Current TTM clean | 2199 | 2522 | 87.1927 |
| Score available | 2044 | 2470 | 82.753 |
| Lifecycle available | 1801 | 2470 | 72.915 |
| Valuation available | 1726 | 2470 | 69.8785 |
| All current downstream layers available | 1370 | 2470 | 55.4656 |

## Fundamental Field Coverage

| Field | Full % | 2024+ % | Latest8Q % | Latest4Q % | Latest quarter % |
| --- | ---: | ---: | ---: | ---: | ---: |
| Capex | 92.9586 | 94.5996 | 94.3329 | 94.4939 | 92.6721 |
| Cash | 97.1724 | 98.9787 | 98.7226 | 99.8988 | 99.8785 |
| Debt | 76.0277 | 89.9403 | 90.7897 | 97.0749 | 96.3158 |
| EBIT | 90.9067 | 94.9431 | 95.5089 | 99.0182 | 98.7449 |
| EBITDA | 75.9905 | 93.393 | 92.9745 | 98.5324 | 97.4089 |
| FCF | 93.8745 | 97.632 | 97.6531 | 99.6559 | 99.4737 |
| Gross Profit | 77.6024 | 85.6743 | 85.7157 | 87.9453 | 86.5992 |
| Net Income | 96.6966 | 98.9199 | 98.5908 | 99.3725 | 98.0567 |
| OCF | 99.0456 | 99.6475 | 99.5843 | 99.8178 | 99.7571 |
| Operating Income | 97.2041 | 99.1368 | 99.0217 | 99.5344 | 99.1498 |
| Revenue | 93.0425 | 97.5145 | 97.6379 | 99.8077 | 99.5951 |
| Shares | 92.546 | 99.417 | 99.3867 | 99.585 | 98.3806 |
| period_end | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 |
| publish_date | 95.2347 | 95.8062 | 95.1845 | 93.4919 | 79.2308 |

## Primary Core Completeness

| Scope | Complete | Total | % |
| --- | ---: | ---: | ---: |
| full | 47038 | 72713 | 64.6899 |
| 2024plus | 18535 | 22128 | 83.7627 |
| latest8q | 16863 | 19728 | 85.4775 |
| latest4q | 9470 | 9880 | 95.8502 |
| latest_quarter | 2329 | 2470 | 94.2915 |

## Remaining Risks

| Defect | Count |
| --- | ---: |
| direct FY conflicts | 2988 |
| direct FQ conflicts | 728 |
| transition reviews | 264 |
| unresolved fiscal history | 2703 |
| original Phase 8E blocked rows | 207 |

This report separates fiscal identity quality from field completeness and downstream availability. Clean means no known exact FY conflict, exact FQ conflict, transition review, or unresolved fiscal identity under the current Phase 8D-7/8E classification.
