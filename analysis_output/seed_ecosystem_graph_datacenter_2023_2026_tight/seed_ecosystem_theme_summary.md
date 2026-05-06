# Seed Ecosystem Theme: datacenter

Input database: `usa_close_change.db`
Raw full directory: `analysis_output/similar_stocks_2023_2026`
Residual full directory: `analysis_output/residual_similar_stocks_2023_2026_wide`
Output directory: `analysis_output/seed_ecosystem_graph_datacenter_2023_2026_tight`

Theme name: `datacenter`
Seed tickers (111): AAOI, ACA, ACLS, ACM, ADI, AEIS, AMAT, AMD, AME, AMKR, ANET, APH, ARM, ASML, ASX, AVGO, BDC, BHE, CAMT, CEG, CIEN, CLS, CMI, COHR, COHU, CR, CSW, CTS, DELL, DLR, DOV, DY, ECG, EME, EMR, ENTG, EQIX, ETN, FIX, FLEX, FLR, FLS, FN, FORM, GEV, GGG, GLW, GNRC, GTES, HPE, HUBB, ICHR, IESC, IEX, IR, ITT, J, JBL, JCI, KLAC, LITE, LRCX, LSCC, MCHP, MKSI, MPWR, MRVL, MTZ, MU, MYRG, NDSN, NEE, NPO, NRG, NTAP, NVDA, NVMI, NVT, NXPI, ON, ONTO, PEG, PH, PLXS, PNR, POWI, POWL, PRIM, PSTG, PWR, QCOM, RMBS, ROK, RRX, SMCI, SPXC, STM, STRL, STX, TEL, TER, TLN, TSM, TT, TTMI, TXN, UCTT, VECO, VRT, VST, WDC

## Why V6b is Seed-Centric

The previous V6 graph engine used only top-pair report files, which contain the most
generally correlated pairs across all tickers. This means thematic companies (e.g.
datacenter ecosystem) can be underrepresented if their strongest relationships are
with specific sector peers rather than the globally top-correlated pairs.

V6b instead reads full pair files (millions of rows) and builds the graph starting
from seed tickers. This allows discovering all statistically connected peers, even
those that would never appear in a generic top-N list.

## Thresholds

- Seed raw min correlation: `0.4`
- Seed residual min correlation: `0.25`
- General raw min correlation: `0.45`
- General residual min correlation: `0.3`
- Second-hop min combined score: `0.5`
- Overall min combined score: `0.4`
- Max first-hop nodes: `200`
- Max second-hop nodes: `400`
- Min component size: `3`
- Min / max clique size: `3` / `6`

## Graph Construction

**Pass 1**: Stream all full pair files. Extract edges where at least one ticker
is a seed. Apply seed-specific (lower) correlation thresholds. Rank non-seed
first-hop nodes by best seed edge score. Retain at most 200 first-hop nodes.

**Pass 2**: Stream all full pair files again. Extract edges from retained first-hop
nodes to new (non-retained) second-hop candidates. Apply general thresholds and
second-hop min combined score. Retain at most 400 second-hop nodes.

**Final edge set**: Combine pass1 + pass2 + rolling edges. Keep only edges where
both endpoints are retained. Apply overall min combined score.

## Summary Statistics

- Seed tickers found in graph: `109`
- Retained first-hop nodes: `200`
- Retained second-hop nodes: `400`
- Graph nodes: `709`
- Graph edges: `41205`
- Components (total): `1`
- First-hop candidates: `592`
- Second-hop candidates: `8`
- Cross-sector edges: `20151`
- Cliques found: `464`
- Cliques written: `464`

## Combined Score

Each edge has a `combined_score` computed as a weighted average of available metrics:
raw_correlation (0.25), raw_rolling_corr_mean (0.15), residual_correlation (0.40),
residual_rolling_corr_mean (0.20). Missing metrics are dropped and remaining
weights rescaled to sum to 1.

## First-Hop and Second-Hop Nodes

**First-hop**: Non-seed tickers directly connected to at least one seed ticker.
These are the most direct statistical peers of the theme tickers.

**Second-hop**: Tickers connected to first-hop nodes but not directly to seeds.
These are statistical peers of peers — potentially theme-adjacent companies.

## Important Caveats

Graph connections are statistical similarity measures, not proof of business
relationship or causality. Two stocks may have high correlation for reasons
unrelated to the datacenter theme: shared macro sensitivity, sector rotation,
or statistical coincidence.

Datacenter ecosystem interpretation requires business validation. Not all tickers
in the graph are directly linked to the datacenter theme.
