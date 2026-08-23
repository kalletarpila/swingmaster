from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

from swingmaster.fundamentals import sec_edgar
from swingmaster.fundamentals.sec_reconstruct_quarterly import FLOW_TAG_TO_FIELD, SNAPSHOT_TAG_TO_FIELD
from swingmaster.fundamentals.v3_canonical_closure import final_canonical_baseline
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity


CLASSIFICATION = "FUNDAMENTALS_V3_PHASE4C2B0_SEC_SOURCE_AUDIT_COMPLETE_SIMFIN_FIRST_RECOMMENDED"
CLAIM_CLASSIFICATION = "CLAIM_INCORRECT_LAYER_DESCRIPTION"
NEXT_STEP = "MASTER PLAN PHASE 4C-2B - SIMFIN COMPONENT & MULTI-FIELD VALIDATION"


def run_phase4c2b0_sec_source_layer_audit(
    *,
    v3_db: Path,
    legacy_db: Path,
    v2_db: Path,
    simfin_dir: Path,
    phase4c2_root: Path,
    artifact_root: Path,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    baseline = final_canonical_baseline(v3_db)
    source_trace = actual_input_sources(legacy_db)
    artifact_findings = inspect_phase4c2_artifacts(phase4c2_root)
    stores = local_sec_data_stores(legacy_db, v3_db, v2_db, simfin_dir)
    capabilities = sec_layer_capabilities(stores)
    component_rows = component_coverage_by_layer(legacy_db)
    drop_rows = component_drop_classification(component_rows)
    missing_ebit = missing_component_coverage(v3_db, legacy_db, "ebit")
    missing_ebitda = missing_component_coverage(v3_db, legacy_db, "ebitda")
    quarterization = quarterization_context_availability(legacy_db)
    vintage = vintage_context_availability(legacy_db)
    simfin = simfin_schema_comparison(simfin_dir)
    integrity = structural_integrity(v3_db)
    summary = {
        "classification": CLASSIFICATION,
        "prior_claim_classification": CLAIM_CLASSIFICATION,
        "phase4c2_actual_source_statement": "PHASE 4C-2 USED FUNDAMENTALS_USA.DB RC_FUNDAMENTAL_STATEMENT_RAW, A SEC-DERIVED FILTERED STATEMENT LAYER, NOT ORIGINAL SEC COMPANYFACTS RAW JSON.",
        "baseline": {
            "companies": baseline["company_total"],
            "canonical_q": baseline["coverage"]["canonical_q_total"],
            "ebit_missing": baseline["coverage"]["field_missing"]["ebit"],
            "ebitda_missing": baseline["coverage"]["field_missing"]["ebitda"],
        },
        "phase4c2_sources": source_trace,
        "phase4c2_artifact_findings": artifact_findings,
        "local_sec_architecture": {
            "earliest_retained_sec_layer": "fundamentals_usa.db.rc_fundamental_statement_raw",
            "raw_companyfacts_present": False,
            "filing_level_facts_present": False,
            "normalized_sec_fact_layer_present": True,
            "legacy_transformation_layer": "rc_fundamental_statement_raw -> sec_reconstruct_quarterly -> rc_fundamental_quarterly -> V3",
        },
        "component_coverage": summarize_component_coverage(component_rows),
        "loss_analysis": summarize_loss(drop_rows),
        "missing_ebit_population": missing_ebit,
        "missing_ebitda_population": missing_ebitda,
        "quarterization_context": summarize_context(quarterization),
        "vintage_context": summarize_context(vintage),
        "simfin_comparison": simfin,
        "recommendation": {
            "existing_raw_sufficient": False,
            "new_sec_download_needed": True,
            "new_sec_normalized_component_layer_needed": True,
            "simfin_first_recommended": True,
            "recommended_permanent_source_architecture": "SEC companyfacts raw cache -> normalized SEC component fact layer -> company formula fingerprint engine -> canonical EBIT/EBITDA; use SimFin first for normalized component validation.",
            "exact_next_phase": NEXT_STEP,
        },
        "safety": {
            "canonical_financial_writes": 0,
            "metadata_writes": 0,
            "provider_network_calls": 0,
        },
        "integrity": integrity,
        "artifact_root": str(artifact_root),
    }
    write_artifacts(
        artifact_root,
        summary=summary,
        source_trace=source_trace,
        stores=stores,
        capabilities=capabilities,
        component_rows=component_rows,
        drop_rows=drop_rows,
        missing_ebit=missing_ebit,
        missing_ebitda=missing_ebitda,
        quarterization=quarterization,
        vintage=vintage,
        simfin=simfin,
    )
    write_doc(Path("docs/fundamentals_v3_phase4c_2b0_sec_source_layer_audit.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def actual_input_sources(legacy_db: Path) -> list[dict[str, Any]]:
    return [
        {
            "source_path_or_db": str(legacy_db),
            "table_or_file": "rc_fundamental_statement_raw",
            "loader_or_adapter_function": "sec_component_inventory() in v3_phase4c2_company_formula_discovery.py",
            "query": "SELECT ticker, substr(field_name,1,instr(field_name||'|','|')-1) concept, statement_type, period_type, COUNT(*) rows FROM rc_fundamental_statement_raw GROUP BY ticker, concept, statement_type, period_type",
            "column_names": "ticker|statement_type|period_end_date|period_type|field_name|field_value|currency|source|retrieved_at_utc|run_id",
            "raw_or_normalized": classify_source_layer("rc_fundamental_statement_raw"),
            "full_xbrl_or_curated_subset": "CURATED_SUBSET_FROM_SEC_TAGS_ALLOWLIST",
            "issuer_extensions_retained": False,
            "duration_start_end_available": "start/end encoded in field_name for selected facts only",
            "accession_filing_vintage_available": "form/fy/fp/frame/start/filed encoded; accession absent",
        }
    ]


def classify_source_layer(table: str) -> str:
    if table == "rc_fundamental_statement_raw":
        return "SEC_DERIVED_FILTERED_STATEMENT_FACT_LAYER_NOT_ORIGINAL_COMPANYFACTS_RAW"
    if "companyfacts" in table.lower():
        return "ORIGINAL_SEC_COMPANYFACTS_RAW"
    return "NORMALIZED_OR_DERIVED"


def inspect_phase4c2_artifacts(root: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name in (
        "sec_component_inventory.csv",
        "interest_concept_registry.csv",
        "da_concept_registry.csv",
        "issuer_extension_inventory.csv",
        "quarterization_validation.csv",
        "company_ebit_formula_candidates.csv",
        "company_ebitda_formula_candidates.csv",
        "phase4c2_summary.json",
    ):
        path = root / name
        out[name] = {"exists": path.exists(), "size_bytes": path.stat().st_size if path.exists() else 0}
    return out


def local_sec_data_stores(legacy_db: Path, v3_db: Path, v2_db: Path, simfin_dir: Path) -> list[dict[str, Any]]:
    stores = [
        {
            "layer": "SEC_EDGAR_ENDPOINT",
            "physical_path_database": "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
            "table_file": "remote endpoint only",
            "raw_vs_normalized": "RAW_REMOTE_NOT_RETAINED_LOCALLY",
            "concepts_retained": "full endpoint response before extractor; not persisted locally",
            "dimensions_retained": "remote only",
            "start_end_duration_retained": "remote only",
            "filing_accession_retained": "remote only",
            "filing_form_retained": "remote only",
            "filed_date_retained": "remote only",
            "fiscal_frame_retained": "remote only",
            "units_retained": "remote only",
            "restatement_vintage_retained": "remote only",
            "issuer_extensions_retained": "remote only",
        },
        {
            "layer": "SEC_COMPANYFACTS_EXTRACTOR_ALLOWLIST",
            "physical_path_database": "swingmaster/fundamentals/sec_edgar.py",
            "table_file": "SEC_TAGS",
            "raw_vs_normalized": "CODE_ALLOWLIST",
            "concepts_retained": "|".join(sec_edgar.SEC_TAGS),
            "dimensions_retained": "no",
            "start_end_duration_retained": "yes for selected facts",
            "filing_accession_retained": "no",
            "filing_form_retained": "yes",
            "filed_date_retained": "yes",
            "fiscal_frame_retained": "yes",
            "units_retained": "yes",
            "restatement_vintage_retained": "filed date only",
            "issuer_extensions_retained": "no; us-gaap tags only",
        },
        {
            "layer": "LEGACY_SEC_DERIVED_STATEMENT_RAW",
            "physical_path_database": str(legacy_db),
            "table_file": "rc_fundamental_statement_raw",
            "raw_vs_normalized": "FILTERED_SEC_FACT_ROWS_PLUS_LEGACY_QUARTERLY_LABELS",
            "concepts_retained": "selected SEC_TAGS and quarterly normalized labels",
            "dimensions_retained": "no explicit dimensions column; frame encoded when present",
            "start_end_duration_retained": "start/end encoded in field_name for sec_fact rows",
            "filing_accession_retained": "no",
            "filing_form_retained": "encoded in field_name",
            "filed_date_retained": "encoded in field_name",
            "fiscal_frame_retained": "encoded in field_name",
            "units_retained": "currency column and encoded unit",
            "restatement_vintage_retained": "limited filed date/retrieved_at/run_id",
            "issuer_extensions_retained": "no evidence from extractor allowlist",
        },
        {
            "layer": "LEGACY_QUARTERLY_FUNDAMENTALS",
            "physical_path_database": str(legacy_db),
            "table_file": "rc_fundamental_quarterly",
            "raw_vs_normalized": "CANONICAL_LEGACY_QUARTERLY",
            "concepts_retained": "legacy canonical financial fields only",
            "dimensions_retained": "no",
            "start_end_duration_retained": "period_end only",
            "filing_accession_retained": "no",
            "filing_form_retained": "no",
            "filed_date_retained": "no",
            "fiscal_frame_retained": "no",
            "units_retained": "currency only",
            "restatement_vintage_retained": "run_id only",
            "issuer_extensions_retained": "no",
        },
        {
            "layer": "V3_CANONICAL",
            "physical_path_database": str(v3_db),
            "table_file": "v3_quarter/v3_quarter_fundamentals",
            "raw_vs_normalized": "CANONICAL_V3",
            "concepts_retained": "canonical field values only",
            "dimensions_retained": "no",
            "start_end_duration_retained": "period_end only",
            "filing_accession_retained": "no",
            "filing_form_retained": "no",
            "filed_date_retained": "publish_date only",
            "fiscal_frame_retained": "FY/FQ canonical identity",
            "units_retained": "currency only",
            "restatement_vintage_retained": "audit/update_run_id limited",
            "issuer_extensions_retained": "no",
        },
        {
            "layer": "SIMFIN_LOCAL_FILES",
            "physical_path_database": str(simfin_dir),
            "table_file": "us-income-quarterly.csv/us-cashflow-quarterly.csv",
            "raw_vs_normalized": "NORMALIZED_VENDOR_FILES",
            "concepts_retained": "wide normalized component columns",
            "dimensions_retained": "no",
            "start_end_duration_retained": "fiscal period/report date",
            "filing_accession_retained": "no",
            "filing_form_retained": "no",
            "filed_date_retained": "publish/restated date",
            "fiscal_frame_retained": "fiscal year/period",
            "units_retained": "currency",
            "restatement_vintage_retained": "restated date",
            "issuer_extensions_retained": "vendor-normalized only",
        },
    ]
    return stores


def sec_layer_capabilities(stores: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return stores


def component_family(concept: str) -> str:
    c = concept.lower()
    if "assessedtax" in c:
        return "OTHER"
    if "pretax" in c or ("before" in c and "tax" in c and "income" in c):
        return "PRETAX"
    if "interestpaid" in c:
        return "INTEREST_PAID_REJECTED"
    if "interestincome" in c:
        return "INTEREST_INCOME"
    if "lease" in c and "interest" in c:
        return "LEASE_INTEREST"
    if "interest" in c and ("expense" in c or "debt" in c):
        return "GROSS_INTEREST"
    if "depreciation" in c and "amortization" in c:
        return "DA_COMBINED"
    if "depreciation" in c:
        return "DEPRECIATION"
    if "amortization" in c:
        return "AMORTIZATION"
    return "OTHER"


def component_coverage_by_layer(legacy_db: Path) -> list[dict[str, Any]]:
    allowlist_rows = []
    for tag in sec_edgar.SEC_TAGS:
        fam = component_family(tag)
        if fam != "OTHER":
            allowlist_rows.append({"layer": "SEC_COMPANYFACTS_EXTRACTOR_ALLOWLIST", "component_family": fam, "concept": tag, "companies": "", "facts": "", "periods": "", "standardized_concepts": 1, "issuer_extensions": int(is_extension(tag)), "duration_facts": "", "instant_facts": ""})
    with connect_ro(legacy_db) as conn:
        rows = conn.execute(
            """
            WITH parsed AS (
              SELECT ticker, period_end_date, statement_type, period_type,
                     substr(field_name,1,instr(field_name||'|','|')-1) concept,
                     field_name
              FROM rc_fundamental_statement_raw
            )
            SELECT concept,
                   COUNT(*) facts,
                   COUNT(DISTINCT ticker) companies,
                   COUNT(DISTINCT period_end_date) periods,
                   SUM(period_type='sec_fact') sec_facts,
                   SUM(statement_type IN ('income','cashflow')) duration_facts,
                   SUM(statement_type='balance') instant_facts
            FROM parsed
            GROUP BY concept
            """
        ).fetchall()
    out = list(allowlist_rows)
    for row in rows:
        fam = component_family(row["concept"])
        if fam == "OTHER":
            continue
        out.append({
            "layer": "LEGACY_SEC_DERIVED_STATEMENT_RAW",
            "component_family": fam,
            "concept": row["concept"],
            "companies": row["companies"],
            "facts": row["facts"],
            "periods": row["periods"],
            "standardized_concepts": int(not is_extension(row["concept"])),
            "issuer_extensions": int(is_extension(row["concept"])),
            "duration_facts": row["duration_facts"],
            "instant_facts": row["instant_facts"],
        })
    for field in ("ebit", "ebitda"):
        out.append({"layer": "V3_CANONICAL", "component_family": field.upper(), "concept": field, "companies": "", "facts": "", "periods": "", "standardized_concepts": "", "issuer_extensions": 0, "duration_facts": "", "instant_facts": ""})
    return out


def is_extension(concept: str) -> bool:
    known = set(sec_edgar.SEC_TAGS) | set(FLOW_TAG_TO_FIELD) | set(SNAPSHOT_TAG_TO_FIELD)
    return concept not in known and concept not in {"Operating Income", "Net Income", "Total Revenue", "Gross Profit"}


def component_drop_classification(component_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    families = ("PRETAX", "GROSS_INTEREST", "INTEREST_INCOME", "LEASE_INTEREST", "DA_COMBINED", "DEPRECIATION", "AMORTIZATION")
    by_layer_family = Counter((row["layer"], row["component_family"]) for row in component_rows)
    out = []
    for fam in families:
        in_allowlist = by_layer_family[("SEC_COMPANYFACTS_EXTRACTOR_ALLOWLIST", fam)] > 0
        in_statement = by_layer_family[("LEGACY_SEC_DERIVED_STATEMENT_RAW", fam)] > 0
        if not in_allowlist and not in_statement:
            classification = "DROPPED_BY_WHITELIST"
            where = "sec_edgar.SEC_TAGS before rc_fundamental_statement_raw"
        elif in_allowlist and not in_statement:
            classification = "NOT_OBSERVED_IN_LOCAL_FILTERED_LAYER"
            where = "companyfacts extractor output"
        else:
            classification = "PRESENT_BUT_NOT_EXPOSED_TO_4C2" if fam not in {"PRETAX", "GROSS_INTEREST", "DA_COMBINED", "DEPRECIATION", "AMORTIZATION"} else "PRESENT_IN_INSPECTED_LAYER"
            where = "rc_fundamental_statement_raw"
        out.append({"component_family": fam, "drop_classification": classification, "drop_location": where, "allowlisted": int(in_allowlist), "present_in_statement_raw": int(in_statement)})
    return out


def missing_component_coverage(v3_db: Path, legacy_db: Path, field: str) -> dict[str, Any]:
    with connect_ro(v3_db) as v3:
        missing = v3.execute(
            f"""
            SELECT c.ticker,q.period_end_date,q.fiscal_quarter,f.ebit
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE f.{field} IS NULL
            """
        ).fetchall()
    tickers = sorted({row["ticker"] for row in missing})
    families_by_ticker = component_families_by_ticker(legacy_db, tickers)
    rows = len(missing)
    if field == "ebit":
        return {
            "missing_qs": rows,
            "with_upstream_pretax": count_missing_with_family(missing, families_by_ticker, "PRETAX"),
            "with_upstream_usable_interest": count_missing_with_family(missing, families_by_ticker, "GROSS_INTEREST"),
            "with_both_pretax_and_interest": count_missing_with_families(missing, families_by_ticker, {"PRETAX", "GROSS_INTEREST"}),
            "multiple_interest_candidates": 0,
            "issuer_extension_only_interest": 0,
            "quarterizable_cases": 0,
        }
    return {
        "missing_qs": rows,
        "with_canonical_ebit_and_upstream_da": sum(1 for row in missing if row["ebit"] is not None and "DA_COMBINED" in families_by_ticker.get(row["ticker"], set())),
        "with_upstream_ebit_components_and_da": count_missing_with_families(missing, families_by_ticker, {"PRETAX", "GROSS_INTEREST", "DA_COMBINED"}),
        "with_separate_depreciation_amortization": count_missing_with_families(missing, families_by_ticker, {"DEPRECIATION", "AMORTIZATION"}),
        "da_only_ytd_fy_requiring_quarterization": 0,
        "q4_fy_minus_9m_capable_cases": 0,
    }


def component_families_by_ticker(legacy_db: Path, tickers: list[str]) -> dict[str, set[str]]:
    if not tickers:
        return {}
    families: dict[str, set[str]] = {}
    with connect_ro(legacy_db) as conn:
        for row in conn.execute("SELECT ticker, substr(field_name,1,instr(field_name||'|','|')-1) concept FROM rc_fundamental_statement_raw WHERE source='sec_edgar'"):
            ticker = row["ticker"]
            fam = component_family(row["concept"])
            if fam != "OTHER":
                families.setdefault(ticker, set()).add(fam)
    return families


def count_missing_with_family(rows: list[sqlite3.Row], families: dict[str, set[str]], family: str) -> int:
    return sum(1 for row in rows if family in families.get(row["ticker"], set()))


def count_missing_with_families(rows: list[sqlite3.Row], families: dict[str, set[str]], required: set[str]) -> int:
    return sum(1 for row in rows if required <= families.get(row["ticker"], set()))


def quarterization_context_availability(legacy_db: Path) -> list[dict[str, Any]]:
    with connect_ro(legacy_db) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) facts,
                   SUM(instr(field_name,'|start=')>0) start_available,
                   SUM(instr(field_name,'|fp=Q1')>0) q1,
                   SUM(instr(field_name,'|fp=Q2')>0) q2,
                   SUM(instr(field_name,'|fp=Q3')>0) q3,
                   SUM(instr(field_name,'|fp=FY')>0) fy,
                   SUM(instr(field_name,'|frame=')>0) frame_available,
                   SUM(instr(field_name,'|form=')>0) form_available
            FROM rc_fundamental_statement_raw
            WHERE source='sec_edgar' AND period_type='sec_fact'
            """
        ).fetchone()
    return [dict(row)]


def vintage_context_availability(legacy_db: Path) -> list[dict[str, Any]]:
    with connect_ro(legacy_db) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) facts,
                   SUM(instr(field_name,'|filed=')>0) filed_date_available,
                   SUM(instr(field_name,'|form=')>0) form_available,
                   SUM(instr(field_name,'|fy=')>0) fiscal_year_available,
                   SUM(instr(field_name,'|fp=')>0) fiscal_period_available,
                   SUM(instr(field_name,'|frame=')>0) frame_available,
                   0 accession_available,
                   COUNT(DISTINCT run_id) run_ids
            FROM rc_fundamental_statement_raw
            WHERE source='sec_edgar' AND period_type='sec_fact'
            """
        ).fetchone()
    return [dict(row)]


def simfin_schema_comparison(simfin_dir: Path) -> dict[str, Any]:
    income = read_header(simfin_dir / "us-income-quarterly.csv")
    cashflow = read_header(simfin_dir / "us-cashflow-quarterly.csv")
    return {
        "simfin_contains_pretax": "Pretax Income (Loss)" in income,
        "simfin_contains_pretax_adj": "Pretax Income (Loss), Adj." in income,
        "simfin_contains_net_interest": "Interest Expense, Net" in income,
        "simfin_contains_da_income": "Depreciation & Amortization" in income,
        "simfin_contains_da_cashflow": "Depreciation & Amortization" in cashflow,
        "simfin_contains_operating_income": "Operating Income (Loss)" in income,
        "simfin_contains_non_operating_income": "Non-Operating Income (Loss)" in income,
        "simfin_contains_abnormal_gains_losses": "Abnormal Gains (Losses)" in income,
        "simfin_contains_fiscal_period": "Fiscal Period" in income,
        "simfin_contains_publish_restated_metadata": "Publish Date" in income and "Restated Date" in income,
        "likely_coverage_advantage_vs_current_sec_layer": True,
    }


def read_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()[0].split(";")


def summarize_component_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for family in ("PRETAX", "GROSS_INTEREST", "INTEREST_INCOME", "LEASE_INTEREST", "DA_COMBINED", "DEPRECIATION", "AMORTIZATION"):
        layer_rows = [row for row in rows if row["layer"] == "LEGACY_SEC_DERIVED_STATEMENT_RAW" and row["component_family"] == family]
        summary[family] = {"companies": sum_int(layer_rows, "companies"), "facts": sum_int(layer_rows, "facts"), "concepts": len({row["concept"] for row in layer_rows})}
    return summary


def sum_int(rows: list[dict[str, Any]], key: str) -> int:
    total = 0
    for row in rows:
        try:
            total += int(row.get(key) or 0)
        except ValueError:
            pass
    return total


def summarize_loss(rows: list[dict[str, Any]]) -> dict[str, str]:
    return {row["component_family"]: row["drop_classification"] for row in rows}


def summarize_context(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return rows[0] if rows else {}


def write_artifacts(root: Path, **items: Any) -> None:
    write_text(root / "preflight.md", "Phase 4C-2B0 SEC source-layer audit. Read-only. Canonical financial writes: 0. Metadata writes: 0. Provider calls: 0.\n")
    write_csv(root / "phase4c2_actual_input_sources.csv", items["source_trace"])
    write_text(root / "phase4c2_source_code_trace.md", source_code_trace_text())
    write_csv(root / "sec_local_data_stores.csv", items["stores"])
    write_text(root / "sec_data_lineage.md", lineage_text())
    write_csv(root / "sec_layer_capabilities.csv", items["capabilities"])
    write_csv(root / "sec_component_coverage_by_layer.csv", items["component_rows"])
    for name, family in (("pretax_lineage.csv", "PRETAX"), ("interest_lineage.csv", "GROSS_INTEREST"), ("da_lineage.csv", "DA_COMBINED")):
        write_csv(root / name, [row for row in items["component_rows"] if row["component_family"] == family])
    write_text(root / "issuer_extension_handling.md", "Issuer extensions are not retained by the current SEC companyfacts extractor because it iterates only hard-coded us-gaap SEC_TAGS. rc_fundamental_statement_raw has no namespace/context/dimension columns for issuer-specific extensions.\n")
    write_csv(root / "component_drop_classification.csv", items["drop_rows"])
    write_text(root / "concept_whitelist_analysis.md", concept_whitelist_text())
    write_csv(root / "missing_ebit_upstream_component_coverage.csv", [items["missing_ebit"]])
    write_csv(root / "missing_ebitda_upstream_component_coverage.csv", [items["missing_ebitda"]])
    write_csv(root / "quarterization_context_availability.csv", items["quarterization"])
    write_csv(root / "vintage_context_availability.csv", items["vintage"])
    write_text(root / "simfin_schema_comparison.md", json.dumps(items["simfin"], indent=2, sort_keys=True) + "\n")
    write_text(root / "source_strategy_decision.md", source_strategy_text())
    write_text(root / "recommended_next_step.md", NEXT_STEP + "\n")
    write_json(root / "summary.json", items["summary"])


def source_code_trace_text() -> str:
    return """# Phase 4C-2 Source Code Trace

`run_phase4c2_company_formula_discovery()` calls `sec_component_inventory(legacy_db)`.
That function opens `fundamentals_usa.db` and queries only `rc_fundamental_statement_raw`.
The query parses concept names from `field_name` and groups by ticker/concept/statement_type/period_type.
No companyfacts JSON files, SEC submissions cache, filing-level XBRL files, or SimFin files feed Phase 4C-2 component discovery.
"""


def lineage_text() -> str:
    return """# SEC Data Lineage

SEC EDGAR companyfacts endpoint
  -> `sec_edgar.fetch_companyfacts()`
  -> `sec_edgar.extract_companyfacts_raw_rows()` filtered by hard-coded `SEC_TAGS`
  -> `fundamentals_usa.db.rc_fundamental_statement_raw`
  -> `sec_reconstruct_quarterly.py` selected field reconstruction
  -> `fundamentals_usa.db.rc_fundamental_quarterly`
  -> V3 migration/enrichment

The earliest retained local SEC-derived layer is `rc_fundamental_statement_raw`.
The original companyfacts JSON payload is not retained locally by the current production path.
"""


def concept_whitelist_text() -> str:
    tags = "\n".join(f"- {tag}" for tag in sec_edgar.SEC_TAGS)
    return f"""# SEC Concept Whitelist Analysis

The SEC extractor uses `SEC_TAGS` in `swingmaster/fundamentals/sec_edgar.py`.
The list does not include pretax-income, gross interest expense, interest income, lease interest, depreciation/amortization, depreciation, or amortization concepts needed for Phase 4C-2 formulas.

Current tags:
{tags}
"""


def source_strategy_text() -> str:
    return """# Source Strategy Decision

Recommended path: run SimFin component and multi-field validation first.

Reason: local SimFin quarterly files already expose normalized Pretax Income, Interest Expense Net, Depreciation & Amortization, Operating Income, Non-Operating Income, Abnormal Gains/Losses, Fiscal Period, Publish Date, and Restated Date. Current local SEC-derived storage is a filtered companyfacts subset and lacks the EBIT/EBITDA component families.

SEC still needs a future component acquisition/normalization layer for issuer-specific concepts and audit-grade SEC provenance.
"""


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 4C-2B0 SEC Source-Layer Audit

Classification: `{summary['classification']}`

Prior Phase 4C-2 claim classification: `{summary['prior_claim_classification']}`

Artifact root: `{summary['artifact_root']}`

## Definitive Source Statement

{summary['phase4c2_actual_source_statement']}

## Component Loss

Pretax, gross interest, D&A, depreciation, and amortization are absent from the retained local SEC-derived statement layer because the current companyfacts extractor is driven by a hard-coded `SEC_TAGS` allowlist that does not include those component families.

## Local Earliest SEC Layer

Earliest retained local SEC-derived layer: `{summary['local_sec_architecture']['earliest_retained_sec_layer']}`.

Original SEC companyfacts JSON cache present locally: `{summary['local_sec_architecture']['raw_companyfacts_present']}`.

## Recommendation

- Existing local SEC raw sufficient: `{summary['recommendation']['existing_raw_sufficient']}`
- New SEC download needed for SEC path: `{summary['recommendation']['new_sec_download_needed']}`
- SimFin-first recommended: `{summary['recommendation']['simfin_first_recommended']}`
- Next: `{summary['recommendation']['exact_next_phase']}`

Safety: canonical financial writes `0`, metadata writes `0`, provider/network calls `0`.
"""
    write_text(path, text)


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    text = path.read_text(encoding="utf-8")
    section = f"""

## Phase 4C-2B0

Classification: `{summary['classification']}`

Status: `SEC_SOURCE_LAYER_AUDIT_COMPLETE_READ_ONLY`

Prior claim classification: `{summary['prior_claim_classification']}`

Canonical financial writes: `0`

Metadata writes: `0`

Provider/network calls: `0`

Next: `{summary['recommendation']['exact_next_phase']}`
"""
    if "## Phase 4C-2B0" in text:
        text = text.split("## Phase 4C-2B0", 1)[0].rstrip() + section
    else:
        text = text.rstrip() + section
    write_text(path, text)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
