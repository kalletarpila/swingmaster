# Vintage and Provenance Deactivation

Date: 2026-07-31

## Policy

SwingMaster now treats `rc_fundamental_quarterly` as the canonical active fundamentals table. Vintage and field-provenance tables remain present as an inactive archive, but normal fundamentals operation must not calculate or write:

- `rc_fundamental_quarterly_vintage`
- `rc_fundamental_quarterly_field_provenance`

The reason is product scope. SwingMaster is a hobby and research-data tool where latest-state fundamentals plus earnings-announcement timing are sufficient for current workflows. The historical investigation in `8aa0584` found that most existing provenance is `UNKNOWN_LEGACY`, full retrospective PIT field reconstruction is not possible, and the extra write complexity is not worth keeping in normal operation.

Earnings announcement matching remains separate. `rc_earnings_event` and effective trading-date logic can still be used for retrospective research timing without reviving field-level vintage/provenance writes.

## Dependency Map

| File/module | Function or CLI | Read/write | Classification | Default behavior | Flag/config | Downstream dependency | Deactivation action |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `swingmaster/fundamentals/reported_vintage_writer.py` | `insert_quarterly_vintage_row` | write vintage | low-level legacy write | rejected | direct call | legacy apply tools | raises `VINTAGE_PROVENANCE_WRITES_DISABLED` |
| `swingmaster/fundamentals/reported_vintage_writer.py` | `insert_quarterly_field_provenance_rows` | write provenance | low-level legacy write | rejected | direct call | legacy apply tools | raises `VINTAGE_PROVENANCE_WRITES_DISABLED` |
| `swingmaster/fundamentals/reported_quarterly_dual_write.py` | `write_normalized_quarterly_rows_with_optional_vintage` | write latest/vintage/provenance | adapter | latest write allowed, vintage disabled | `write_vintage` | SEC/Yahoo adapters | returns `VINTAGE_DISABLED`, writes latest only |
| `swingmaster/fundamentals/reported_quarterly_dual_write.py` | `write_quarterly_latest_and_vintage` | write latest/vintage/provenance | adapter helper | latest write allowed, vintage disabled | direct helper | older adapter tests/tools | returns `VINTAGE_DISABLED`, does not build provenance rows |
| `swingmaster/fundamentals/reported_sec_dual_write_adapter.py` | `write_sec_reconstructed_quarterly_rows_with_optional_vintage` | write latest/vintage/provenance | SEC adapter | latest write allowed, vintage disabled | `write_vintage` | SEC reconstruction CLI | bypasses SEC vintage metadata/provenance construction |
| `swingmaster/fundamentals/reported_yahoo_dual_write_adapter.py` | Yahoo bridge/fallback adapter functions | write latest/vintage/provenance | Yahoo adapter | latest write allowed, vintage disabled | `write_vintage` | Yahoo quarterly/fallback CLIs | bypasses Yahoo vintage metadata/provenance construction |
| `swingmaster/fundamentals/reported_final_mixed_execution.py` | `execute_final_mixed_vintage_write` | write vintage/provenance | mixed-source legacy write | rejected | direct helper | quarter-update final mixed helper | rejects before metadata/write work |
| `swingmaster/cli/run_fundamental_quarter_update.py` | normal quarter update | write latest | active production | latest only | default flags | TTM, score, valuation, snapshots | default summary reports vintage disabled; no vintage planning/execution |
| `swingmaster/cli/run_fundamental_quarter_update.py` | explicit vintage modes | write vintage/provenance | retired production path | rejected | `--write-vintage`, vintage modes | none active | clear disabled error before DB/source mutation |
| `swingmaster/cli/run_fundamental_sec_reconstruct_quarterly.py` | SEC reconstruction CLI | write latest/vintage | active latest, retired vintage | latest only unless retired flag used | `--write-vintage` | latest quarterly | retired flag rejected before write work |
| `swingmaster/cli/run_fundamental_yahoo_to_quarterly.py` | Yahoo quarterly bridge CLI | write latest/vintage | active latest, retired vintage | latest only unless retired flag used | `--write-vintage` | latest quarterly | retired flag rejected before write work |
| `swingmaster/cli/run_fundamental_yahoo_fallback_enrich.py` | Yahoo fallback CLI | write latest/vintage | active latest, retired vintage | latest only unless retired flag used | `--write-vintage` | latest quarterly | retired flag rejected before write work |
| `ui_fundamental_pipeline/command_builder.py` | `build_usa_update_command` | command construction | UI/scheduler integration | latest-only command | `vintage_options` object retained | USA quarter update UI | ignores retired options; no vintage flags emitted |
| `ui_fundamental_pipeline/components/market_panel.py` | USA vintage checkbox | config/UI | compatibility-only | disabled, always false | checkbox value | USA quarter update UI | visible retired label; cannot enable runtime behavior |
| `ui_fundamental_pipeline/vintage_status.py` | apply/recovery gate helpers | decision read | UI compatibility | disabled | summaries from old flows | UI buttons/workflow | returns disabled status/reason |
| `swingmaster/fundamentals/reported_vintage_reader.py` | PIT/provenance readers | read | diagnostic/legacy | retained | direct diagnostic calls | old investigations/tests | retained as archive readers only |
| `swingmaster/cli/preflight_quarter_update_vintage_readiness.py` | readiness preflight | read | diagnostic/legacy | not in normal flow | direct CLI/UI recovery | old recovery diagnostics | retained, but UI gates cannot apply writes |
| `swingmaster/cli/dry_run_*vintage*`, `diagnose_*vintage*`, `verify_*vintage*` | diagnostics | read/plan | diagnostic/legacy | direct only | direct CLI | old investigations | retained as read-only or low-level-write-rejected tooling |
| migrations | vintage/provenance table creation | schema | migration-only | unchanged | migration runner | existing DB compatibility | retained; no schema/table cleanup in this phase |
| tests/docs with vintage names | old contracts/history | test/docs | test-only/historical | mixed | pytest/direct reading | regression history | active write-contract files retired or replaced by deactivation tests |

## Active Latest-State Consumers

The active fundamentals consumers were inspected for their source tables:

- TTM calculations read `rc_fundamental_quarterly`.
- Score calculations read `rc_fundamental_ttm`.
- Percentile calculations read `rc_fundamental_ttm`.
- Valuation reads `rc_fundamental_ttm` and `rc_fundamental_quarterly`.
- Ticker snapshots read latest quarterly, TTM, score percentile, and valuation tables.
- Current UI update commands launch latest-state quarterly updates and no longer append vintage flags.

No active scoring, valuation, snapshot, or update workflow requires `rc_fundamental_quarterly_vintage` or `rc_fundamental_quarterly_field_provenance`.

## CLI And Config Behavior

- `--write-vintage` in quarter update is `REJECTED_AS_RETIRED`.
- `--write-vintage` in SEC reconstruction is `REJECTED_AS_RETIRED`.
- `--write-vintage` in Yahoo quarterly bridge is `REJECTED_AS_RETIRED`.
- `--write-vintage` in Yahoo fallback enrich is `REJECTED_AS_RETIRED`.
- UI `vintage_options` is `ACCEPTED_NO_OP_FOR_COMPATIBILITY` in the command builder.
- The USA PIT/vintage checkbox is retained as compatibility UI but disabled and cannot enable runtime behavior.
- Legacy vintage readers and preflight/diagnostic commands are `DIAGNOSTIC_ONLY`.

The common disabled status is `VINTAGE_DISABLED`, and explicit write attempts fail with `VINTAGE_PROVENANCE_WRITES_DISABLED`.

## Database Preservation

No table deletion, truncation, schema migration, vacuum, or live DB write is part of this phase. Existing vintage and provenance rows remain intact.

Controlled write verification must use temporary migrated databases under:

```text
temp/vintage_provenance_deactivation/<UTC_TIMESTAMP>/
```

## Verification Summary

Focused regression coverage verifies:

- default quarter update dry-run reports vintage disabled;
- latest quarterly writes still succeed;
- SEC, Yahoo bridge, and Yahoo fallback adapter paths write latest rows only when vintage is requested;
- direct vintage/provenance insert helpers reject writes;
- final mixed, SEC latest-writer, and Yahoo-aware write helpers reject immediately;
- UI command/config paths cannot enable vintage/provenance writes;
- TTM reads latest quarterly data without vintage/provenance rows;
- score, percentile, valuation, and snapshot source modules do not require vintage/provenance tables;
- temporary test databases are created under repository `temp/`.

Controlled verification artifact:

```text
temp/vintage_provenance_deactivation/20260731T142155Z/controlled_verification.json
```

Result:

- temporary migrated database quick check: `ok`;
- latest quarterly rows written: `4`;
- temporary vintage rows: `0` before writes and `0` after writes;
- temporary field-provenance rows: `0` before writes and `0` after writes;
- TTM revenue from latest quarterly rows: `460.0`;
- explicit vintage request rejected with `VINTAGE_PROVENANCE_WRITES_DISABLED`;
- explicit provenance request rejected with `VINTAGE_PROVENANCE_WRITES_DISABLED`;
- counts remained unchanged after rejected write attempts.

## Rollback

Rollback is code-only in this phase because the real tables and rows are preserved. Revert the deactivation commit to restore the previous opt-in vintage/provenance write behavior. If rollback is needed after later physical cleanup, restore an archived database backup before reverting application behavior.

## Future Cleanup Options

- `KEEP_INACTIVE`: keep the tables and rows as an inactive archive.
- `ARCHIVE_THEN_EMPTY`: export/archive the tables, then empty them after explicit approval.
- `EMPTY_IN_PLACE`: empty the existing tables without dropping schema after explicit approval.
- `DROP_IN_LATER_MIGRATION`: remove schema in a later migration after all diagnostics/tests are retired.

Recommendation: `KEEP_INACTIVE` for now. The tables are no longer active dependencies, but they still have rollback and diagnostic value, old tooling still inspects them, and deleting data/schema would increase operational risk for little immediate benefit.
