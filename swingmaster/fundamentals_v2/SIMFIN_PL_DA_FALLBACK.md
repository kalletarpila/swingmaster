# SimFin PL Depreciation & Amortization Fallback

The canonical statement API mapper may use SimFin `PL:Depreciation & Amortization`
as a fallback for canonical `depreciation_amortization` only when the cashflow
statement value is absent for the same company, fiscal year, fiscal period, and
report date.

`CF:Depreciation & Amortization` remains authoritative. If the CF value exists,
the PL fallback is ignored.

Fallback eligibility is intentionally narrow:

- statement endpoint payload is compact quarterly data only (`q1,q2,q3,q4`)
- target CF depreciation/amortization is missing
- target PL depreciation/amortization exists and is non-positive
- target PL operating income exists
- at least four exact historical PL/CF overlap rows exist for the same SimFin id,
  fiscal year, fiscal period, and report date
- every overlap has abs-normalized relative difference at or below 1%:
  `abs(abs(PL)-abs(CF))/max(abs(PL),abs(CF),1) <= 0.01`

When eligible, the mapper stores `abs(PL:Depreciation & Amortization)`.
Provenance records the value as `SIMFIN_API_DERIVED` with transformation
`validated_abs_pl_da_fallback`. Derived EBITDA provenance is also distinct:
`operating_income + validated_abs_pl_depreciation_amortization`.

This is not a broad PL-to-CF mapping and does not backfill historical production
rows by itself. Existing differing non-null canonical values remain conflicts
and are not overwritten.
