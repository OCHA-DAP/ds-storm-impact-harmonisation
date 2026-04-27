# CERF-Exposure Merge: Findings

## Key Problem: Minimal Temporal Overlap

- **CERF** storm allocations span 2006–2023 (82 rows, 44 unique sids, 24 rows have null sids)
- **ADAM** exposure covers only 2023–2025 (47 sids) — zero sid overlap with CERF
- **GDACS** exposure has rows for 2015–2025 (177 sids) but actual population values only exist for 2015, 2022–2025 (60 sids with data). 2016–2021 rows have NaN exposure.

**Result: Only 1 CERF row has any usable exposure data** (Cuba, 2022, Hurricane Ian, sid `2022266N12294`, pop_64kt=655,778).

## Exposure Column Semantics

### ADAM (binned, NOT cumulative)
- `pop_60kmh` = population exposed to 60–90 km/h winds (bin only)
- `pop_90kmh` = population exposed to 90–120 km/h winds (bin only)
- `pop_120kmh` = population exposed to >= 120 km/h winds

**To convert to cumulative:**
- `pop_gte_60kmh = pop_60kmh + pop_90kmh + pop_120kmh`
- `pop_gte_90kmh = pop_90kmh + pop_120kmh`
- `pop_gte_120kmh = pop_120kmh` (already cumulative)

### GDACS (cumulative)
- `pop_34kt` = all population exposed to >= 34 kt winds
- `pop_64kt` = all population exposed to >= 64 kt winds

### Cross-validation
After converting ADAM to cumulative, ADAM `pop_gte_60kmh` (~32 kt) aligns closely with GDACS `pop_gte_34kt`, and ADAM `pop_gte_120kmh` (~65 kt) aligns with GDACS `pop_gte_64kt`.

## Join Keys
- CERF uses `iso2`, exposure datasets use `iso3` — need mapping
- Join on `sid` + `iso3` (after iso2→iso3 conversion)
- Some CERF storms have multiple allocations per country (e.g., Haiti 2016, Myanmar 2008)

## Next Steps
- The exposure datasets need to be expanded historically to cover the CERF period (2006–2023)
- OR the CERF data needs to include more recent storms (2022+)
- Without more overlap, building a predictive model is not feasible with current data
