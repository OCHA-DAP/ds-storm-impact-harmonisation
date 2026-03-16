"""Loading and wrangling CERF storm allocation data."""

import numpy as np
import pandas as pd

ISO2_TO_ISO3 = {
    "AG": "ATG", "BD": "BGD", "BO": "BOL", "BS": "BHS", "CO": "COL",
    "CU": "CUB", "DJ": "DJI", "DM": "DMA", "DO": "DOM", "FJ": "FJI",
    "GH": "GHA", "GT": "GTM", "HN": "HND", "HT": "HTI", "KM": "COM",
    "KP": "PRK", "LA": "LAO", "MG": "MDG", "ML": "MLI", "MM": "MMR",
    "MW": "MWI", "MZ": "MOZ", "NI": "NIC", "PH": "PHL", "PK": "PAK",
    "RW": "RWA", "SD": "SDN", "SV": "SLV", "UG": "UGA", "VN": "VNM",
    "VU": "VUT", "ZW": "ZWE",
}

# Known matches where auto-assignment picks the wrong storm or the storm
# is absent from the OCHA exposure data.  Each key is (iso2, allocation_date).
DEFAULT_MANUAL_OVERRIDES: dict[tuple[str, str], str] = {
    ("BD", "2023-06-14"): "2023129N08091",   # Cyclone MOCHA
    ("MZ", "2023-04-05"): "2023036S12117",   # Cyclone FREDDY
    ("MG", "2023-04-19"): "2023036S12117",   # Cyclone FREDDY
    ("MW", "2023-03-30"): "2023036S12117",   # Cyclone FREDDY
    ("MW", "2023-12-14"): "2023036S12117",   # Cyclone FREDDY follow-on
    ("MW", "2019-04-04"): "2019063S18038",   # Cyclone IDAI
    ("VU", "2023-11-27"): "2023292S03172",   # Cyclone LOLA
}


def lookup_candidate_sids(
    df_exposure: pd.DataFrame,
    iso3: str,
    allocation_date: str,
    lookback_days: int = 365,
    speed: int = 34,
) -> pd.DataFrame:
    """Find candidate storm sids from exposure data for a given country and date.

    Searches for storms that affected ``iso3`` with wind speeds at the given
    ``speed`` threshold, within ``lookback_days`` before ``allocation_date``.

    Parameters
    ----------
    df_exposure
        OCHA exposure dataframe (columns: speed, sid, pop_exposed, ADM0_A3).
    iso3
        ISO3 country code.
    allocation_date
        CERF allocation date string (YYYY-MM-DD).
    lookback_days
        How many days before the allocation to search.
    speed
        Wind speed threshold to filter on (default 34 kt).

    Returns
    -------
    pd.DataFrame
        Candidate storms sorted by pop_exposed descending.
    """
    alloc_dt = pd.Timestamp(allocation_date)
    candidates = df_exposure[
        (df_exposure["ADM0_A3"] == iso3) & (df_exposure["speed"] == speed)
    ].copy()

    # Derive approximate storm date from sid (first 7 chars = YYYYDDD)
    candidates["storm_date"] = pd.to_datetime(
        candidates["sid"].str[:7], format="%Y%j", errors="coerce"
    )

    mask = (
        (candidates["storm_date"] <= alloc_dt)
        & (candidates["storm_date"] >= alloc_dt - pd.Timedelta(days=lookback_days))
    )
    return (
        candidates[mask]
        .sort_values("pop_exposed", ascending=False)
        .reset_index(drop=True)
    )


def assign_sids(
    df_cerf: pd.DataFrame,
    df_exposure: pd.DataFrame,
    manual_overrides: dict[tuple[str, str], str] | None = None,
    lookback_days: int = 365,
) -> pd.DataFrame:
    """Fill missing sids in CERF data using exposure-based lookup.

    For each CERF row missing a sid, finds candidate storms from the
    exposure data and assigns the one with the highest population exposure
    (most likely match). Manual overrides take precedence.

    Parameters
    ----------
    df_cerf
        CERF dataframe with columns: iso2, sid, Allocation date.
    df_exposure
        OCHA exposure dataframe.
    manual_overrides
        Dict mapping (iso2, allocation_date) to sid for known matches.
    lookback_days
        How far back to search for storms.

    Returns
    -------
    pd.DataFrame
        Copy of df_cerf with sid column filled where possible.
        A ``sid_source`` column indicates how the sid was assigned:
        "original", "manual", "auto", or NaN if still missing.
    """
    if manual_overrides is None:
        manual_overrides = {}

    df = df_cerf.copy()
    df["iso3"] = df["iso2"].map(ISO2_TO_ISO3)
    df["sid_source"] = "original"
    df.loc[df["sid"].isna(), "sid_source"] = pd.NA

    for idx, row in df[df["sid"].isna()].iterrows():
        key = (row["iso2"], row["Allocation date"])
        if key in manual_overrides:
            df.at[idx, "sid"] = manual_overrides[key]
            df.at[idx, "sid_source"] = "manual"
            continue

        candidates = lookup_candidate_sids(
            df_exposure,
            iso3=row["iso3"],
            allocation_date=row["Allocation date"],
            lookback_days=lookback_days,
        )
        if len(candidates) > 0:
            df.at[idx, "sid"] = candidates.iloc[0]["sid"]
            df.at[idx, "sid_source"] = "auto"

    return df


def pivot_ocha_wide(df_ocha: pd.DataFrame) -> pd.DataFrame:
    """Pivot long-format OCHA exposure to wide (one row per sid x country)."""
    wide = df_ocha.pivot_table(
        index=["sid", "ADM0_A3"], columns="speed", values="pop_exposed"
    ).reset_index()
    wide.columns = ["sid", "iso3", "pop_exp_34kt", "pop_exp_50kt", "pop_exp_64kt"]
    return wide


def build_analysis_dataset(
    df_cerf: pd.DataFrame,
    df_ocha: pd.DataFrame,
    manual_overrides: dict[tuple[str, str], str] | None = None,
) -> pd.DataFrame:
    """Run the full pipeline: match sids, pivot exposure, merge, aggregate.

    Returns the storm-country-level analysis dataset with columns:
    sid, iso3, Country, total_usd, n_allocations, allocation_year,
    sid_source, pop_exp_34kt, pop_exp_50kt, pop_exp_64kt.
    """
    if manual_overrides is None:
        manual_overrides = DEFAULT_MANUAL_OVERRIDES

    df_matched = assign_sids(df_cerf, df_ocha, manual_overrides)
    ocha_wide = pivot_ocha_wide(df_ocha)

    cerf_valid = df_matched.dropna(subset=["sid"])
    merged = cerf_valid.merge(ocha_wide, on=["sid", "iso3"], how="left")

    exp_cols = ["pop_exp_34kt", "pop_exp_50kt", "pop_exp_64kt"]
    has_exp = merged.dropna(subset=exp_cols, how="all")

    merged_agg = has_exp.groupby(["sid", "iso3", "Country"]).agg(
        total_usd=("Amount in US$", "sum"),
        n_allocations=("Amount in US$", "size"),
        allocation_year=("allocation_year", "first"),
        sid_source=("sid_source", "first"),
        pop_exp_34kt=("pop_exp_34kt", "first"),
        pop_exp_50kt=("pop_exp_50kt", "first"),
        pop_exp_64kt=("pop_exp_64kt", "first"),
    ).reset_index()

    return merged_agg


def remove_col_outliers_iqr(
    df: pd.DataFrame, col: str, factor: float = 2.0
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Remove IQR outliers for a single column on log10 scale.

    Filters to rows where *col* > 0 and total_usd > 0, then applies
    the ``factor`` x IQR rule on log10(*col*).

    Returns ``(clean_df, outlier_df)``.
    """
    subset = df.dropna(subset=[col])
    subset = subset[(subset[col] > 0) & (subset["total_usd"] > 0)]
    log_vals = np.log10(subset[col])
    q1, q3 = log_vals.quantile(0.25), log_vals.quantile(0.75)
    iqr = q3 - q1
    lower, upper = q1 - factor * iqr, q3 + factor * iqr
    mask = (log_vals >= lower) & (log_vals <= upper)
    return subset[mask], subset[~mask]
