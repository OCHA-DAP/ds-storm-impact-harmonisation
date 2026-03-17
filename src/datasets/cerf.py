"""Loading and wrangling CERF storm allocation data."""

import xml.etree.ElementTree as ET
from io import BytesIO

import numpy as np
import ocha_stratus as stratus
import pandas as pd
import requests

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


CERF_API_URL = "https://cerfgms-webapi.unocha.org/v1/application/All.xml"

# Authoritative ApplicationCode -> IBTrACS sid mapping.
# Built from CERF API + IBTrACS DB + old CSV + manual research.
# None = allocation is not a tropical cyclone event.
CERFCODE_TO_SID: dict[str, str | None] = {
    # 2007
    "07-RR-BGD-297": "2007133N15091",      # Bangladesh - AKASH
    "07-RR-BGD-323": "2007314N10093",      # Bangladesh - SIDR
    "07-RR-DOM-13362": "2007345N18298",    # Dominican Republic - OLGA
    "07-RR-DOM-4780": "2007297N18300",     # Dominican Republic - NOEL
    "07-RR-GHA-5535": None,                # Ghana (non-TC, inland flooding)
    "07-RR-MDG-13072": "2007066S12066",    # Madagascar - INDLALA
    "07-RR-MDG-6860": "2006364S12058",     # Madagascar - CLOVIS
    "07-RR-MLI-7117": None,                # Mali (non-TC, inland flooding)
    "07-RR-MOZ-7639": "2007043S11071",     # Mozambique - FAVIO
    "07-RR-NIC-8323": "2007244N12303",     # Nicaragua - FELIX
    "07-RR-PHL-9602": "2006329N06150",     # Philippines - DURIAN
    "07-RR-RWA-10462": None,               # Rwanda (non-TC)
    "07-RR-SDN-10078": None,               # Sudan (non-TC)
    "07-RR-SDN-13738": None,               # Sudan (non-TC)
    "07-RR-UGA-11920": None,               # Uganda (non-TC)
    # 2008
    "08-RR-HTI-5820": "2008245N17323",     # Haiti - IKE
    "08-RR-HTI-5831": "2008245N17323",     # Haiti - IKE
    "08-RR-HTI-5837": "2008245N17323",     # Haiti - IKE
    "08-RR-MMR-13525": "2008117N11090",    # Myanmar - NARGIS
    "08-RR-MMR-7768": "2008117N11090",     # Myanmar - NARGIS
    "08-RR-MMR-7787": "2008117N11090",     # Myanmar - NARGIS
    "08-RR-MOZ-7661": "2008062S10064",     # Mozambique - JOKWE
    # 2009
    "09-RR-LAO-6930": "2009268N14128",     # Lao PDR - KETSANA
    "09-RR-NIC-8334": "2009308N11279",     # Nicaragua - IDA
    "09-RR-SLV-4818": "2009308N11279",     # El Salvador - IDA
    # 2010
    "10-RR-BOL-453": None,                 # Bolivia (non-TC, landlocked)
    "10-RR-COL-2904": None,                # Colombia (non-TC, La Nina flooding)
    "10-RR-GTM-5597": "2010149N13266",     # Guatemala - AGATHA
    "10-RR-MMR-7858": "2010293N17093",     # Myanmar - GIRI
    # 2011
    "11-RR-GTM-5608": "2011280N10268",     # Guatemala
    "11-RR-ZWE-12668": None,               # Zimbabwe (non-TC, inland flooding)
    # 2012
    "12-RR-CUB-4468": "2012296N14283",     # Cuba - SANDY
    "12-RR-HTI-5933": "2012296N14283",     # Haiti - SANDY
    # 2013
    "13-RR-PHL-9970": "2013306N07162",     # Philippines - HAIYAN
    # 2015
    "15-RR-PHL-17750": "2015285N14151",    # Philippines - KOPPU
    "15-RR-VUT-14293": "2015066S08170",    # Vanuatu - PAM
    # 2016
    "16-RR-CUB-22839": "2016273N13300",    # Cuba - MATTHEW
    "16-RR-FJI-18935": "2016041S14170",    # Fiji - WINSTON
    "16-RR-HTI-22873": "2016273N13300",    # Haiti - MATTHEW
    "16-RR-HTI-23486": "2016273N13300",    # Haiti - MATTHEW
    # 2017
    "17-RR-ATG-27500": "2017242N16333",    # Antigua and Barbuda - IRMA
    "17-RR-BGD-26267": "2017147N14087",    # Bangladesh - MORA
    "17-RR-CUB-27383": "2017242N16333",    # Cuba - IRMA
    "17-RR-DMA-27733": "2017260N12310",    # Dominica - MARIA
    "17-RR-MDG-25219": "2017061S11063",    # Madagascar - ENAWO
    "17-RR-MMR-26516": "2017147N14087",    # Myanmar - MORA
    "17-RR-MOZ-24650": "2017043S19040",    # Mozambique - DINEO
    "17-RR-VNM-28329": "2017304N11127",    # Viet Nam - DAMREY
    # 2018
    "18-RR-DJI-30969": "2018136N11054",    # Djibouti - SAGAR
    # 2019
    "19-RR-BHS-38922": "2019236N10314",    # Bahamas - DORIAN
    "19-RR-COM-37104": "2019112S10053",    # Comoros - KENNETH
    "19-RR-CUB-34583": None,               # Cuba (non-TC, tornado)
    "19-RR-MOZ-35492": "2019063S18038",    # Mozambique - IDAI
    "19-RR-MOZ-37184": "2019112S10053",    # Mozambique - KENNETH
    "19-RR-MWI-35650": "2019063S18038",    # Malawi - IDAI
    "19-RR-PRK-39293": "2019243N06136",    # DPR Korea - LINGLING
    "19-RR-ZWE-35840": "2019063S18038",    # Zimbabwe - IDAI
    # 2020
    "20-RR-BGD-43537": "2020136N10088",    # Bangladesh - AMPHAN
    "20-RR-FJI-42874": "2020092S09155",    # Fiji - HAROLD
    "20-RR-GTM-46209": "2020306N15288",    # Guatemala - ETA
    "20-RR-HND-45959": "2020306N15288",    # Honduras - ETA
    "20-RR-NIC-46275": "2020318N16289",    # Nicaragua - IOTA
    "20-RR-PAK-41273": None,               # Pakistan (non-TC, winter emergency)
    "20-RR-PHL-45955": "2020299N11144",    # Philippines - GONI
    "20-RR-SLV-43848": "2020152N12269",    # El Salvador - AMANDA
    "20-RR-VUT-42734": "2020092S09155",    # Vanuatu - HAROLD
    # 2021
    "21-RR-FJI-46848": "2020346S13168",    # Fiji - YASA
    "21-RR-PHL-50868": "2021346N05145",    # Philippines - RAI
    # 2022
    "22-RR-CUB-55712": "2022266N12294",    # Cuba - IAN
    "22-RR-MDG-51622": "2022037S10103",    # Madagascar - EMNATI
    "22-RR-MOZ-52564": "2022065S16055",    # Mozambique - GOMBE
    # 2023
    "23-RR-BGD-59459": "2023129N08091",    # Bangladesh - MOCHA
    "23-RR-MMR-59095": "2023129N08091",    # Myanmar - MOCHA
    "23-RR-MOZ-57965": "2023036S12117",    # Mozambique - FREDDY
    "23-RR-MWI-58010": "2023036S12117",    # Malawi - FREDDY
    "23-RR-VUT-58018": "2023055S14184",    # Vanuatu - JUDY
    "23-RR-VUT-61859": "2023292S03172",    # Vanuatu - LOLA
    # 2024
    "24-RR-BGD-63521": "2024145N14087",    # Bangladesh - REMAL
    "24-RR-MDG-64484": "2024084S12054",    # Madagascar - GAMANE
    "CERF-CUB-24-RR-1430": "2024293N21294",  # Cuba - OSCAR
    "CERF-CUB-24-RR-1432": "2024309N13283",  # Cuba - RAFAEL
    "CERF-GRD-24-RR-1393": "2024181N09320",  # Grenada - BERYL
    "CERF-JAM-24-RR-1392": "2024181N09320",  # Jamaica - BERYL
    "CERF-MMR-24-RR-1423": "2024244N09137",  # Myanmar - YAGI
    "CERF-PHL-24-RR-1391": "2024293N13141",  # Philippines - TRAMI
    "CERF-PHL-24-RR-1431": "2024293N13141",  # Philippines - TRAMI
    # 2025
    "CERF-CUB-25-RR-1478": "2025291N11319",  # Cuba - MELISSA
    "CERF-CUB-25-RR-1495": "2025291N11319",  # Cuba - MELISSA
    "CERF-HTI-24-RR-1399": "2025291N11319",  # Haiti - MELISSA
    "CERF-JAM-25-RR-1494": "2025291N11319",  # Jamaica - MELISSA
    "CERF-MOZ-24-RR-1438": "2024345S11062",  # Mozambique - CHIDO
    "CERF-MOZ-24-RR-1440": "2024345S11062",  # Mozambique - CHIDO
    "CERF-PHL-25-RR-1485": "2025308N10143",  # Philippines - FUNG-WONG
    # 2026
    "CERF-MDG-24-RR-1434": "2026030S16043",  # Madagascar - FYTIA
    "CERF-MDG-26-RR-1518": "2026030S16043",  # Madagascar - FYTIA
    "CERF-MOZ-26-RR-1513": "2025068S15046",  # Mozambique - JUDE
}


_3RM_BLOB = (
    "ds-storm-impact-harmonisation/raw/"
    "CERF 3RM - RR Regression Model - version 1.8.xlsx"
)


def load_3rm_cirv(
    blob_name: str = _3RM_BLOB,
    sheet: str = "MergedData",
) -> pd.DataFrame:
    """Load CIRV scores from the 3RM dataset on blob storage.

    Returns a DataFrame with columns: ApplicationCode, iso3, Year, CIRV.
    Joins are intended on ApplicationCode (matching CERFCODE_TO_SID keys).
    """
    raw = stratus.load_blob_data(blob_name)
    df = pd.read_excel(BytesIO(raw), sheet_name=sheet)
    df = df.rename(columns={"Application Code": "ApplicationCode"})
    keep = ["ApplicationCode", "Country", "Year", "CIRV"]
    return df[keep].copy()


def load_cerf_api_data(
    emergency_type: str = "Storm",
    window: str = "Rapid Response",
) -> pd.DataFrame:
    """Load CERF allocations from the CERF GMS API.

    Returns a cleaned DataFrame with one row per allocation, filtered to
    the given emergency type and funding window.

    Columns returned: ApplicationCode, CountryCode, CountryName, Year,
    TotalAmountApproved, FirstProjectApprovedDate, ApplicationTitle,
    CN_Summary.
    """
    response = requests.get(CERF_API_URL, timeout=60)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    records = [
        {
            child.tag: (
                None
                if child.get(
                    "{http://www.w3.org/2001/XMLSchema-instance}nil"
                )
                == "true"
                else child.text
            )
            for child in app
        }
        for app in root
    ]
    df = pd.DataFrame(records)

    # Filter
    df = df[
        (df["EmergencyTypeName"] == emergency_type)
        & (df["WindowFullName"] == window)
    ].copy()

    # Clean types
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["TotalAmountApproved"] = pd.to_numeric(
        df["TotalAmountApproved"], errors="coerce"
    )
    df["FirstProjectApprovedDate"] = pd.to_datetime(
        df["FirstProjectApprovedDate"], errors="coerce"
    )

    keep = [
        "ApplicationCode",
        "CountryCode",
        "CountryName",
        "Year",
        "TotalAmountApproved",
        "FirstProjectApprovedDate",
        "ApplicationTitle",
        "CN_Summary",
    ]
    return df[keep].reset_index(drop=True)


def apply_cerfcode_sids(
    df_cerf_api: pd.DataFrame,
    cerfcode_to_sid: dict[str, str | None] | None = None,
) -> pd.DataFrame:
    """Apply the ApplicationCode -> sid mapping to API-sourced CERF data.

    Drops rows where the mapping is None (non-TC events).
    Returns DataFrame with sid, iso3, Country, allocation_year, and amount
    columns matching the format expected by build_analysis_dataset_api.
    """
    if cerfcode_to_sid is None:
        cerfcode_to_sid = CERFCODE_TO_SID

    df = df_cerf_api.copy()
    df["sid"] = df["ApplicationCode"].map(cerfcode_to_sid)

    # Rows in dict with None are non-TC — drop them
    # Rows not in dict at all are unmatched — keep as NaN for reporting
    non_tc_codes = {k for k, v in cerfcode_to_sid.items() if v is None}
    df = df[~df["ApplicationCode"].isin(non_tc_codes)]

    # Standardise column names for downstream compatibility
    df = df.rename(columns={
        "CountryCode": "iso3",
        "CountryName": "Country",
        "TotalAmountApproved": "Amount in US$",
        "Year": "allocation_year",
    })

    return df


def build_analysis_dataset_api(
    df_ocha: pd.DataFrame,
    cerfcode_to_sid: dict[str, str | None] | None = None,
) -> pd.DataFrame:
    """Build analysis dataset using CERF API data + cerfcode->sid mapping.

    Loads CERF data from the API, applies sid mapping, merges with OCHA
    exposure, and aggregates. Returns the same format as
    build_analysis_dataset().
    """
    df_cerf_api = load_cerf_api_data()
    df_matched = apply_cerfcode_sids(df_cerf_api, cerfcode_to_sid)

    ocha_wide = pivot_ocha_wide(df_ocha)

    cerf_valid = df_matched.dropna(subset=["sid"])
    merged = cerf_valid.merge(ocha_wide, on=["sid", "iso3"], how="left")

    exp_cols = ["pop_exp_34kt", "pop_exp_50kt", "pop_exp_64kt"]
    has_exp = merged.dropna(subset=exp_cols, how="all")

    merged_agg = has_exp.groupby(["sid", "iso3", "Country"]).agg(
        total_usd=("Amount in US$", "sum"),
        n_allocations=("Amount in US$", "size"),
        allocation_year=("allocation_year", "first"),
        pop_exp_34kt=("pop_exp_34kt", "first"),
        pop_exp_50kt=("pop_exp_50kt", "first"),
        pop_exp_64kt=("pop_exp_64kt", "first"),
    ).reset_index()

    return merged_agg


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
