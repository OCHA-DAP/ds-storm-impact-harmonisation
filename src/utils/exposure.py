import geopandas as gpd
import pandas as pd
import xarray as xr
from rioxarray.exceptions import NoDataInBounds
from tqdm import tqdm

GEO_CRS_ANTIMERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=180"


def calculate_single_adm_exposure(
    gdf_buffers: gpd.GeoDataFrame, da_wp: xr.DataArray
) -> pd.DataFrame:
    records = []
    for _, row in gdf_buffers.iterrows():
        row_data = row.drop(labels="geometry").to_dict()

        if not row.geometry or row.geometry.is_empty:
            pop_exposed = 0
        else:
            if row.geometry.bounds[0] < -160 or row.geometry.bounds[2] > 160:
                # if the geometry crosses the antimeridian, we need to use a different CRS to ensure correct clipping
                row_geometry_work = gpd.GeoSeries([row.geometry], crs=4326).to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
            else:
                row_geometry_work = row.geometry
            try:
                da_wp_clip_buffer = da_wp.rio.clip([row_geometry_work])
                pop_exposed = int(da_wp_clip_buffer.where(da_wp_clip_buffer > 0).sum())
            except NoDataInBounds:
                pop_exposed = 0

        row_data["pop_exposed"] = pop_exposed
        records.append(row_data)

    return pd.DataFrame(records)


def calculate_multi_adm_exposure(
    gdf_buffers: gpd.GeoDataFrame,
    da_wp: xr.DataArray,
    gdf_adm: gpd.GeoDataFrame,
    adm_index: str = "ADM3_PCODE",
    disable_tqdm: bool = True,
) -> pd.DataFrame:
    # ensure correct CRS
    gdf_buffers = gdf_buffers.to_crs(4326)
    gdf_adm = gdf_adm.to_crs(4326)
    da_wp = da_wp.assign_coords({"x": ((da_wp.x + 360) % 360)}).sortby("x")

    dfs = []
    for _, adm_row in tqdm(
        gdf_adm.iterrows(), total=len(gdf_adm), disable=disable_tqdm
    ):
        # note that we have to set all_touched=True here to ensure that
        # all possible pixels are grabbed and the sum over all the
        # buffers is correct (all_touched is then False in the admin
        # aggregation to avoid double counting)
        da_wp_adm = da_wp.rio.clip([adm_row.geometry], all_touched=True)
        _df_exp = calculate_single_adm_exposure(gdf_buffers, da_wp_adm)
        _df_exp[adm_index] = adm_row[adm_index]
        dfs.append(_df_exp)
    df_exp = pd.concat(dfs, ignore_index=True)
    return df_exp
