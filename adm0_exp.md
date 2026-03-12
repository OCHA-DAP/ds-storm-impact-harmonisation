---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.19.1
  kernelspec:
    display_name: ds-storm-impact-harmonisation
    language: python
    name: ds-storm-impact-harmonisation
---

# USA radii exposure

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import ocha_stratus as stratus
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

from tqdm import tqdm
from src.utils.exposure import calculate_single_adm_exposure
```

```python
blob_name = "ds-storm-impact-harmonisation/processed/ibtracs_usa_buffers.parquet"
```

```python
gdf_buffers = stratus.load_geoparquet_from_blob(blob_name)
```

```python
url = "https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.parquet"

adm0 = gpd.read_parquet(f"simplecache::{url}")
```

```python
url = "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip"
adm0 = gpd.read_file(url)
```

```python
fig, ax = plt.subplots(dpi=1000)
adm0.plot(ax=ax)
```

```python
iso3 = "hti"
adm0[adm0["ISO_A3"] == iso3.upper()].plot()
```

```python
blob_name = "worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif"
da_wp_global = stratus.open_blob_cog(blob_name, container_name="raster")
```

```python
da_wp_global = da_wp_global.squeeze(drop=True)
```

```python
da_wp_global
```

```python
adm0_plot = adm0[adm0["ISO_A3"] == iso3.upper()]
minx, miny, maxx, maxy = adm0_plot.total_bounds
```

```python
da_clip = da_wp_global.sel(
    x=slice(minx, maxx), y=slice(maxy, miny)  # y reversed if descending
)
```

```python
fig, ax = plt.subplots(dpi=300)
adm0_plot.boundary.plot(ax=ax, linewidth=0.5)
da_clip.where(da_clip > 0).plot(ax=ax)
```

```python
GEO_CRS_MERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=0"
GEO_CRS_ANTIMERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=180"
```

```python
adm0_plot = adm0[adm0["ADM0_A3"] == "FJI"]
# adm0_plot = adm0_plot.to_crs(GEO_CRS_ANTIMERIDIAN)
```

```python
adm0_plot.total_bounds
```

```python
adm0.columns
```

```python
adm0.groupby("ADM0_A3").size().max()
```

```python
import warnings
from rasterio.errors import ShapeSkipWarning

warnings.filterwarnings("ignore", category=ShapeSkipWarning)
```

```python
adm0_group.to_crs(GEO_CRS_ANTIMERIDIAN).plot()
```

```python
existing_blobs = stratus.list_container_blobs(
    name_starts_with="ds-storm-impact-harmonisation/processed/adm0_ibtracs_exp/"
)
```

```python
existing_blobs
```

```python
adm_index_col = "ADM0_A3"

da_wp_wrapped = da_wp_global.assign_coords(
    {"x": ((da_wp_global.x + 360) % 360)}
).sortby("x")

gdf_buffers_antimeridian = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)

for adm_index, adm0_group in tqdm(adm0.groupby(adm_index_col)):
    blob_name = f"ds-storm-impact-harmonisation/processed/adm0_ibtracs_exp/{adm_index.lower()}_exp.parquet"

    adm_row = adm0_group.iloc[0]
    adm_name = adm_row["NAME"]
    if blob_name in existing_blobs:
        print(f"already done for {adm_name}")
        continue

    minx, miny, maxx, maxy = adm0_group.total_bounds
    wrap_antimeridian = maxx > 160 or minx < -160

    if wrap_antimeridian:
        da_wp_work = da_wp_wrapped
        adm0_work = adm0_group.to_crs(GEO_CRS_ANTIMERIDIAN)
        gdf_buffers_work = gdf_buffers_antimeridian
    else:
        da_wp_work = da_wp_global
        adm0_work = adm0_group
        gdf_buffers_work = gdf_buffers

    buffers_in_country = gdf_buffers_work[
        gdf_buffers_work.intersects(adm0_work.iloc[0].geometry)
    ]

    if buffers_in_country.empty:
        print(f"no buffer overlap with {adm_name}")
        continue

    da_wp_adm = da_wp_work.rio.clip(adm0_work.geometry, all_touched=True)

    _df_exp = calculate_single_adm_exposure(buffers_in_country, da_wp_adm)
    _df_exp[adm_index_col] = adm_index

    stratus.upload_parquet_to_blob(_df_exp, blob_name)

    del da_wp_adm
```

```python
existing_blobs = stratus.list_container_blobs(
    name_starts_with="ds-storm-impact-harmonisation/processed/adm0_ibtracs_exp/"
)
```

```python
dfs = []
for blob_name in tqdm(existing_blobs):
    _df_in = stratus.load_parquet_from_blob(blob_name)
    _df_in = _df_in[_df_in["pop_exposed"] > 0]
    if not _df_in.empty:
        dfs.append(_df_in)
```

```python
df_exp_all = pd.concat(dfs, ignore_index=True)
```

```python
for speed, group in df_exp_all.groupby("speed"):
    fig, ax = plt.subplots()
    group.groupby("ADM0_A3")["pop_exposed"].max().sort_values(ascending=True).iloc[
        -20:
    ].plot.bar(ax=ax)
    ax.set_title(speed)
```

```python
df_exp_sid_sum = df_exp_all.groupby(["sid", "speed"])["pop_exposed"].sum().reset_index()
```

```python
df_exp_sid_sum.sort_values("pop_exposed", ascending=False).iloc[:20]
```

```python
df_exp_sid_sum[df_exp_sid_sum["speed"] == 50].sort_values(
    "pop_exposed", ascending=False
).iloc[:20]
```

```python
df_exp_sid_sum[df_exp_sid_sum["speed"] == 64].sort_values(
    "pop_exposed", ascending=False
).iloc[:20]
```

```python
df_exp_all["sid"].nunique()
```

```python
df_exp_all.groupby(["sid", "ADM0_A3"]).size()
```

```python
df_exp_all
```

```python
blob_name = "ds-storm-impact-harmonisation/processed/adm0_ibtracs_exp_all.parquet"
```

```python
stratus.upload_parquet_to_blob(df_exp_all, blob_name)
```

```python
df_exp_all["sid"].min()
```
