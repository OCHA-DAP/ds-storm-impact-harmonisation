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

# Total pop calc

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
```

```python
url = "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip"
adm0 = gpd.read_file(url)
```

```python
blob_name = "worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif"
da_wp_global = stratus.open_blob_cog(blob_name, container_name="raster")
```

```python
da_wp_global = da_wp_global.squeeze(drop=True)
```

```python
adm0[adm0["ADM0_A3"] == "FJI"].plot()
```

```python
da_wp_adm = da_wp_global.rio.clip(adm0[adm0["ADM0_A3"] == "FJI"].geometry).compute()
```

```python
da_wp_adm.where(da_wp_adm > 0).sum()
```

```python
dicts = []

for iso3, row in tqdm(adm0.set_index("ADM0_A3").iterrows(), total=len(adm0)):
    _da_wp_adm = da_wp_global.rio.clip([row.geometry]).compute()
    try:
        pop = int(_da_wp_adm.where(_da_wp_adm > 0).sum())
    except Exception as e:
        print(iso3)
        print(e)
        pop = 0
    dicts.append({"ADM0_A3": iso3, "total_pop": pop})
        
```

```python

```
