import matplotlib.pyplot as plt
import xarray as xr
import pandas as pd
import numpy as np
from dask.diagnostics import ProgressBar

p1 = "/lustre/orion/cli138/world-shared/moet/DaymetV4/prcp/DaymetV4_VIC4_prcp_%{year}.nc"
p2 = "/lustre/orion/cli138/world-shared/moet/DaymetV4/tmax/DaymetV4_VIC4_tmax_%{year}.nc"
p3 = "/lustre/orion/cli138/world-shared/moet/DaymetV4/tmin/DaymetV4_VIC4_tmin_%{year}.nc"
year_list = list(range(1980, 2023))

xa_list = list()
for year in year_list:
    for p in [p1, p2, p3]:
        filename = p.replace("%{year}", str(year))
        x = xr.open_dataset(filename, chunks={"time": 10})
        time_range = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31", freq="D")
        time_range = time_range[:365]
        x = x.assign_coords(time=time_range)
        xa_list.append(x)

xa = xr.combine_by_coords(xa_list, combine_attrs="drop")
xa = xa.rename({"lat": "latitude", "lon": "longitude"})

mk = xa["prcp"][0,:,:].isnull().compute()
mk = (~mk).astype(np.float32)
xa["land_sea_mask"] = mk

with ProgressBar():
    mn = xa.min(skipna=True).compute()
    xa = xa.fillna(mn)
    # mx = xa.max(skipna=True).compute()
    # xa = xa.fillna(mn - (mx-mn)/2)

with ProgressBar():
    xa = xa.chunk({"time": 10})
    print(xa)
    xa.to_zarr("datasets/daymet/daymet_1980-2023-1d-1405x697.zarr", mode="w")
