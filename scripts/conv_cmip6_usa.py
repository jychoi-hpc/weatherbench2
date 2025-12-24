import xarray as xr
import argparse
import os
import glob
from tqdm import tqdm
from dask.diagnostics import ProgressBar
import numpy as np
import pandas as pd

var_dict = {
    "hus": "specific_humidity",
    "ts": "2m_temperature",
    "tas": "2m_temperature",
    "ps": "surface_pressure",
    "ta": "temperature",
    "ua": "u_component_of_wind",
    "va": "v_component_of_wind",
    "t2m": "2m_temperature",
    "u10": "10m_u_component_of_wind",
    "v10": "10m_v_component_of_wind",
}
coord = {
    "lat": "latitude",
    "lon": "longitude",
    "plev": "level",
}
DEFAULT_PRESSURE_LEVELS = [
    200,
    500,
    850,
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset")
    parser.add_argument("--chunk_time", type=int, default=100)
    args = parser.parse_args()

    prefix = f"cmip6_remap/{args.dataset}"
    print("Reading", prefix)

    with ProgressBar():
        ds = xr.open_mfdataset(
            os.path.join(prefix, f"{args.dataset}_ATM.*.nc"),
            combine="by_coords",
            compat="override",
            coords="minimal",
        )

    ds = ds.isel(time=slice(0, -1))
    counts = ds.groupby('time.date').count('time')
    assert (counts == 4).all(), "Not all days have exactly 4 samples"
    
    timedelta = ds.time[0] - np.datetime64(f"{ds.time[0].dt.year.item()}-01-01")
    ds["time"] = ds["time"] - timedelta
    ds = ds.ffill(dim="lon")

    ## Change noleap to leap:
    ds["time"] = ds["time"].astype("datetime64[ns]")
    if (len(ds["time"].sel(time="2012-02-28")) > 0) and (len(ds["time"].sel(time="2012-02-29")) == 0):
        print("Change no-leap to leap calendar")
        time_new = np.array([time_shift(x) for x in ds["time"].values])
        ds = ds.assign_coords(time=time_new)
        
    rename_dict = {old: new for old, new in var_dict.items() if old in ds}
    ds = ds.rename(rename_dict)

    ds = ds.rename(coord)
    ds = ds.assign_coords(level=ds["level"] / 100)
    ds["level"].attrs["standard_name"] = "air_pressure"
    ds["level"].attrs["long_name"] = "pressure"
    ds["level"].attrs["units"] = "hPa"

    ds = ds.sel(level=DEFAULT_PRESSURE_LEVELS)
    ds = ds.sel(time=slice("1980-01-01", None))

    # ds = ds.chunk({"time": args.chunk_time})
    # ds = ds.chunk({"time": args.chunk_time, "level": 13})
    ds = ds.chunk(
        {"time": args.chunk_time, "level": -1, "latitude": -1, "longitude": -1}
    )
    print(ds)

    ## Save as
    year0, year1 = ds.time[0].dt.year.item(), ds.time[-1].dt.year.item() + 1
    nlon = len(ds["longitude"])
    nlat = len(ds["latitude"])
    gridshape = f"{nlon}x{nlat}"
    outfile = f"datasets/cmip6/{args.dataset}-usa-{year0}-{year1}-{gridshape}.zarr"

    if not os.path.exists("datasets/cmip6"):
        os.makedirs("datasets/cmip6", exist_ok=True)

    with ProgressBar():
        print(f"Saving {outfile} ...")
        ds.to_zarr(outfile, consolidated=True, mode="w")
