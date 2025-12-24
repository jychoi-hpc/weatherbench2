import xarray as xr
import argparse
import os
import glob
from tqdm import tqdm
from dask.diagnostics import ProgressBar
import numpy as np

var_dict = {
    "q": "specific_humidity",
    "t": "temperature",
    "u": "u_component_of_wind",
    "v": "v_component_of_wind",
    "t2m": "2m_temperature",
    "u10": "10m_u_component_of_wind",
    "v10": "10m_v_component_of_wind",
    "z": "geopotential",
    ## cmip6
    "hus": "specific_humidity",
    "ua": "u_component_of_wind",
    "va": "v_component_of_wind",
    "ta": "temperature",
    "zg": "geopotential",
    "tas": "sea_surface_temperature",
    "uas": "10m_u_component_of_wind",
    "vas": "10m_v_component_of_wind",
}
coord = {
    "lat": "latitude",
    "lon": "longitude",
}
DEFAULT_PRESSURE_LEVELS = [
    # 50,
    # 100,
    # 150,
    200,
    # 250,
    # 300,
    # 400,
    500,
    # 600,
    # 700,
    850,
    # 925,
    # 1000,
]


def time_shift(dt64):

    year = dt64.astype('datetime64[Y]').astype(int) + 1970
    month = dt64.astype('datetime64[M]').astype(int) % 12 + 1

    is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

    # Shift
    if is_leap and (month >= 3):
        return dt64 - np.timedelta64(1, 'D')
    else:
        return dt64

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # parser.add_argument("dataset")
    parser.add_argument("--chunk_time", type=int, default=1)
    parser.add_argument("--year", type=int, default=2020)
    args = parser.parse_args()

    # prefix = f"/lustre/orion/world-shared/lrn036/jyc/frontier/ClimaX-v2/data/CMIP6-v2/{args.dataset}/1.0deg"
    prefix = f"IMERG-full"
    files = list()
    # pattern = os.path.join(prefix, f"3B-DAY.MS.MRG.3IMERG.{args.year}*.nc4")
    pattern = os.path.join(prefix, f"3B-DAY.MS.MRG.3IMERG.*.nc4")
    print("glob:", pattern)
    files += glob.glob(pattern)
    print("Reading", files)

    with ProgressBar():
        ds = xr.open_mfdataset(
            files,
            combine="by_coords",
            compat="override",
            coords="minimal",
            join='inner',
        )

    ## change lon: -180-+180 to 0-360
    ds["lon"] = (ds["lon"] + 360) % 360
    ds = ds.sortby("lon")       

    ## nan value
    ds["precipitation"] = ds["precipitation"].bfill(dim="lat") 
    ds["precipitation"] = ds["precipitation"].ffill(dim="lat") 

    ds = ds.rename(coord)
    ds = ds.chunk(
        {"time": args.chunk_time, "latitude": -1, "longitude": -1}
    )
    print(ds)

    ## Save as
    year0, year1 = ds.time[0].dt.year.item(), ds.time[-1].dt.year.item() + 1
    nlon = len(ds["longitude"])
    nlat = len(ds["latitude"])
    gridshape = f"{nlon}x{nlat}"
    outfile = f"datasets/IMERG/IMERG-{year0}-{year1}-1d-{gridshape}.zarr"

    if not os.path.exists("datasets/IMERG"):
        os.makedirs("datasets/IMERG", exist_ok=True)

    with ProgressBar():
        print(f"Saving {outfile} ...")
        ds.to_zarr(outfile, consolidated=True, mode="w")
