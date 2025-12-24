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
    "plev": "level",
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
    parser.add_argument("dataset")
    parser.add_argument("--chunk_time", type=int, default=100)
    args = parser.parse_args()

    # prefix = f"/lustre/orion/world-shared/lrn036/jyc/frontier/ClimaX-v2/data/CMIP6-v2/{args.dataset}/1.0deg"
    prefix = f"/lustre/orion/world-shared/lrn036/CMIPS6-dataset/raw/{args.dataset}/"
    if args.dataset == "MRI-ESM2-0":
        files = list()
        files += glob.glob(os.path.join(prefix, "hus_6hrPlevPt_*.nc"))
        files += glob.glob(os.path.join(prefix, "ta_6hrPlevPt_*.nc"))
        files += glob.glob(os.path.join(prefix, "ua_6hrPlevPt_*.nc"))
        files += glob.glob(os.path.join(prefix, "va_6hrPlevPt_*.nc"))
        files += glob.glob(os.path.join(prefix, "zg_6hrPlevPt_*.nc"))
        files += glob.glob(os.path.join(prefix, "tas_*.nc"))
        files += glob.glob(os.path.join(prefix, "uas_*.nc"))
        files += glob.glob(os.path.join(prefix, "vas_*.nc"))
    print("Reading", files)

    with ProgressBar():
        ds = xr.open_mfdataset(
            files,
            combine="by_coords",
            compat="override",
            coords="minimal",
            join='inner',
        )

    ds = ds.sel(time=~ds.time.dt.hour.isin([3, 9, 15, 21]))
    # ## Change noleap to leap:
    # ds["time"] = ds["time"].astype("datetime64[ns]")
    # if (len(ds["time"].sel(time="2012-02-28")) > 0) and (len(ds["time"].sel(time="2012-02-29")) == 0):
    #     print("Change no-leap to leap calendar")
    #     time_new = np.array([time_shift(x) for x in ds["time"].values])
    #     ds = ds.assign_coords(time=time_new)
        
    ds = ds.drop_vars(set(ds.data_vars) - var_dict.keys())
    ds = ds.reset_coords(drop=True)

    rename_dict = {old: new for old, new in var_dict.items() if old in ds}
    ds = ds.rename(rename_dict)

    ds = ds.rename(coord)
    ds = ds.assign_coords(level=ds["level"] / 100)
    ds["level"].attrs["standard_name"] = "air_pressure"
    ds["level"].attrs["long_name"] = "pressure"
    ds["level"].attrs["units"] = "hPa"

    ds = ds.interp(level=DEFAULT_PRESSURE_LEVELS)
    ds = ds.sel(level=DEFAULT_PRESSURE_LEVELS)
    # ds = ds.sel(time=slice("1980-01-01", None))

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
    outfile = f"datasets/cmip6/{args.dataset}-{year0}-{year1}-{gridshape}.zarr"

    if not os.path.exists("datasets/cmip6"):
        os.makedirs("datasets/cmip6", exist_ok=True)

    with ProgressBar():
        print(f"Saving {outfile} ...")
        ds.to_zarr(outfile, consolidated=True, mode="w")
