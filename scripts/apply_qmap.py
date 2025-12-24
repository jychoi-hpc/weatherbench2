import xarray as xr
import argparse
import os
import glob
from tqdm import tqdm
from dask.diagnostics import ProgressBar
import numpy as np
from scipy.interpolate import interp1d
import sys

def macro_replace(outfile, xa):
    if "%{grid_shape}" in outfile:
        nlon = len(xa["longitude"])
        nlat = len(xa["latitude"])
        grid_shape = f"{nlon}x{nlat}"

        outfile = outfile.replace("%{grid_shape}", grid_shape)

    if "time" in xa.dims:
        tmin = xa.time.min()
        tmax = xa.time.max()
        if "%{year}" in outfile:
            timestr0 = tmin.dt.strftime("%Y").item()
            outfile = outfile.replace("%{year}", f"{timestr0}")

        if "%{yearmon}" in outfile:
            timestr0 = tmin.dt.strftime("%Y%m").item()
            outfile = outfile.replace("%{yearmon}", f"{timestr0}")

        if "%{yearmonday}" in outfile:
            timestr0 = tmin.dt.strftime("%Y%m%d").item()
            outfile = outfile.replace("%{yearmonday}", f"{timestr0}")

        if "%{year_range}" in outfile:
            timestr0 = tmin.dt.strftime("%Y").item()
            timestr1 = (tmax + np.timedelta64(1, "Y").astype("timedelta64[D]")).dt.strftime("%Y").item()
            outfile = outfile.replace("%{year_range}", f"{timestr0}-{timestr1}")

        if "%{yearmon_range}" in outfile:
            timestr0 = tmin.dt.strftime("%Y%m").item()
            timestr1 = (tmax + np.timedelta64(1, "M").astype("timedelta64[D]")).dt.strftime("%Y%m").item()
            outfile = outfile.replace("%{yearmon_range}", f"{timestr0}-{timestr1}")

        if "%{yearmonday_range}" in outfile:
            timestr0 = tmin.dt.strftime("%Y%m%d").item()
            nhourly = (xa.time[1] - xa.time[0]).item() / 3600 / 1e9
            assert nhourly.is_integer()
            nhourly = int(nhourly)
            timestr1 = (
                (tmax + np.timedelta64(nhourly, "h")).dt.strftime("%Y%m%d").item()
            )
            outfile = outfile.replace("%{yearmonday_range}", f"{timestr0}-{timestr1}")

    return outfile

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path")
    parser.add_argument("--output_path")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--day_begin", type=int)
    parser.add_argument("--day_end", type=int)
    args = parser.parse_args()

    # data = np.load('interp_data.npz')
    data = np.load('interp_data-daymet.npz')
    x_loaded = data['x']
    y_loaded = data['y']
    qm_func = interp1d(x_loaded, y_loaded, kind='linear', fill_value="extrapolate")

    ds = xr.open_zarr(args.input_path)
    print(ds)

    ## subselect
    if args.year is not None:
        start_time = f"{args.year:04d}"
        end_time = f"{args.year:04d}"
        if args.month is not None:
            start_time = f"{args.year:04d}-{args.month:02d}"
            end_time = f"{args.year:04d}-{args.month:02d}"
            if args.day_begin is not None:
                start_time = f"{args.year:04d}-{args.month:02d}-{args.day_begin:02d}"
                end_time = f"{args.year:04d}-{args.month:02d}-{args.day_end:02d}"
        ds = ds.sel(time=slice(start_time, end_time))

    ## apply quantile mapping
    with ProgressBar():
        print(f"QMaping ...")
        ds["total_precipitation_24hr"] = xr.apply_ufunc(
            qm_func,
            ds["total_precipitation_24hr"].load(),
            vectorize=True,                 # apply separately to each (lat, lon)
        )
        ds["total_precipitation_24hr"] = ds["total_precipitation_24hr"].where(ds["total_precipitation_24hr"] >= 0, 0.0)

    output_path = args.output_path
    output_path = macro_replace(output_path, ds)
    print("output_path:", output_path)

    if os.path.exists(output_path):
        print(f"Already exists. Skip: {output_path}")
        sys.exit()

    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with ProgressBar():
        print(f"Saving {output_path} ...")
        ds.to_zarr(output_path, mode="w")

    ## reopen and check negative
    with ProgressBar():
        ds = xr.open_zarr(output_path)
        print("Check total_precipitation_24hr ...")
        print(ds["total_precipitation_24hr"].min().compute())

    print("Done.")
