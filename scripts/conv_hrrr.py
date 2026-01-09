import warnings

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

import xarray as xr
import argparse
import os
import glob
from tqdm import tqdm
from dask.diagnostics import ProgressBar
import numpy as np
import xesmf as xe
from functools import partial
import sys

def macro_replace(outfile, xa):
    if "%{grid_shape}" in outfile:
        nlat = len(xa["lat"])
        nlon = len(xa["lon"])
        grid_shape = f"{nlat}x{nlon}"

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

def save_to_zarr(dx, output_path, chunk_time=1):
    if os.path.exists(output_path):
        print(f"Already exists. Skip: {output_path}")
        sys.exit()

    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with ProgressBar():
        if "time" in dx.dims:
            dx = dx.chunk({"time": chunk_time})
        print(f"Saving {output_path} ...")
        dx.to_zarr(output_path, mode="w")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # parser.add_argument("dataset")
    parser.add_argument("--chunk_time", type=int, default=1)
    parser.add_argument("--year", type=int, default=2022)
    parser.add_argument("--month", type=int, default=None)
    parser.add_argument("--day", type=int, default=None)
    parser.add_argument("--output_path", default="hrrr_%{yearmonday_range}_%{grid_shape}-bilinear.zarr")
    parser.add_argument("--deg", type=float, default=0.025, help="grid resolution in degree")
    parser.add_argument("--checkonly", action='store_true', help="checkonly")
    parser.add_argument("--dryrun", action="store_true", help="dry run")
    args = parser.parse_args()

    if args.month is not None and args.day is not None:
        yearmonthday = f"{args.year:04d}{args.month:02d}{args.day:02d}*"
    elif args.month is not None:
        yearmonthday = f"{args.year:04d}{args.month:02d}*"
    else:
        yearmonthday = f"{args.year:04d}*"

    # prefix = "/lustre/orion/csc662/world-shared/irl1/HRRR/*/hrrr"
    prefix = "/lustre/orion/csc662/world-shared/irl1/HRRR_upd/%{type}/hrrr"
    # prefix = "/lustre/orion/csc662/world-shared/irl1/HRRR_upd/missing/hrrr"

    us_bounds = (
        24,
        54,
        235,
        295,
    )  # (lat_min, lat_max, lon_min, lon_max) or (125W, 66.5W)
    us_lat_min, us_lat_max, us_lon_min, us_lon_max = us_bounds

    backend_kwargs = {
        "indexpath": "",
    }

    def _preprocess(ds):
        exclude_vars = ["orog", "lsm"]
        # vars_to_drop = [v for v in ds.data_vars if v in exclude_vars]
        # if len(vars_to_drop) > 0:
        #     ds = ds.drop_vars(vars_to_drop)
        # ds = ds.expand_dims("time")
        vars_to_expand = [v for v in ds.data_vars if v not in exclude_vars]
        if len(vars_to_expand) > 0:
            ds[vars_to_expand] = ds[vars_to_expand].expand_dims("time")
        return ds

    partial_func = partial(_preprocess)

    files = list()
    for dataset_type in ["train", "val"]:
        pattern = os.path.join(prefix.replace("%{type}", dataset_type), f"{yearmonthday}/*.grib2")
        print("Searching files with pattern:", pattern)
        files += glob.glob(pattern)

    if len(files) < 1:
        print("No files found. Exit.")
        sys.exit()
    
    if args.checkonly:
        isfirst = True
        for file in tqdm(files):
            ds = xr.open_dataset(file, engine="cfgrib", backend_kwargs=backend_kwargs)
            if "isobaricInhPa" in ds:
                if isfirst:
                    isobaric_levels = ds["isobaricInhPa"].values
                    isfirst = False
                if not np.array_equal(isobaric_levels, ds["isobaricInhPa"].values):
                    print(file, ds["isobaricInhPa"].values)
        sys.exit()

    ds = xr.open_mfdataset(
        files,
        combine="by_coords",
        compat="override",
        coords="minimal",
        decode_timedelta=True,
        preprocess=partial_func,
        engine="cfgrib",
        backend_kwargs=backend_kwargs,
    )

    # output_path = os.path.join("regrid", args.output_path1)
    # output_path = macro_replace(output_path, ds)
    # print("output_path:", output_path)
    # save_to_zarr(ds, output_path, args.chunk_time)

    # ## Reopen and check
    # dy = xr.open_zarr(output_path)
    # print(dy)

    # sys.exit()


    # Define the target uniform grid
    ds_out = xr.Dataset(
        {
            "latitude": (["latitude"], np.arange(us_lat_min, us_lat_max, args.deg)),
            "longitude": (["longitude"], np.arange(us_lon_min, us_lon_max, args.deg)),
        }
    )

    # Create regridder and apply it
    regridder = xe.Regridder(ds, ds_out, method="bilinear", unmapped_to_nan=True)  # Options: "bilinear", "conservative", "nearest_s2d"
    dx = regridder(ds)

    # longitude should be in [-180, 180]
    dx = dx.rename({"longitude": "lon", "latitude": "lat", "isobaricInhPa": "level"})
    dx = dx.assign_coords(lon=((dx.lon + 180) % 360) - 180)
    dx = dx.ffill(dim="lat").bfill(dim="lat")

    output_path = os.path.join("regrid", args.output_path)
    output_path = macro_replace(output_path, dx)
    print("output_path:", output_path)
    if not args.dryrun:
        exclude_vars = ["orog", "lsm"]
        if "orog" in dx:
            dy = dx[exclude_vars]
            dx = dx.drop_vars(exclude_vars)
            output_path2 = os.path.join("regrid", "hrrr_constant_%{grid_shape}-bilinear.zarr")
            output_path2 = macro_replace(output_path2, dy)
            save_to_zarr(dy, output_path2)

        save_to_zarr(dx, output_path, args.chunk_time)

        ## Reopen and check
        dy = xr.open_zarr(output_path)
        print(dy)
    else:
        print("Dry run. Skip saving.")
        print(dx)

    print("Done.")
