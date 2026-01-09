import xarray as xr
import os
from dask.diagnostics import ProgressBar
import argparse
import dask.array as da
import numpy as np
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tqdm import tqdm

xr.set_options(display_max_rows=1000)

def macro_replace(outfile, xa):
    if "%{grid_shape}" in outfile:
        nlon = len(xa["longitude"])
        nlat = len(xa["latitude"])
        grid_shape = f"{nlon}x{nlat}"

        outfile = outfile.replace("%{grid_shape}", grid_shape)

    if "%{year_range}" in outfile:
        year0, year1 = xa.time.min().dt.year.item(), xa.time.max().dt.year.item()
        if year0 == year1:
            year_range = f"{year0}"
        else:
            year_range = f"{year0}-{year1+1}"

        outfile = outfile.replace("%{year_range}", year_range)

    if "%{yearmon_range}" in outfile:
        year0, year1 = xa.time.min().dt.year.item(), xa.time.max().dt.year.item()
        mon0, mon1 = xa.time.min().dt.month.item(), xa.time.max().dt.month.item()
        yearmon_range = f"{year0}{mon0}-{year1}{mon1}"

        if (year0 == year1) and (mon0 == mon1):
            yearmon_range = f"{year0}{mon0:02d}"
        else:
            yearmon_range = f"{year0}{mon0:02d}-{year1+(mon1+1)//12}{(mon1+1)%12:02d}"

        outfile = outfile.replace("%{yearmon_range}", yearmon_range)

    return outfile

def dojob(filename, ds_target, ds, varname, time_start, time_end):
    source = ds_target[varname].isel(time=slice(time_start, time_end))
    var = ds[varname].sel(time=source.time)

    region = dict()    
    region["time"] = slice(time_start, time_end)
    region["longitude"] = slice(0, len(var["longitude"]))
    region["latitude"] = slice(0, len(var["latitude"]))
    
    var.to_zarr(filename, mode="a", region=region)
    
    return varname, time_istart

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("inputfile", help="input")
    parser.add_argument("target", help="target")
    parser.add_argument("--chunk_time", type=int, default=100)
    parser.add_argument("--join", help="join outer, inner, etc.", default="outer")
    parser.add_argument("--dryrun", action="store_true", help="dry run")
    parser.add_argument("--preonly", action="store_true", help="preonly")
    parser.add_argument("--max_workers", type=int, default=8)
    args = parser.parse_args()

    ds = xr.open_zarr(args.inputfile)
    ds_target = xr.open_zarr(args.target)

    if args.preonly:
        encoding = dict()
        for var in ds.data_vars:
            if var in ds_target:
                continue
            shape = [ len(ds_target[x]) for x in ds[var].dims ]
            empty_data = da.full(shape, np.nan, dtype="float32")
            x = xr.DataArray(empty_data, dims=ds[var].dims)
            x = x.chunk({"time": 100})
            ds_target[var] = x
            encoding[var] = {"_FillValue": np.nan}
        print(ds_target)
        ds_target.to_zarr(args.target, mode="a", encoding=encoding, compute=False)
        sys.exit()

    var_list = list()
    for var in ds.data_vars:
        ds_target[var] = ds[var]
        var_list.append(var)

    ds_target = ds_target.chunk({"time": args.chunk_time})
    print(ds_target)

    with ProgressBar():
        if not args.dryrun:
            ds_target[var_list].to_zarr(args.target, mode="a")
    print("Done.")

    # with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
    #     future_list = list()
    #     for time_start in range(0, len(ds_target.time), 100):
    #         for var in ds_target.data_vars:
    #             time_end = time_start + 100 if time_start < len(ds_target.time) - 100 else len(ds_target.time)
    #             future = executor.submit(dojob, args.target, ds_target, ds, var, time_start)
    #             future_list.append(future)

    #     # for future in tqdm(concurrent.futures.as_completed(futures), total=len(future_list)):
    #     for future in tqdm(future_list):
    #         res = future.result()
    
    # print("Done.")

    # ds = xr.open_zarr("datasets/era5/1959-2022-1h-64x33-bilinear.zarr")
    # print(ds["volumetric_soil_water_layer_1"].isel(time=0).values)

    # x = ds_target["volumetric_soil_water_layer_1"].isel(time=slice(0, 100))

    # region = dict()    
    # region["time"] = ds_target.indexes["time"].get_loc("2021")
    # region["longitude"] = slice(0, len(x["longitude"]))
    # region["latitude"] = slice(0, len(x["latitude"]))
    
    # x.to_zarr(args.target, mode="a", region=region)

    # region = {"time": }

    # with ProgressBar():
    #     if not args.dryrun:
    #         ds_target.to_zarr(args.target, mode="a", compute=False)
    # print("Done.")
