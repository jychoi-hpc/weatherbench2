import xarray as xr
import os
from dask.diagnostics import ProgressBar
import argparse
import pandas as pd

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
            yearmon_range = f"{year0}{mon0:02d}-{year1+1}{(mon1+1)%12:02d}"

        outfile = outfile.replace("%{yearmon_range}", yearmon_range)

    return outfile


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="+", help="list of files")
    parser.add_argument(
        "--outfile", help="outfile", default="output-%{grid_shape}.zarr"
    )
    parser.add_argument("--chunk_time", type=int, default=100)
    parser.add_argument("--join", help="join outer, inner, etc.", default="outer")
    parser.add_argument("--mode", help="mode", default="w")
    parser.add_argument("--dryrun", action="store_true", help="dry run")
    args = parser.parse_args()

    # nlon = args.nlon
    # nlat = nlon // 2 + 1

    # outdir = "datasets/era5"
    # # gridshape = "64x33"
    # # gridshape = "256x129"
    # # gridshape = "360x181"
    # # gridshape = "1440x721"
    # gridshape = f"{nlon}x{nlat}"
    # print("gridshape:", gridshape)

    # start_year, end_year = 1959, 2022
    # outfile = os.path.join(outdir, f"1959-2022-6h-{gridshape}-bilinear.zarr")

    isfirst = True
    ds_list = list()
    const_var_list = set()

    for fname in args.filenames:
        print("Read:", fname)
        ds = xr.open_zarr(fname)
        ds_list.append(ds)

        for var in ds.data_vars:
            if len(ds[var].dims) < 3:
                const_var_list.add(var)

    xa = xr.combine_by_coords(ds_list, join=args.join, combine_attrs="override")
    xa = xa.drop_vars(const_var_list)

    for var in const_var_list:
        for ds in ds_list:
            if var in ds:
                xa = xa.merge(ds[var])
                break

    # selected_vars = [
    #         "sea_surface_temperature",
    #         "2m_temperature",
    #         "total_precipitation_24hr",
    #         "2m_temperature_min",
    #         "2m_temperature_max",
    #         "volumetric_soil_water_layer_1",
    # ]
    # xa = xa[selected_vars]

    # Ensure time is a pandas datetime index
    time_index = pd.DatetimeIndex(xa.time.values)

    # Generate the full expected range of dates
    full_time_range = pd.date_range(start=time_index.min(), end=time_index.max(), freq="D")

    # Find missing dates
    missing_days = full_time_range.difference(time_index)
    print("Time range:", time_index.min(), time_index.max())
    print("Missing days:", missing_days)

    xa = xa.chunk({"time": args.chunk_time})
    print(xa)

    outfile = macro_replace(args.outfile, xa)
    print("output_path:", outfile)
    print("mode:", args.mode)

    with ProgressBar():
        for var in xa:
            del xa[var].encoding["chunks"]
        if not args.dryrun:
            xa.to_zarr(outfile, mode=args.mode)
        
        # if args.mode == "a'":
        #     assert os.path.exists(outfile)
        #     ds = xr.open_zarr(outfile)
            
        #     # Add variables
        #     for var in xa.data_vars:
        #         ds[var] = xa[var]
            
        #     print("Appending:", outfile)
        #     ds.to_zarr(outfile, mode=args.mode)
        # else:
        #     print("Writing:", outfile)
        #     xa.to_zarr(outfile, mode="w")
    
    print("Done.")
