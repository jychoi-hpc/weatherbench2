import xarray as xr
import os
from dask.diagnostics import ProgressBar
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs='+', help="list of files")
    parser.add_argument("--outfile", help="outfile", default="output-%{gridshape}.zarr")
    parser.add_argument("--chunk_time", type=int, default=100)
    # parser.add_argument("nlon", type=int, help="longitude")
    # parser.add_argument("--use_month", action='store_true', help="use_month")
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
    
    xa = xr.combine_by_coords(ds_list)
    xa = xa.drop_vars(const_var_list)

    for var in const_var_list:
        for ds in ds_list:
            if var in ds:
                xa = xa.merge(ds[var])
                break

    xa = xa.chunk({"time": args.chunk_time})
    print(xa)

    outfile = args.outfile
    if "%{gridshape}" in outfile:
        nlon = len(xa["longitude"])
        nlat = len(xa["latitude"])
        gridshape = f"{nlon}x{nlat}"

        outfile = outfile.replace("%{gridshape}", gridshape)

    with ProgressBar():
        for var in xa:
            del xa[var].encoding['chunks']
        print("outfile:", outfile)
        xa.to_zarr(outfile, mode="w")

