import xarray as xr
import numpy as np
import argparse
import pandas as pd

xr.set_options(display_max_rows=1000)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="filename")
    parser.add_argument("--checknan", action="store_true", help="check nan")
    args = parser.parse_args()

    print("filename:", args.filename)

    ds = xr.open_zarr(args.filename)
    print(ds)
    print("Chunk:")
    print(ds.chunk)

    if "time" in ds.dims:
        print(ds.time)

        # Ensure time is a pandas datetime index
        time_index = pd.DatetimeIndex(ds.time.values)

        # Generate the full expected range of dates
        full_time_range = pd.date_range(start=time_index.min(), end=time_index.max(), freq="D")

        # Find missing dates
        missing_days = full_time_range.difference(time_index)
        print("Time range:", time_index.min(), time_index.max())
        print("Missing days:", missing_days)

    if args.checknan:
        print("Checking nan:")
        # Check which variables have NaNs
        for var in ds.data_vars:
            print(var, "\t", ds[var].isnull().any().compute().item())

    print("Done.")
