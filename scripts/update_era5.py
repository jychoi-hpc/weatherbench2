import xarray as xr
import numpy as np
import argparse
import pandas as pd
from dask.diagnostics import ProgressBar
import sys

xr.set_options(display_max_rows=1000)

if __name__ == '__main__':
    ds = xr.open_zarr("datasets/era5/1959-2022-1h-1440x721.zarr/")
    ds["sea_surface_temperature"] = ds["sea_surface_temperature"].combine_first(ds["2m_temperature"])
    ds["2m_temperature"] = ds["sea_surface_temperature"].combine_first(ds["2m_temperature"])
    print(ds[["sea_surface_temperature", "2m_temperature"]])

    with ProgressBar():
        ds[["sea_surface_temperature", "2m_temperature"]].to_zarr("datasets/era5/1979-2022-1h-1440x721-bilinear.zarr", mode="a")
    sys.exit()

    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="filename")
    parser.add_argument("--year", type=int)
    parser.add_argument("--chunk_time", type=int, default=1)
    args = parser.parse_args()

    source_ds = xr.open_zarr(args.filename)
    if args.year is not None:
        source_ds = source_ds.sel(time=f"{args.year}")
    print(source_ds)

    # nhourly = (source_ds.time[1] - source_ds.time[0]).item() / 3600 / 1e9
    # assert nhourly.is_integer()
    # nhourly = int(nhourly)
    # nsamples = 24 // nhourly

    # extra = source_ds.sel(
    #     time=slice(
    #         source_ds.time[0],
    #         source_ds.time[0] + np.timedelta64(24 - nhourly, "h") - 1,
    #     )
    # )
    # extra = extra.assign_coords(
    #     time=extra.time - np.timedelta64(24 - nhourly, "h")
    # )

    # selected_plus = xr.concat(
    #     [
    #         extra[["sea_surface_temperature", "2m_temperature"]],
    #         source_ds[["sea_surface_temperature", "2m_temperature"]],
    #     ],
    #     dim="time",
    # )

    # ## handle nan values in sea_surface_temperature
    # if "sea_surface_temperature" in selected_plus:
    #     selected_plus["2m_temperature_combined"] = selected_plus[
    #         "sea_surface_temperature"
    #     ].combine_first(selected_plus["2m_temperature"])

    # source_ds["2m_temperature_min"] = (
    #     selected_plus["2m_temperature_combined"]
    #     .rolling(time=nsamples, center=False)
    #     .min()
    #     .dropna("time")
    # )

    # source_ds["2m_temperature_max"] = (
    #     selected_plus["2m_temperature_combined"]
    #     .rolling(time=nsamples, center=False)
    #     .max()
    #     .dropna("time")
    # )

    ds = source_ds["sea_surface_temperature"].combine_first(source_ds["2m_temperature"])
    # source_ds["sea_surface_temperature_combined"] = source_ds["sea_surface_temperature"].combine_first(source_ds["2m_temperature"])
    ds = ds.chunk({"time": args.chunk_time})

    print("ds:", ds)

    with ProgressBar():
        ds.to_zarr(args.filename, mode="a", append_dim="time")
    
    print("Done.")


