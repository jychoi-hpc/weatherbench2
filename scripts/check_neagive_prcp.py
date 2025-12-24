import xarray as xr

for fname in [
    "datasets/IMERG/era5-imerg_1998-2022-1d-1440x721-bilinear.zarr",
    "datasets/IMERG/era5-imerg_1998-2022-1d-360x181-bilinear.zarr"
    ]:

    ds = xr.open_zarr(fname)

    ds["total_precipitation_24hr"] = ds["total_precipitation_24hr"].where(ds["total_precipitation_24hr"]>0, 0.0)
    ds["total_precipitation_24hr"].to_zarr(fname, mode="a")

    ds = xr.open_zarr(fname)
    print(ds["total_precipitation_24hr"].min().compute())

    print("Done:", fname)
