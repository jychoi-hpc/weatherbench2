import xarray as xr
import os
import glob
import numpy as np
import argparse
import sys

varname_list = [
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "10m_wind_speed",
    "2m_temperature",
    "geopotential",
    "mean_sea_level_pressure",
    "sea_ice_cover",
    "sea_surface_temperature",
    "specific_humidity",
    "surface_pressure",
    "temperature",
    "toa_incident_solar_radiation",
    "toa_incident_solar_radiation_12hr",
    "toa_incident_solar_radiation_24hr",
    "toa_incident_solar_radiation_6hr",
    "total_cloud_cover",
    "total_column_water_vapour",
    "total_precipitation_12hr",
    "total_precipitation_24hr",
    "total_precipitation_6hr",
    "u_component_of_wind",
    "v_component_of_wind",
    "vertical_velocity",
    "wind_speed",
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--grid", default="256x128")
    args = parser.parse_args()
    grid = args.grid
    nlon, nlat = int(grid.split("x")[0]), int(grid.split("x")[1])
    print (grid, nlon, nlat)

    outfile = f"tmp/1959-2022-6h-{grid}_mean_std.zarr"
    isfirst = True
    for stat in ["mean", "std"]:
        ds_list = list()
        var_list = list()
        for varname in varname_list:
            fname = f"tmp/1959-2022-6h-{grid}_{varname}_level0_mean.zarr"
            if os.path.exists(fname):
                level_list = list()
                for level in range(13):
                    fname = f"tmp/1959-2022-6h-{grid}_{varname}_level{level}_{stat}.zarr"
                    ds = xr.open_zarr(fname)
                    print(fname, ds[varname].shape)
                    level_list.append(ds)
                ds_ = xr.concat(level_list, dim="level")
                ds_list.append(ds_)
                var_list.append(varname)
            else:
                fname = f"tmp/1959-2022-6h-{grid}_{varname}_{stat}.zarr"
                ds = xr.open_zarr(fname)
                print(fname, ds[varname].shape)
                ds_list.append(ds)
                var_list.append(varname)

        for varname, ds in zip(var_list, ds_list):
            print("write:", varname)
            print("dims:", ds[varname].dims)
            ds[varname] = ds[varname].astype(np.float32)
            ds = ds.chunk({"hour": 13, "longitude": nlon, "latitude": nlat})
            if "level" in ds[varname].dims:
                ds = ds.chunk({"level": 13})
                print("level:", ds["level"])

            if stat == "std":
                ds = ds.rename({f"{varname}": f"{varname}_{stat}"})
                varname = f"{varname}_{stat}"

            if isfirst:
                ds.to_zarr(outfile, mode="w")
                isfirst = False
            else:
                ds.to_zarr(outfile, mode="a")
    
    ds = xr.open_zarr(outfile)
    print(ds)
    print("Done.")

