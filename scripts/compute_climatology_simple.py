import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from weatherbench2 import utils
import argparse
import sys

def process(dataset, stat, outfilename):
    compute_kwargs = {
        #'obs': obs["10m_u_component_of_wind"],
        'obs': dataset,
        'window_size': 61,
        'clim_years': slice("1990", "2019"),
        'stat_fn': stat,
        'hour_interval': 6,
    }
    print("Processing:", outfilename)
    x = utils.compute_hourly_stat(**compute_kwargs)
    print("Saving:", outfilename)
    x.to_zarr(outfilename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--varname")
    args = parser.parse_args()
    varname = args.varname

    obs = xr.open_zarr("datasets/era5/1959-2022-6h-256x128.zarr")
    obs = obs.drop_vars([k for k, v in obs.items() if 'time' not in v.dims])
    for var in obs.data_vars:
        if "level" in obs[var].dims:
            print("var with level:", var, )
        else:
            print("var:", var)
    print(obs)
    # sys.exit()

    def helper(stat, varname):
        if "level" in obs[varname].dims:
            n = 1
            for level in range(0, 13, n):
                print("var:", varname, level)
                obs_ = obs[varname].isel(level=slice(level, level+n))
                process(obs_, stat, f"tmp/1959-2022-6h-256x128_{varname}_level{level}_{stat}.zarr")
        else:
            print("var:", varname)
            obs_ = obs[varname]
            process(obs_, stat, f"tmp/1959-2022-6h-256x128_{varname}_{stat}.zarr")

    for stat in ["mean", "std"]:
        if varname is None:
            for varname in obs.data_vars:
                helper(stat, varname)
        else:
            helper(stat, varname)

    print("Done.")