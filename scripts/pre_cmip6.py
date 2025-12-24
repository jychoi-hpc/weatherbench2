import xarray as xr
import argparse
import os
import glob
from tqdm import tqdm
from dask.diagnostics import ProgressBar
import numpy as np
import geocat.comp as gc


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--prefix", default="cmip6_pre")
    args = parser.parse_args()

    ds = xr.open_dataset(args.input)

    p_levels_selected = np.array([850, 500, 200])*100
    p_levels = np.arange(1000, 100, -10)*100
    # pressure = ds.kz * ds.ps
    hyam = ds.kz * 0.0
    hybm = ds.kz

    ds_interp = xr.Dataset()

    for var in tqdm(["ua", "va", "ta", "hus"]):
        ta_interp = gc.interp_hybrid_to_pressure(
            data=ds[var],       # 3D or 4D field (time, kz, iy, jx)
            ps=ds.ps,      # Surface pressure
            hyam=hyam,     # Hybrid A coefficients
            hybm=hybm,     # Hybrid B coefficients
            new_levels=p_levels,  # Target pressure levels
            p0=100_000,         # Reference pressure (Pa),
        ).compute()
        ds_interp[var] = ta_interp

    ta_interp_filled = ds_interp.bfill(dim="plev")
    ta_interp_filled = ta_interp_filled.sel(plev=p_levels_selected)

    for var in ["ps", "ts"]:
        ta_interp_filled[var] = ds[var]

    dirname = os.path.dirname(args.input)
    basename = os.path.basename(args.input)
    prefix = args.prefix
    if not os.path.exists(prefix):
        os.makedirs(prefix, exist_ok=True)
    outfile = os.path.join(prefix, basename)
    print("Save:", outfile)

    with ProgressBar():
        ta_interp_filled.to_netcdf(outfile)
    print("Done.")
