import numpy as np
import xarray as xr
import glob
import os

from natsort import natsorted
from tqdm import tqdm
import argparse
from dask.diagnostics import ProgressBar

def check(expdir, year):
    prefix = "/lustre/orion/world-shared/lrn036/jyc/frontier/super-res-torchlight/examples"
    regexp = f"{prefix}/exp/{expdir}/eval/{year}/eval_rank0_{year}-*.nc"
    files = natsorted(glob.glob(regexp))
    print("EXP:", expdir, regexp, len(files))

    fname = f"{prefix}/exp/{expdir}/eval-{year}.nc"
        
    ds = xr.open_mfdataset(files, combine="nested", compat="override")
    ds = ds.load()
    ds

    ds["input"][:] = np.nan
    ds["truth"][:] = np.nan
    ds["prediction"][:] = np.nan
    ds["count"] = xr.zeros_like(ds["prediction"])

    ds_list = list()
    for fname in tqdm(files):
        ds_ = xr.open_dataset(fname)
        ds_list.append(ds_)
    
    for fname in tqdm(files):
        ds_ = xr.open_dataset(fname)
        for var in ["input"]:
            ds[var].loc[dict(time=ds_.time, longitude2=ds_.longitude2, latitude2=ds_.latitude2)] = ds_[var]
        for var in ["truth", "prediction"]:
            ds[var].loc[dict(time=ds_.time, longitude=ds_.longitude, latitude=ds_.latitude)] = ds_[var]
        for var in ["prediction"]:
            ds["count"].loc[dict(time=ds_.time, longitude=ds_.longitude, latitude=ds_.latitude)] += 1.0

    fname = f"{prefix}/exp/{expdir}/eval-{year}.nc"
    ds.to_netcdf(fname, mode="w")


def check_(expdir, year, month):
    prefix = "/lustre/orion/world-shared/lrn036/jyc/frontier/super-res-torchlight/examples"
    regexp = f"{prefix}/exp/{expdir}/eval/eval_rank0_{year}-{month:02d}-*.nc"
    files = natsorted(glob.glob(regexp))
    print("EXP:", expdir, regexp, len(files))

    fname = f"{prefix}/exp/{expdir}/eval-{year}-{month:02d}.nc"
        
    ds = xr.open_mfdataset(files, combine="nested", compat="override")
    ds = ds.load()
    ds

    ds["input"][:] = np.nan
    ds["truth"][:] = np.nan
    ds["prediction"][:] = np.nan
    ds["count"] = xr.zeros_like(ds["prediction"])

    ds_list = list()
    for fname in tqdm(files):
        ds_ = xr.open_dataset(fname)
        ds_list.append(ds_)
    
    for fname in tqdm(files):
        ds_ = xr.open_dataset(fname)
        for var in ["input"]:
            ds[var].loc[dict(time=ds_.time, longitude2=ds_.longitude2, latitude2=ds_.latitude2)] = ds_[var]
        for var in ["truth", "prediction"]:
            ds[var].loc[dict(time=ds_.time, longitude=ds_.longitude, latitude=ds_.latitude)] = ds_[var]
        for var in ["prediction"]:
            ds["count"].loc[dict(time=ds_.time, longitude=ds_.longitude, latitude=ds_.latitude)] += 1.0

    fname = f"{prefix}/exp/{expdir}/eval-{year}-{month:02d}.nc"
    ds.to_netcdf(fname, mode="w")

    # plt.figure()
    # # ds["input"].isel(time=0).plot.imshow(x="longitude2", y="latitude2", size=4, aspect=2)
    # ds["truth"].isel(time=0).plot.imshow(x="longitude", y="latitude", size=4, aspect=2)
    # ds["prediction"].isel(time=0).plot.imshow(x="longitude", y="latitude", size=4, aspect=2)
    # plt.show()

    # dx = xr.open_zarr("/lustre/orion/lrn036/world-shared/jyc/frontier/weatherbench2/datasets/IMERG/IMERG-1998-2025-1d-5760x2881-bilinear.zarr")
    # mask = dx["land_sea_mask"].values[:,:-1].T
    # mask.shape

    # x = ds["truth"].sel(time=slice("2020-07-01", "2020-07-30"))
    # y = ds["prediction"].sel(time=slice("2020-07-01", "2020-07-30"))
    # x = np.expm1(x)
    # y = np.expm1(y)
    # x = x.where(mask > 0, np.nan)
    # y = y.where(mask > 0, np.nan)

    # plt.figure()
    # x.isel(time=0).plot.imshow(x="longitude", y="latitude", size=4, aspect=2)
    # y.isel(time=0).plot.imshow(x="longitude", y="latitude", size=4, aspect=2)
    # plt.show()
    
    # xx = x.values.ravel()
    # yy = y.values.ravel()
    # # plt.scatter(xx, yy, edgecolors='none', s=10, alpha=0.5)
    # # plt.axline(slope=1, color='r', linestyle='--')
    # # plt.xlabel("truth")
    # # plt.ylabel("prediction")

    # plt.figure()
    # mk = np.isnan(xx) | np.isnan(yy)
    # plt.hist2d(xx[~mk], yy[~mk], bins=100, norm=LogNorm(), cmap='viridis');
    # plt.axline((0, 0), slope=1, color='red', linestyle='--', linewidth=0.5)
    # plt.xlabel("truth")
    # plt.ylabel("prediction")
    # plt.axis("square")
    # plt.colorbar().set_label("Frequency")
    # plt.show()

    # rmse = np.sqrt(np.nanmean((x - y)**2))
    # print("RMSE:", rmse.item())
    # print("max truth, prediction:", np.nanmax(x), np.nanmax(y))

    return ds

if __name__ == "__main__":
    argparse = argparse.ArgumentParser()
    argparse.add_argument("--expdir", type=str, default="finetune-xi-20250707")
    argparse.add_argument("--year", type=int, default=2020)
    # argparse.add_argument("--month", type=int, default=7)
    args = argparse.parse_args()

    # check(args.expdir, args.year, args.month)
    check(args.expdir, args.year)

    print("Done.")

