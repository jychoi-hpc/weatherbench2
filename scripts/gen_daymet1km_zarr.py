import xarray as xr
import pandas as pd
import numpy as np
from dask.diagnostics import ProgressBar
import argparse
import time
import logging
from mpi4py import MPI
import zarr
import sys

def macro_replace(outfile, xa):
    if "%{grid_shape}" in outfile:
        nlon = len(xa["longitude"])
        nlat = len(xa["latitude"])
        grid_shape = f"{nlon}x{nlat}"

        outfile = outfile.replace("%{grid_shape}", grid_shape)

    if "time" in xa.dims:
        tmin = xa.time.min()
        tmax = xa.time.max()
        if "%{year}" in outfile:
            timestr0 = tmin.dt.strftime("%Y").item()
            outfile = outfile.replace("%{year}", f"{timestr0}")

        if "%{yearmon}" in outfile:
            timestr0 = tmin.dt.strftime("%Y%m").item()
            outfile = outfile.replace("%{yearmon}", f"{timestr0}")

        if "%{yearmonday}" in outfile:
            timestr0 = tmin.dt.strftime("%Y%m%d").item()
            outfile = outfile.replace("%{yearmonday}", f"{timestr0}")

        if "%{year_range}" in outfile:
            timestr0 = tmin.dt.strftime("%Y").item()
            timestr1 = (
                (tmax + np.timedelta64(1, "Y").astype("timedelta64[D]"))
                .dt.strftime("%Y")
                .item()
            )
            outfile = outfile.replace("%{year_range}", f"{timestr0}-{timestr1}")

        if "%{yearmon_range}" in outfile:
            timestr0 = tmin.dt.strftime("%Y%m").item()
            timestr1 = (
                (tmax + np.timedelta64(1, "M").astype("timedelta64[D]"))
                .dt.strftime("%Y%m")
                .item()
            )
            outfile = outfile.replace("%{yearmon_range}", f"{timestr0}-{timestr1}")

        if "%{yearmonday_range}" in outfile:
            timestr0 = tmin.dt.strftime("%Y%m%d").item()
            nhourly = (xa.time[1] - xa.time[0]).item() / 3600 / 1e9
            assert nhourly.is_integer()
            nhourly = int(nhourly)
            timestr1 = (
                (tmax + np.timedelta64(nhourly, "h")).dt.strftime("%Y%m%d").item()
            )
            outfile = outfile.replace("%{yearmonday_range}", f"{timestr0}-{timestr1}")

    return outfile


if __name__ == "__main__":

    # from dask.distributed import Client

    # client = Client(n_workers=8, threads_per_worker=2, memory_limit="32GB", dashboard_address=":8798")

    # num_workers = len(client.scheduler_info()["workers"])
    # print(f"Number of workers: {num_workers}")
    # print(f"Number of Dask workers: {len(client.nthreads())}")
    # logging.getLogger("distributed").setLevel(logging.ERROR)

    parser = argparse.ArgumentParser()
    parser.add_argument("output_path")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    p1 = "/lustre/orion/world-shared/lrn036/jyc/frontier/moet/Daymet_1km/daymet_v4_daily_na_prcp_%{year}.nc"
    p2 = "/lustre/orion/world-shared/lrn036/jyc/frontier/moet/Daymet_1km/daymet_v4_daily_na_tmax_%{year}.nc"
    p3 = "/lustre/orion/world-shared/lrn036/jyc/frontier/moet/Daymet_1km/daymet_v4_daily_na_tmin_%{year}.nc"
    year_list = list(range(1980, 2023))

    t0 = time.time()
    xa_list = list()
    for year in year_list:
        for p in [p1, p2, p3]:
            filename = p.replace("%{year}", str(year))
            x = xr.open_dataset(filename, chunks={"time": 10})
            # x = xr.open_dataset(filename)
            time_range = pd.date_range(
                start=f"{year}-01-01", end=f"{year}-12-31", freq="D"
            )
            # time_range = np.arange(f"{year}-01-01", f"{year+1}-01-01", dtype='datetime64[D]')
            time_range = time_range[:365]
            x = x.assign_coords(time=time_range)
            xa_list.append(x)

    xa = xr.combine_by_coords(xa_list, combine_attrs="drop")
    xa = xa.rename({"lat": "latitude", "lon": "longitude"})

    mk = xa["prcp"][0, :, :].isnull().compute()
    mk = (~mk).astype(np.float32)
    xa["land_sea_mask"] = mk

    xa = xa[["prcp", "tmax", "tmin"]]
    # xa = xa[[args.varname]]

    xa = xa.chunk({"time": 1, "latitude": -1, "longitude": -1})

    # output = f"datasets/daymet/daymet_${year_range}-1d-7200x3600-tmp.zarr"
    output_path = macro_replace(args.output_path, xa)
    print(rank, "output_path:", args.output_path, output_path)
    if rank == 0:
        xa.to_zarr(output_path, mode="w", compute=False)

    comm.Barrier()

    nchunk = len(xa.time) // size
    if rank == size - 1:
        chunk_time = xa.time[rank * nchunk :]
    else:
        chunk_time = xa.time[rank * nchunk : rank * nchunk + nchunk]

    slice_index = xa.time.searchsorted(chunk_time)
    region = {
        "time": slice(slice_index[0], slice_index[-1] + 1),
        "latitude": slice(0, len(xa["latitude"])),
        "longitude": slice(0, len(xa["longitude"])),
    }
    print(rank, "region:", region)

    xa = xa.sel(time=chunk_time)

    if rank == 0:
        print(f"Calc min ({time.time() - t0} sec)")

    mn = xa.sel(time=chunk_time).min().compute()
    for var in ["prcp", "tmax", "tmin"]:
        val_ = mn[var].item()
        val = comm.allreduce(val_, op=MPI.MIN)
        xa[var] = xa[var].fillna(val)

    if rank == 0:
        print(f"Save min ({time.time() - t0} sec)")

    if rank == 0:
        with ProgressBar():
            xa.to_zarr(output_path, region=region)
    else:
        xa.to_zarr(output_path, region=region)

    comm.Barrier()

    if rank == 0:
        print(f"Done ({time.time() - t0} sec)")
