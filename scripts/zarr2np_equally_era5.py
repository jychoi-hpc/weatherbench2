import glob
import os
import sys

import click
import numpy as np
import xarray as xr
import pandas as pd
from tqdm import tqdm

import shutil
import re

import dask
from dask.distributed import Client
import dask.array as da
from dask.array.core import is_dask_collection
from glob import glob
from natsort import natsorted
import calendar
from filelock import FileLock, SoftFileLock, Timeout

from mpi4py import MPI
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from mpi4py.futures import MPICommExecutor, MPIPoolExecutor
from contextlib import nullcontext

dask.config.set(**{"array.slicing.split_large_chunks": False})

"""
import xarray as xr
import numpy as np

# Open the existing Zarr file in "append" mode
ds = xr.open_zarr('era5/1959-2022-6h-64x32_equiangular_conservative.zarr')

# Create the new data to add as a variable
orography = ob["orography"][0,0,:,:].T  # Shape must match existing dimensions
print(orography.shape)

# Add the new variable to the Dataset
ds['orography'] = (('longitude', 'latitude'), orography)

doy = np.array([ x.astype('M8[h]').item().timetuple().tm_yday for x in ds.time.data ], dtype=np.int32)
tod = np.array([ x.astype('M8[h]').item().timetuple().tm_hour for x in ds.time.data ], dtype=np.int32)
ds['days_of_year'] = (('time'), doy)
ds['time_of_day'] = (('time'), tod)

# Save changes back to the Zarr store
ds.to_zarr('era5/1959-2022-6h-64x32_equiangular_conservative.zarr', mode='a')

"""
CONSTANT_VARS = ["land_sea_mask", "orography", "latitude"]
EXTRA_VARS = ["days_of_year", "time_of_day"]

SINGLE_LEVEL_VARS = [
    "2m_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "mean_sea_level_pressure",
    "surface_pressure",
    # "toa_incident_solar_radiation",
    # "total_precipitation_6hr",
    "total_precipitation",
    "sea_surface_temperature",
]

PRESSURE_LEVEL_VARS = [
    "geopotential",
    "u_component_of_wind",
    "v_component_of_wind",
    "temperature",
    "specific_humidity",
]

DEFAULT_PRESSURE_LEVELS = [
    50,
    100,
    150,
    200,
    250,
    300,
    400,
    500,
    600,
    700,
    850,
    925,
    1000,
]


DAYS_PER_YEAR = 365  # 365-day year
HOURS_PER_YEAR = DAYS_PER_YEAR * 24  # 365-day year


class TqdmWithDepth(tqdm):
    # Class variable to track the current depth (global counter)
    depth = 0

    def __init__(self, *args, **kwargs):
        # Increment depth when creating a new instance
        # print("TqdmWithDepth.depth:", TqdmWithDepth.depth)
        if TqdmWithDepth.depth > 0:
            kwargs["leave"] = False
        else:
            kwargs["leave"] = True
        TqdmWithDepth.depth += 1
        super().__init__(*args, **kwargs)

    def __del__(self):
        TqdmWithDepth.depth -= 1
        super().__del__()


def da2np(np_vars):
    for k in list(np_vars.keys()):
        if is_dask_collection(np_vars[k]):
            np_vars[k] = np_vars[k].compute()
        # if isinstance(np_vars[k], np.ndarray):
        #     print(k, np_vars[k].shape)
        # else:
        #     print(k, np_vars[k])


def get_sharded_time_range(
    year,
    shard_id,
    num_steps_per_shard,
    extra_steps,
    hrs_each_step,
    year_end,
    daysofyear=366,
):
    base_time = pd.to_datetime(f"{year}-01-01")
    start_timedelta = pd.to_timedelta(
        shard_id * num_steps_per_shard * hrs_each_step, unit="h"
    )
    end_timedelta = pd.to_timedelta(
        (shard_id + 1) * num_steps_per_shard * hrs_each_step
        + extra_steps * hrs_each_step,
        unit="h",
    )
    start_time = base_time + start_timedelta
    end_time = base_time + end_timedelta

    ## year end as +365
    # year_end_time = pd.to_datetime(f"{year_end}-01-01") + pd.to_timedelta(DAYS_PER_YEAR, unit="D")
    year_end_time = pd.to_datetime(f"{year_end+1}-01-01")

    if end_time > year_end_time:
        end_time = year_end_time

    sharded_time_range = pd.date_range(
        start_time,
        end_time,
        freq=f"{hrs_each_step}h",
        inclusive="left",
    )
    sharded_time_range = sharded_time_range[
        sharded_time_range.dayofyear < daysofyear + 1
    ]

    return sharded_time_range


def get_data(
    xa,
    var,
    total_num_steps_per_shard,
    hrs_each_step,
    sharded_time_range=None,
    level=None,
):
    kw = {}
    if sharded_time_range is not None:
        kw["time"] = sharded_time_range
    if level is not None:
        kw["level"] = level

    xdata = xa[var].sel(**kw)
    assert len(xdata.shape) < 5, ("too many dims", xdata.shape)
    x = xdata.data
    if len(x.shape) == 1:  ## extra vars
        x = x[:, np.newaxis, np.newaxis]
        x = np.broadcast_to(x, (len(xdata), len(xa.longitude), len(xa.latitude)))

    if len(x) < total_num_steps_per_shard:
        year = sharded_time_range[0].year
        extra_steps = total_num_steps_per_shard - len(x)
        assert extra_steps == 36 if calendar.isleap(year) else 40, (
            extra_steps,
            year,
            calendar.isleap(year),
            var,
        )
        ## wrap-around
        base_time = pd.to_datetime(f"{year}-01-01")
        extra_time_range = pd.date_range(
            base_time,
            base_time + pd.to_timedelta(extra_steps * hrs_each_step, unit="h"),
            freq=f"{hrs_each_step}h",
            inclusive="left",
        )

        kw = {}
        kw["time"] = extra_time_range
        if level is not None:
            kw["level"] = level

        extra = xa[var].sel(**kw)
        extra = extra.data
        if len(extra.shape) == 1:  ## extra vars
            extra = extra[:, np.newaxis, np.newaxis]
            extra = np.broadcast_to(
                extra, (len(extra), len(xa.longitude), len(xa.latitude))
            )

        x = np.vstack((x, extra))
    return x


def get_mean_std(
    xa,
    var,
    sharded_time_range=None,
    level=None,
):
    if level is not None:
        mean = xa[var].sel(time=sharded_time_range, level=level).mean().compute()
        std = xa[var].sel(time=sharded_time_range, level=level).std().compute()
    else:
        mean = xa[var].sel(time=sharded_time_range).mean().compute()
        std = xa[var].sel(time=sharded_time_range).std().compute()

    return mean, std


def get_mean2d(
    xa,
    var,
    sharded_time_range=None,
    level=None,
):
    if level is not None:
        mean2d = (
            xa[var].sel(time=sharded_time_range, level=level).mean(axis=0).compute()
        )
    else:
        mean2d = xa[var].sel(time=sharded_time_range).mean(axis=0).compute()

    return mean2d


def zarr2nc_normalize(
    xa,
    years,
    save_dir,
    partition,
    num_shards_per_year=8,
    hrs_each_step=6,
    extra_steps=40,
    executor=None,
    daysofyear=366,
):
    ## mean and std
    sharded_time_range = pd.date_range(
        f"{years[0]}-01-01",
        f"{years[-1]+1}-01-01",
        freq=f"{hrs_each_step}h",
        inclusive="left",
    )
    sharded_time_range = sharded_time_range[
        sharded_time_range.dayofyear < daysofyear + 1
    ]

    if partition == "train":
        filename = os.path.join(save_dir, "normalize_mean.npz")
        if os.path.exists(filename):
            ## skip when already exists
            return
        lockfile = os.path.join(save_dir, ".normalize_mean.npz.lock")
        lock = SoftFileLock(lockfile, timeout=0)
        try:
            lock.acquire()
        except Timeout:
            ## someone is working
            return

        normalize_mean = dict()
        normalize_std = dict()

        for var in TqdmWithDepth(CONSTANT_VARS, desc="constant"):
            xdata = xa[var]
            x = xdata.data
            if var == "latitude":
                x = np.repeat(x[np.newaxis, :], len(xa["longitude"]), axis=0)
            normalize_mean[var] = [x.mean()]
            normalize_std[var] = [x.std()]

        if executor is not None:
            future_list = list()
            var_list = list()
            for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
                f = executor.submit(
                    get_mean_std,
                    xa,
                    var,
                    sharded_time_range=sharded_time_range,
                )
                future_list.append(f)
                var_list.append(var)

            for var, f in TqdmWithDepth(
                zip(var_list, future_list),
                desc="single (concurrent)",
                total=len(future_list),
            ):
                mean, std = f.result()
                normalize_mean[var] = [mean]
                normalize_std[var] = [std]
        else:
            for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
                mean = xa[var].sel(time=sharded_time_range).mean().compute()
                std = xa[var].sel(time=sharded_time_range).std().compute()
                normalize_mean[var] = [mean]
                normalize_std[var] = [std]

        if executor is not None:
            future_list = list()
            var_list = list()
            level_list = list()
            for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
                for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
                    f = executor.submit(
                        get_mean_std,
                        xa,
                        var,
                        sharded_time_range=sharded_time_range,
                        level=level,
                    )
                    future_list.append(f)
                    var_list.append(var)
                    level_list.append(level)

            for var, level, f in TqdmWithDepth(
                zip(var_list, level_list, future_list),
                desc="pressure (concurrent)",
                total=len(future_list),
            ):
                mean, std = f.result()
                normalize_mean[f"{var}_{level}"] = [mean]
                normalize_std[f"{var}_{level}"] = [std]
        else:
            for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
                for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
                    mean = (
                        xa[var]
                        .sel(time=sharded_time_range, level=level)
                        .mean()
                        .compute()
                    )
                    std = (
                        xa[var]
                        .sel(time=sharded_time_range, level=level)
                        .std()
                        .compute()
                    )
                    normalize_mean[f"{var}_{level}"] = [mean]
                    normalize_std[f"{var}_{level}"] = [std]

        da2np(normalize_mean)
        da2np(normalize_std)
        np.savez(os.path.join(save_dir, "normalize_mean.npz"), **normalize_mean)
        np.savez(os.path.join(save_dir, "normalize_std.npz"), **normalize_std)


def zarr2nc_climatology(
    xa,
    years,
    save_dir,
    partition,
    num_shards_per_year=8,
    hrs_each_step=6,
    extra_steps=40,
    executor=None,
    daysofyear=366,
):
    filename = os.path.join(save_dir, partition, "climatology.npz")
    if os.path.exists(filename):
        ## skip when already exists
        return
    lockfile = os.path.join(save_dir, partition, ".climatology.npz.lock")
    lock = SoftFileLock(lockfile, timeout=0)
    try:
        lock.acquire()
    except Timeout:
        ## someone is working
        return

    ## mean and std
    sharded_time_range = pd.date_range(
        f"{years[0]}-01-01",
        f"{years[-1]+1}-01-01",
        freq=f"{hrs_each_step}h",
        inclusive="left",
    )
    sharded_time_range = sharded_time_range[
        sharded_time_range.dayofyear < daysofyear + 1
    ]

    ## climatology
    climatology = dict()

    for var in TqdmWithDepth(CONSTANT_VARS, desc="constant"):
        xdata = xa[var]
        x = xdata.data
        if var == "latitude":
            x = np.repeat(x[np.newaxis, :], len(xa["longitude"]), axis=0)
        x = np.expand_dims(x, axis=0)
        climatology[var] = x

    if executor is not None:
        future_list = list()
        var_list = list()
        for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
            f = executor.submit(
                get_mean2d,
                xa,
                var,
                sharded_time_range=sharded_time_range,
            )
            future_list.append(f)
            var_list.append(var)

        for var, f in TqdmWithDepth(
            zip(var_list, future_list),
            desc="single (concurrent)",
            total=len(future_list),
        ):
            mean2d = f.result()
            mean2d = np.expand_dims(mean2d, axis=0)
            climatology[var] = mean2d
    else:
        for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
            mean2d = xa[var].sel(time=sharded_time_range).mean(axis=0).compute()
            mean2d = np.expand_dims(mean2d, axis=0)
            climatology[var] = mean2d

    if executor is not None:
        future_list = list()
        var_list = list()
        level_list = list()

        for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
            for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
                f = executor.submit(
                    get_mean2d,
                    xa,
                    var,
                    sharded_time_range=sharded_time_range,
                    level=level,
                )
                future_list.append(f)
                var_list.append(var)
                level_list.append(level)

        for var, level, f in TqdmWithDepth(
            zip(var_list, level_list, future_list),
            desc="pressure (concurrent)",
            total=len(future_list),
        ):
            mean2d = f.result()
            mean2d = np.expand_dims(mean2d, axis=0)
            climatology[f"{var}_{level}"] = mean2d
    else:
        for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
            for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
                mean2d = (
                    xa[var]
                    .sel(time=sharded_time_range, level=level)
                    .mean(axis=0)
                    .compute()
                )
                mean2d = np.expand_dims(mean2d, axis=0)
                climatology[f"{var}_{level}"] = mean2d

    da2np(climatology)
    np.savez(os.path.join(save_dir, partition, "climatology.npz"), **climatology)

    # for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
    #     clim = dict()
    #     fname = os.path.join(save_dir, partition, f"climatology_{var}.npz")
    #     if not os.path.exists(fname):
    #         mean2d = xa[var].sel(time=sharded_time_range).mean(axis=0).compute()
    #         mean2d = np.expand_dims(mean2d, axis=0)
    #         climatology[var] = mean2d
    #         clim[var] = mean2d
    #         np.savez(fname, **clim)
    #     else:
    #         with np.load(fname) as f:
    #             clim[var] = f[var]
    #         climatology.update(clim)

    # for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
    #     clim = dict()
    #     fname = os.path.join(save_dir, partition, f"climatology_{var}.npz")
    #     if not os.path.exists(fname):
    #         for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
    #             mean2d = (
    #                 xa[var]
    #                 .sel(time=sharded_time_range, level=level)
    #                 .mean(axis=0)
    #                 .compute()
    #             )
    #             mean2d = np.expand_dims(mean2d, axis=0)
    #             clim[f"{var}_{level}"] = mean2d
    #         np.savez(fname, **clim)
    #     else:
    #         with np.load(fname) as f:
    #             for x in f.files:
    #                 clim[x] = f[x]
    #     climatology.update(clim)

    # da2np(climatology)
    # print(os.path.join(save_dir, partition, "climatology.npz"))
    # np.savez(os.path.join(save_dir, partition, "climatology.npz"), **climatology)

    # ## delete temp files
    # for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
    #     fname = os.path.join(save_dir, partition, f"climatology_{var}.npz")
    #     os.remove(fname)

    # for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
    #     fname = os.path.join(save_dir, partition, f"climatology_{var}.npz")
    #     os.remove(fname)


def isclose_with_nan(a, b, **kwargs):
    # Check where both values are NaN
    both_nan = np.isnan(a) & np.isnan(b)
    # Apply np.isclose to non-NaN values
    close = np.isclose(a, b, **kwargs)
    # Return True where both are NaN, or np.isclose result otherwise
    return both_nan | close


def zarr2nc_check(
    xa,
    save_dir,
    daysofyear=366,
):
    for dataset in TqdmWithDepth(["train", "val", "test"], desc="dataset"):
        files = natsorted(glob(os.path.join(save_dir, dataset, "????_*.npz")))
        basename = os.path.basename(files[-1])
        year_end, _ = map(lambda x: int(x), os.path.splitext(basename)[0].split("_"))
        for file in TqdmWithDepth(files, desc="files"):
            basename = os.path.basename(file)
            year, shard_id = map(
                lambda x: int(x), os.path.splitext(basename)[0].split("_")
            )
            with np.load(file) as f:
                hrs_each_step = f["hrs_each_step"].item()
                num_steps_per_shard = f["num_steps_per_shard"].item()
                extra_steps = f["extra_steps"].item()
                total_num_steps_per_shard = num_steps_per_shard + extra_steps

                sharded_time_range = get_sharded_time_range(
                    year,
                    shard_id,
                    num_steps_per_shard,
                    extra_steps,
                    hrs_each_step,
                    year_end,
                    daysofyear,
                )

                if len(sharded_time_range) < total_num_steps_per_shard:
                    extra_steps_ = total_num_steps_per_shard - len(sharded_time_range)
                    assert extra_steps_ == 36 if calendar.isleap(year) else 40

                    ## wrap-around
                    base_time = pd.to_datetime(f"{year}-01-01")
                    extra_time_range = pd.date_range(
                        base_time,
                        base_time
                        + pd.to_timedelta(extra_steps_ * hrs_each_step, unit="h"),
                        freq=f"{hrs_each_step}h",
                        inclusive="left",
                    )

                for var in TqdmWithDepth(CONSTANT_VARS, desc="constant"):
                    if (var == "orography") and (var not in xa):
                        var = "geopotential_at_surface"
                    xdata = xa[var]
                    x = xdata.data
                    if var == "latitude":
                        x = np.repeat(x[np.newaxis, :], len(xa["longitude"]), axis=0)
                        x = np.repeat(
                            x[np.newaxis, :, :], total_num_steps_per_shard, axis=0
                        )
                    else:
                        x = np.repeat(
                            x[np.newaxis, :, :], total_num_steps_per_shard, axis=0
                        )
                    x = np.expand_dims(x, axis=1)
                    y = f[var]
                    assert isclose_with_nan(
                        x, y
                    ).all(), f"Not matching: {year} {shard_id} {var}"

                for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
                    xdata = xa[var].sel(time=sharded_time_range)
                    x = xdata.data
                    if len(x) < total_num_steps_per_shard:
                        extra = xa[var].sel(time=extra_time_range)
                        extra = extra.data
                        x = np.vstack((x, extra))
                    x = np.expand_dims(x, axis=1)
                    y = f[var]
                    assert isclose_with_nan(
                        x, y
                    ).all(), f"Not matching: {year} {shard_id} {var}"

                for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
                    for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
                        xdata = xa[var].sel(time=sharded_time_range, level=level)
                        x = xdata.data
                        if len(x) < total_num_steps_per_shard:
                            extra = xa[var].sel(time=extra_time_range, level=level)
                            extra = extra.data
                            x = np.vstack((x, extra))
                        x = np.expand_dims(x, axis=1)
                        y = f[f"{var}_{level}"]
                        assert isclose_with_nan(
                            x, y
                        ).all(), f"Not matching: {year} {shard_id} {var}"


def zarr2nc(
    xa,
    years,
    save_dir,
    partition,
    num_shards_per_year=8,
    hrs_each_step=6,
    extra_steps=40,
    executor=None,
    daysofyear=366,
):
    os.makedirs(os.path.join(save_dir, partition), exist_ok=True)

    assert 24 % hrs_each_step == 0
    num_steps_per_day = 24 // hrs_each_step
    num_steps_per_year = DAYS_PER_YEAR * num_steps_per_day
    num_steps_per_shard = num_steps_per_year // num_shards_per_year
    total_num_steps_per_shard = num_steps_per_shard + extra_steps

    for year in TqdmWithDepth(years, desc="year"):
        for shard_id in TqdmWithDepth(range(num_shards_per_year), desc="shard"):
            filename = os.path.join(save_dir, partition, f"{year}_{shard_id}.npz")
            if os.path.exists(filename):
                ## skip when already exists
                continue
            lockfile = os.path.join(save_dir, partition, f".{year}_{shard_id}.npz.lock")
            lock = SoftFileLock(lockfile, timeout=0)
            try:
                lock.acquire()
            except Timeout:
                ## someone is working
                continue

            np_vars = dict()

            year_end = years[-1]
            sharded_time_range = get_sharded_time_range(
                year,
                shard_id,
                num_steps_per_shard,
                extra_steps,
                hrs_each_step,
                year_end,
                daysofyear,
            )

            for var in TqdmWithDepth(CONSTANT_VARS, desc="constant"):
                if (var == "orography") and (var not in xa):
                    var = "geopotential_at_surface"
                xdata = xa[var]
                x = xdata.data
                if var == "latitude":
                    x = np.repeat(x[np.newaxis, :], len(xa["longitude"]), axis=0)
                    x = np.repeat(
                        x[np.newaxis, :, :], total_num_steps_per_shard, axis=0
                    )
                else:
                    x = np.repeat(
                        x[np.newaxis, :, :], total_num_steps_per_shard, axis=0
                    )
                x = np.expand_dims(x, axis=1)
                np_vars[var] = x
                # print("var:", var, year, np_vars[var].shape)

            if executor is not None:
                future_list = list()
                var_list = list()
                for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
                    f = executor.submit(
                        get_data,
                        xa,
                        var,
                        total_num_steps_per_shard,
                        hrs_each_step,
                        sharded_time_range=sharded_time_range,
                    )
                    future_list.append(f)
                    var_list.append(var)

                for var, f in TqdmWithDepth(
                    zip(var_list, future_list),
                    desc="single (concurrent)",
                    total=len(future_list),
                ):
                    x = f.result()
                    x = np.expand_dims(x, axis=1)
                    # print(f"{var}:", x.shape)
                    np_vars[var] = x
            else:
                for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
                    x = get_data(
                        xa,
                        var,
                        total_num_steps_per_shard,
                        hrs_each_step,
                        sharded_time_range=sharded_time_range,
                    )
                    x = np.expand_dims(x, axis=1)
                    np_vars[var] = x
                    # print("var:", var, year, np_vars[var].shape)

            if executor is not None:
                future_list = list()
                var_list = list()
                level_list = list()
                for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
                    for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
                        f = executor.submit(
                            get_data,
                            xa,
                            var,
                            total_num_steps_per_shard,
                            hrs_each_step,
                            sharded_time_range=sharded_time_range,
                            level=level,
                        )
                        future_list.append(f)
                        var_list.append(var)
                        level_list.append(level)

                for var, level, f in TqdmWithDepth(
                    zip(var_list, level_list, future_list),
                    desc="pressure (concurrent)",
                    total=len(future_list),
                ):
                    x = f.result()
                    x = np.expand_dims(x, axis=1)
                    np_vars[f"{var}_{level}"] = x

            else:
                for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
                    for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
                        x = get_data(
                            xa,
                            var,
                            total_num_steps_per_shard,
                            hrs_each_step,
                            sharded_time_range=sharded_time_range,
                            level=level,
                        )
                        x = np.expand_dims(x, axis=1)
                        np_vars[f"{var}_{level}"] = x
                        # print("var:", var, year, level, np_vars[f"{var}_{level}"].shape)

            for var in TqdmWithDepth(EXTRA_VARS, desc="extra"):
                x = get_data(
                    xa,
                    var,
                    total_num_steps_per_shard,
                    hrs_each_step,
                    sharded_time_range=sharded_time_range,
                )
                x = np.expand_dims(x, axis=1)
                np_vars[var] = x

            np_vars["hrs_each_step"] = hrs_each_step
            np_vars["num_steps_per_shard"] = num_steps_per_shard
            np_vars["extra_steps"] = extra_steps

            ## save
            da2np(np_vars)
            np.savez(
                os.path.join(save_dir, partition, f"{year}_{shard_id}.npz"), **np_vars
            )
            lock.release()


def zarr2nc_wb(
    xa,
    ds,
    save_dir,
):

    if "land_sea_mask" not in ds:
        ds["land_sea_mask"] = xa["land_sea_mask"]

    if "orography" not in ds:
        ds["orography"] = xa["geopotential_at_surface"]

    ## climatology
    climatology = dict()
    climatology_std = dict()

    ## Save original value, not mean nor std
    for var in TqdmWithDepth(CONSTANT_VARS, desc="climatology_wb"):
        xdata = ds[var]
        x = xdata.data
        if var == "latitude":
            x = np.repeat(x[np.newaxis, :], len(ds["longitude"]), axis=0)
            x = np.tile(x, [len(ds["hour"]), len(ds["dayofyear"]), 1, 1])
        if var in ["land_sea_mask", "orography"]:
            x = np.tile(x, [len(ds["hour"]), len(ds["dayofyear"]), 1, 1])
        x = np.expand_dims(x, axis=0)
        climatology[var] = x

    for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
        mean2d = ds[var].data
        mean2d = np.expand_dims(mean2d, axis=0)
        climatology[var] = mean2d

    for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
        for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
            mean2d = ds[var].sel(level=level)
            mean2d = np.expand_dims(mean2d, axis=0)
            climatology[f"{var}_{level}"] = mean2d

    da2np(climatology)
    print(os.path.join(save_dir, "climatology_wb.npz"))
    np.savez(os.path.join(save_dir, "climatology_wb.npz"), **climatology)
    np.savez(os.path.join(save_dir, "normalize_mean_wb.npz"), **climatology)

    climatology_std = dict()

    ## Save original value, not mean nor std
    for var in TqdmWithDepth(CONSTANT_VARS, desc="climatology_wb"):
        xdata = ds[var]
        x = xdata.data
        if var == "latitude":
            x = np.repeat(x[np.newaxis, :], len(ds["longitude"]), axis=0)
            x = np.tile(x, [len(ds["hour"]), len(ds["dayofyear"]), 1, 1])
        if var in ["land_sea_mask", "orography"]:
            x = np.tile(x, [len(ds["hour"]), len(ds["dayofyear"]), 1, 1])
        x = np.expand_dims(x, axis=0)
        climatology_std[var] = x

    for var in TqdmWithDepth(SINGLE_LEVEL_VARS, desc="single"):
        var_ = var + "_std"
        std2d = ds[var_].data
        std2d = np.expand_dims(std2d, axis=0)
        climatology_std[var] = std2d

    for var in TqdmWithDepth(PRESSURE_LEVEL_VARS, desc="pressure"):
        var_ = var + "_std"
        for level in TqdmWithDepth(DEFAULT_PRESSURE_LEVELS, desc="level"):
            std2d = ds[var_].sel(level=level)
            std2d = np.expand_dims(std2d, axis=0)
            climatology_std[f"{var}_{level}"] = std2d

    da2np(climatology_std)
    np.savez(os.path.join(save_dir, "normalize_std_wb.npz"), **climatology_std)

def mpihello():
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    processor_name = MPI.Get_processor_name()
    return (rank, size, processor_name)

@click.command()
@click.option("--source_file", type=str)
@click.option("--climatology_file", type=str)
@click.option("--save_dir", type=str)
@click.option("--start_train_year", type=int, default=1979)
@click.option("--start_val_year", type=int, default=2018)
@click.option("--start_test_year", type=int, default=2020)
@click.option("--end_year", type=int, default=2021)
@click.option("--num_shards", type=int, default=4)
@click.option("--use_dask", is_flag=True, default=False)
@click.option(
    "--tasks",
    type=str,
    help="Comma-separated list of task.",
    default="main,check,norm,clim,clim2",
)
@click.option("--max_workers", type=int, default=8)
@click.option("--parallel", type=str, default="mpi")
@click.option("--hrs_each_step", type=int, default=6)
@click.option("--extra_steps", type=int, default=40)
@click.option("--daysofyear", type=int, default=366)
def main(
    source_file,
    climatology_file,
    save_dir,
    start_train_year,
    start_val_year,
    start_test_year,
    end_year,
    num_shards,
    use_dask,
    tasks,
    max_workers,
    parallel,
    hrs_each_step,
    extra_steps,
    daysofyear,
):

    assert (
        start_val_year > start_train_year
        and start_test_year > start_val_year
        and end_year > start_test_year
    )

    if use_dask:
        client = Client(
            processes=True, threads_per_worker=2, n_workers=4, memory_limit="32GB"
        )
        print(client)
        print(client.dashboard_link)

    train_years = range(start_train_year, start_val_year)
    val_years = range(start_val_year, start_test_year)
    test_years = range(start_test_year, end_year)

    # xa = xr.open_zarr(source_file, chunks={"time": 100, "level": 1})
    # xa = xr.open_zarr(source_file, chunks='auto')
    xa = xr.open_zarr(source_file)
    if ("longitude" not in xa) and ("latitude" not in xa):
        xa = xa.rename({"lon": "longitude", "lat": "latitude"})

    global CONSTANT_VARS, EXTRA_VARS, SINGLE_LEVEL_VARS, PRESSURE_LEVEL_VARS, DEFAULT_PRESSURE_LEVELS

    ## handle for prism, daymet
    if (("prism" in source_file) or ("daymet" in source_file)) and ("era" not in source_file):
        CONSTANT_VARS = ["land_sea_mask", "latitude",]
        SINGLE_LEVEL_VARS = list(xa.data_vars)
        SINGLE_LEVEL_VARS.remove("land_sea_mask")
        PRESSURE_LEVEL_VARS = list()
        print("CONSTANT_VARS:", CONSTANT_VARS)
        print("SINGLE_LEVEL_VARS:", SINGLE_LEVEL_VARS)
        xa = xa.assign_coords(level=("level", DEFAULT_PRESSURE_LEVELS))
    
    elif "era5-daymet" in source_file:
        DEFAULT_PRESSURE_LEVELS = [500, 850]
        CONSTANT_VARS = ["land_sea_mask", "latitude", "orography"]
        SINGLE_LEVEL_VARS = ["prcp"]
        PRESSURE_LEVEL_VARS = [
            "geopotential",
            "u_component_of_wind",
            "v_component_of_wind",
            "temperature",
            "specific_humidity",
        ]
        print("CONSTANT_VARS:", CONSTANT_VARS)
        print("SINGLE_LEVEL_VARS:", SINGLE_LEVEL_VARS)
        print("PRESSURE_LEVEL_VARS:", PRESSURE_LEVEL_VARS)
        xa = xa.assign_coords(level=("level", DEFAULT_PRESSURE_LEVELS))

    nlon = len(xa.longitude)
    nlat = len(xa.latitude)
    if nlon % 2 == 1:
        xa = xa.isel(longitude=slice(0, nlon - 1))
    if nlat % 2 == 1:
        xa = xa.isel(latitude=slice(0, nlat - 1))
    
    ## lat is revered in 1959-2022-6h-1440x721.zarr
    if xa.longitude[0] > xa.longitude[1]:
        xa = xa.isel(longitude=slice(None, None, -1))
    if xa.latitude[0] > xa.latitude[1]:
        xa = xa.isel(latitude=slice(None, None, -1))

    xa = xa.transpose("time", "level", "longitude", "latitude")    

    if "orography" not in xa:
        if "geopotential_at_surface" in xa:
            xa["orography"] = xa["geopotential_at_surface"]

    doy = np.array(
        [x.astype("M8[h]").item().timetuple().tm_yday for x in xa.time.data],
        dtype=np.int32,
    )
    tod = np.array(
        [x.astype("M8[h]").item().timetuple().tm_hour for x in xa.time.data],
        dtype=np.int32,
    )
    xa["days_of_year"] = (("time"), doy)
    xa["time_of_day"] = (("time"), tod)

    if "%{gridshape}" in save_dir:
        save_dir = save_dir.replace(
            "%{gridshape}", f"{len(xa.longitude)}x{len(xa.latitude)}"
        )
    if "%{deg}" in save_dir:
        deg = np.diff(xa.longitude.data)[0]
        save_dir = save_dir.replace(
            "%{deg}", f"{deg:.01f}"
        )
    if "%{arcmin}" in save_dir:
        arcmin = np.diff(xa.longitude.data)[0] * 60
        save_dir = save_dir.replace(
            "%{arcmin}", f"{arcmin:.01f}"
        )
    os.makedirs(save_dir, exist_ok=True)
    print("save_dir:", save_dir)

    task_list = tasks.split(",")

    if parallel == "process":
        pool = ProcessPoolExecutor(max_workers=max_workers)
    elif parallel == "thread":
        pool = ThreadPoolExecutor(max_workers=max_workers)
    elif parallel == "mpi":
        pool = MPICommExecutor(MPI.COMM_WORLD)
    elif parallel == "serial":
        pool = nullcontext()

    if parallel == "mpi":
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()
        print ("#1: MPI rank, size:", rank, size)
        comm.Barrier()

    with pool as executor:
        if parallel == "mpi":
            comm = MPI.COMM_WORLD
            size = comm.Get_size()
            rank = comm.Get_rank()
            print ("#2: MPI rank, size:", rank, size)

            future_list = list()
            for i in range(size):
                f = executor.submit(mpihello)
                future_list.append(f)
            
            for f in future_list:
                print("MPI ready:", f.result())
                
        # Only master enters this block
        if (executor is not None) or (parallel == "serial"):
            if parallel == "mpi":
                print ("#3: Master?", rank, size)

            print(">>> main")
            kw = {
                "hrs_each_step": hrs_each_step,
                "extra_steps": extra_steps,
                "daysofyear": daysofyear,
                "executor": executor,
            }
            if "main" in task_list:
                zarr2nc(xa, test_years, save_dir, "test", num_shards, **kw)
                zarr2nc(xa, val_years, save_dir, "val", num_shards, **kw)
                zarr2nc(xa, train_years, save_dir, "train", num_shards, **kw)

                lat = xa["latitude"].data
                lon = xa["longitude"].data
                np.save(os.path.join(save_dir, "lat.npy"), lat)
                np.save(os.path.join(save_dir, "lon.npy"), lon)

            print(">>> check")
            if "check" in task_list:
                ## same for all: train, val, and test
                zarr2nc_check(xa, save_dir, **kw)

            print(">>> norm")
            if "norm" in task_list:
                zarr2nc_normalize(xa, test_years, save_dir, "test", num_shards, **kw)
                zarr2nc_normalize(xa, val_years, save_dir, "val", num_shards, **kw)
                zarr2nc_normalize(xa, train_years, save_dir, "train", num_shards, **kw)

            print(">>> clim")
            if "clim" in task_list:
                zarr2nc_climatology(xa, test_years, save_dir, "test", num_shards, **kw)
                zarr2nc_climatology(xa, val_years, save_dir, "val", num_shards, **kw)
                zarr2nc_climatology(
                    xa, train_years, save_dir, "train", num_shards, **kw
                )

            print(">>> clim2")
            if "clim2" in task_list:
                xb = xr.open_zarr(climatology_file)

                ## same for all: train, val, and test
                zarr2nc_wb(xa, xb, save_dir)

            print("Done.")


if __name__ == "__main__":
    main()
