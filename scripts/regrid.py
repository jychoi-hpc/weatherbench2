# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
r"""Run WeatherBench 2 regridding pipeline.

Only rectalinear grids (one dimensional lat/lon coordinates) on the input Zarr
file are supported, but irregular spacing is OK.

Example Usage:
  ```
  export BUCKET=my-bucket
  export PROJECT=my-project
  export REGION=us-central1

  python scripts/regrid.py \
    --input_path=gs://weatherbench2/datasets/era5/1959-2022-6h-1440x721.zarr \
    --output_path=gs://$BUCKET/datasets/era5/$USER/1959-2022-6h-64x33.zarr \
    --output_chunks="time=100" \
    --longitude_nodes=64 \
    --latitude_nodes=33 \
    --latitude_spacing=equiangular_with_poles \
    --regridding_method=conservative \
    --runner=DataflowRunner \
    -- \
    --project=$PROJECT \
    --region=$REGION \
    --temp_location=gs://$BUCKET/tmp/ \
    --setup_file=./setup.py \
    --requirements_file=./scripts/dataflow-requirements.txt \
    --job_name=regrid-$USER
  ```
"""
from absl import app
from absl import flags
import apache_beam as beam
import numpy as np
from weatherbench2 import flag_utils
from weatherbench2 import regridding
import xarray_beam
import pandas as pd
import os
from tqdm import tqdm
from dask.diagnostics import ProgressBar
import xarray as xr
from filelock import FileLock, SoftFileLock, Timeout

INPUT_PATH = flags.DEFINE_string("input_path", None, help="zarr inputs")
OUTPUT_PATH = flags.DEFINE_string("output_path", None, help="zarr outputs")
OUTPUT_CHUNKS = flag_utils.DEFINE_chunks(
    "output_chunks", "", help="desired chunking of output zarr"
)
LATITUDE_NODES = flags.DEFINE_integer(
    "latitude_nodes", None, help="number of desired latitude nodes"
)
LONGITUDE_NODES = flags.DEFINE_integer(
    "longitude_nodes", None, help="number of desired longitude nodes"
)
REGRIDDING_SCALE = flags.DEFINE_integer("scale", None, help="regridding_scale")
LATITUDE_SPACING = flags.DEFINE_enum(
    "latitude_spacing",
    "equiangular_with_poles",
    ["equiangular_with_poles", "equiangular_without_poles"],
    help="desired latitude spacing",
)
REGRIDDING_METHOD = flags.DEFINE_enum(
    "regridding_method",
    "conservative",
    ["nearest", "bilinear", "conservative"],
    help="regridding method",
)
LATITUDE_NAME = flags.DEFINE_string(
    "latitude_name", "latitude", help="Name of latitude dimension in dataset"
)
LONGITUDE_NAME = flags.DEFINE_string(
    "longitude_name", "longitude", help="Name of longitude dimension in dataset"
)
NUM_THREADS = flags.DEFINE_integer(
    "num_threads",
    None,
    help="Number of chunks to read/write in parallel per worker.",
)
RUNNER = flags.DEFINE_string("runner", None, "beam.runners.Runner")
YEAR = flags.DEFINE_integer("year", None, help="year")
MONTH = flags.DEFINE_integer("month", None, help="month")
FILLNA = flags.DEFINE_bool("fillna", False, "fillna")


class ProgressDoFn(beam.DoFn):
    """A custom DoFn to track progress with tqdm."""

    def __init__(self, total_chunks):
        self.total_chunks = total_chunks
        self.progress_bar = None

    def setup(self):
        # Initialize tqdm once per worker
        if self.progress_bar is None:
            self.progress_bar = tqdm(
                total=self.total_chunks,
                desc="Processing Chunks",
                position=0,
                leave=True,
            )

    def process(self, element):
        # Update progress bar
        self.progress_bar.update(1)
        yield element

    def teardown(self):
        # Close tqdm when done
        if self.progress_bar is not None:
            self.progress_bar.close()
            self.progress_bar = None


def main(argv):
    source_ds, input_chunks = xarray_beam.open_zarr(INPUT_PATH.value)
    if YEAR.value is not None:
        source_ds = source_ds.sel(
            time=slice(f"{YEAR.value}-01-01", f"{YEAR.value}-12-31")
        )
        if MONTH.value is not None:
            source_ds = source_ds.sel(
                time=slice(
                    f"{YEAR.value}-{MONTH.value:02d}", f"{YEAR.value}-{MONTH.value:02d}"
                )
            )
    print("source_ds:", source_ds)
    print("input_chunks:", input_chunks)

    # Rename latitude/longitude names
    renames = {
        LONGITUDE_NAME.value: "longitude",
        LATITUDE_NAME.value: "latitude",
    }
    source_ds = source_ds.rename(renames)
    input_chunks = {renames.get(k, k): v for k, v in input_chunks.items()}

    # Lat/lon must be single chunk for regridding.
    input_chunks["longitude"] = -1
    input_chunks["latitude"] = -1

    us_bounds = (24, 53, 235, 293.5)  # (lat_min, lat_max, lon_min, lon_max)
    us_lat_min, _, us_lon_min, _ = us_bounds

    # if "prism" not in INPUT_PATH.value and "daymet" not in INPUT_PATH.value:
    if False:
        ## (2025/01) temporarily disable
        if LATITUDE_SPACING.value == "equiangular_with_poles":
            lat_start = -90
            lat_stop = 90
        else:
            assert LATITUDE_SPACING.value == "equiangular_without_poles"
            lat_start = -90 + 0.5 * 180 / LATITUDE_NODES.value
            lat_stop = 90 - 0.5 * 180 / LATITUDE_NODES.value

        old_lon = source_ds.coords["longitude"].data
        old_lat = source_ds.coords["latitude"].data

        new_lon = np.linspace(0, 360, num=LONGITUDE_NODES.value, endpoint=False)
        new_lat = np.linspace(
            lat_start, lat_stop, num=LATITUDE_NODES.value, endpoint=True
        )
    else:
        ## prism, daymet
        ## convert to ERA5 longitude 0-360
        if (source_ds.longitude < 0).any():
            source_ds = source_ds.assign_coords(
                longitude=(source_ds.longitude + 360) % 360
            )

        ## Filter variables
        const_variables = ["geopotential_at_surface"]
        prism_selected_variables = ["land_sea_mask", "prcp", "tmax", "tmin"]
        era5_selected_variables = [
            "u_component_of_wind",
            "v_component_of_wind",
            "temperature",
            "specific_humidity",
            "geopotential",
            "sea_surface_temperature",
            "geopotential_at_surface",
            "2m_temperature",
            "total_precipitation",
            "total_precipitation_24hr",
        ]
        era5_selected_levels = [500, 850]

        data_variables = list(source_ds.data_vars)
        if "prism" in INPUT_PATH.value or "daymet" in INPUT_PATH.value:
            selected_variables = list(
                set(data_variables) & set(prism_selected_variables)
            )
        else:
            selected_variables = list(
                set(data_variables) & set(era5_selected_variables)
            )
        source_ds = source_ds[selected_variables]

        ## Filter levels
        if "level" in source_ds.coords:
            source_ds = source_ds.sel(level=era5_selected_levels)

        if "total_precipitation_24hr" in source_ds:
            assert "total_precipitation" not in source_ds
            source_ds = source_ds.rename(
                {"total_precipitation_24hr": "total_precipitation"}
            )

        ## handle nan values in sea_surface_temperature
        if "sea_surface_temperature" in source_ds:
            source_ds["sea_surface_temperature"] = source_ds[
                "sea_surface_temperature"
            ].combine_first(source_ds["2m_temperature"])

        ## Filter hours
        # source_ds = source_ds.sel(time=source_ds.time.dt.hour == 0)
        # source_ds = source_ds.resample(time="1D").mean() ## slow
        # source_ds = source_ds.groupby(source_ds.time.dt.floor("D")).mean() ## time var changes
        hourly = (source_ds.time[1] - source_ds.time[0]).item() / 3600 / 1e9
        assert hourly.is_integer()
        hourly = int(hourly)
        if hourly < 24:
            samples_per_day = 24 // hourly
            source_ds = source_ds.coarsen(time=samples_per_day).mean()
            source_ds["time"] = source_ds.time.dt.floor("D")

        old_lon = source_ds.coords["longitude"].data
        old_lat = source_ds.coords["latitude"].data

        regrid_scale = REGRIDDING_SCALE.value
        lon_start = us_lon_min
        lon_interval = regrid_scale * 2.5 / 60  # 2.5 minutes in degrees
        new_lon = lon_start + lon_interval * np.arange(LONGITUDE_NODES.value)

        lat_start = us_lat_min
        lat_interval = regrid_scale * 2.5 / 60  # 2.5 minutes in degrees
        new_lat = lat_start + lat_interval * np.arange(LATITUDE_NODES.value)

        print("lon start:", lon_start)
        print("lat start:", lat_start)
        print("interval:", lon_interval * 60, lat_interval * 60)
        print("gridshape:", f"{len(new_lon)}x{len(new_lat)}")

    output_chunks = OUTPUT_CHUNKS.value
    print("OUTPUT_CHUNKS:", repr(output_chunks))

    output_path = OUTPUT_PATH.value
    if "%{grid_shape}" in output_path:
        output_path = output_path.replace(
            "%{grid_shape}", f"{len(new_lon)}x{len(new_lat)}"
        )

    print("output_path:", output_path)
    if os.path.exists(output_path):
        print("Skip:", output_path)
        return

    dirname = os.path.dirname(output_path)
    basename = os.path.basename(output_path)
    lockfile = os.path.join(dirname, "." + basename + ".lock")
    lock = SoftFileLock(lockfile, timeout=0)
    try:
        lock.acquire()
    except Timeout:
        ## someone is working
        return

    regridder_cls = {
        "nearest": regridding.NearestRegridder,
        "bilinear": regridding.BilinearRegridder,
        "conservative": regridding.ConservativeRegridder,
    }[REGRIDDING_METHOD.value]

    source_grid = regridding.Grid.from_degrees(lon=old_lon, lat=np.sort(old_lat))
    target_grid = regridding.Grid.from_degrees(lon=new_lon, lat=new_lat)
    regridder = regridder_cls(source_grid, target_grid)

    template = (
        xarray_beam.make_template(source_ds)
        .isel(longitude=0, latitude=0, drop=True)
        .expand_dims(longitude=new_lon, latitude=new_lat)
        .transpose(..., "longitude", "latitude")
    )

    chunked_ds = source_ds.chunk(input_chunks)
    # Calculate total number of chunks
    total_chunks = sum(
        np.prod([len(c) for c in da.chunks]) for da in chunked_ds.data_vars.values()
    )
    print(f"Total number of chunks across all variables: {total_chunks}")

    with ProgressBar():
        with beam.Pipeline(runner=RUNNER.value, argv=argv) as root:
            _ = (
                root
                | xarray_beam.DatasetToChunks(
                    source_ds,
                    input_chunks,
                    split_vars=True,
                    num_threads=NUM_THREADS.value,
                )
                | "Progress" >> beam.ParDo(ProgressDoFn(total_chunks))
                | "Regrid"
                >> beam.MapTuple(lambda k, v: (k, regridder.regrid_dataset(v)))
                | xarray_beam.ConsolidateChunks(output_chunks)
                | xarray_beam.ChunksToZarr(
                    output_path,
                    template,
                    output_chunks,
                    num_threads=NUM_THREADS.value,
                )
            )

        # ds = xr.open_zarr(output_path)
        # print("NULL?:", ds.isnull().any().compute())

    lock.release()


if __name__ == "__main__":
    from dask.distributed import Client

    # Create a Dask client
    client = Client()
    print(f"Number of Dask workers: {len(client.nthreads())}")

    app.run(main)
