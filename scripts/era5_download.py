import xarray as xr
import argparse
import os
from dask.diagnostics import ProgressBar

DEFAULT_PRESSURE_LEVELS = [
    # 50,
    # 100,
    # 150,
    200,
    # 250,
    # 300,
    # 400,
    500,
    # 600,
    # 700,
    850,
    # 925,
    # 1000,
]

def main(zarr_path, variable_list, outputdir, year=None, month=None):
    # Open Zarr dataset
    print(f"Opening Zarr dataset from {zarr_path}...")
    ds = xr.open_zarr(zarr_path, consolidated=True)

    # Check if the variable exists in the dataset
    for variable_name in variable_list:
        if variable_name not in ds:
            print(f"Error: Variable '{variable_name}' not found in the dataset.")
            print(f"Available variables: {list(ds.data_vars)}")
            return

    # Select the variable and subset the data
    print(f"Subsetting variable '{','.join(variable_list)}'...")
    if year is not None and month is not None:
        subset = ds.sel(time=f"{year}-{month}", level=DEFAULT_PRESSURE_LEVELS)
    elif year is not None:
        subset = ds.sel(time=f"{year}", level=DEFAULT_PRESSURE_LEVELS)
    else:
        subset = ds.sel(time=slice("1959", "2021"), level=DEFAULT_PRESSURE_LEVELS)
    subset = subset[variable_list]
    print(subset)

    # total_elements = subset[variable_name].size
    # dtype_size = subset[variable_name].dtype.itemsize
    # data_size_bytes = total_elements * dtype_size
    print(f"Estimated size: {subset.nbytes / 1024 / 1024 / 1024:.2f} GB")    

    # Save the subset to a new Zarr store
    basename = os.path.basename(zarr_path)
    filename, ext = os.path.splitext(basename)

    if year is not None and month is not None:
        subset_path = os.path.join(outputdir, f"{filename}_subset_{year}_{month}.zarr")
    elif year is not None:
        subset_path = os.path.join(outputdir, f"{filename}_subset_{year}.zarr")
    else:
        subset_path = os.path.join(outputdir, f"{filename}_subset.zarr")

    if os.path.exists(subset_path):
        print(f"Skip {subset_path}")
        return

    with ProgressBar():
        print(f"Saving subset to {subset_path}...")
        subset.to_zarr(subset_path, consolidated=True, mode="w")

if __name__ == "__main__":
    const_variables = ["geopotential_at_surface", "orography", "landcover"]
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
        "2m_temperature",
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "volumetric_soil_water_layer_1",
    ]

    parser = argparse.ArgumentParser()
    parser.add_argument("--variable_name", nargs='+', default=era5_selected_variables)
    parser.add_argument("--zarr_path", default="gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr")
    parser.add_argument("--outputdir", default="subset2")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()

    if not os.path.exists(args.outputdir):
        os.makedirs(args.outputdir, exist_ok=True)

    main(args.zarr_path, args.variable_name, args.outputdir, year=args.year, month=args.month)
    print("Done.")