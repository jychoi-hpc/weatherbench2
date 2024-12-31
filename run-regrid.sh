#!/bin/bash

DATAROOT=/lustre/orion/lrn036/world-shared/jyc/frontier/weatherbench2

YEAR=$1
[ -z $YEAR ] && YEAR=2021
echo "YEAR=$YEAR"

if [ ! -d datasets/regrid/${YEAR}-6h-256x128.zarr ]; then
  python ./scripts/regrid.py \
    --input_path=datasets/era5/1959-2022-6h-1440x721.zarr \
    --output_path=datasets/regrid/${YEAR}-6h-256x128.zarr \
    --output_chunks="time=100" \
    --latitude_nodes=128 \
    --longitude_nodes=256 \
    --latitude_spacing=equiangular_with_poles \
    --regridding_method=conservative \
    --runner=DirectRunner \
    --year=$YEAR
    # --year=$YEAR
    # --num_threads=16
    # --input_path=$DATAROOT/datasets/era5/1959-2022-6h-1440x721.zarr \
    # --output_path=$DATAROOT/datasets/regrid/1959-2022-6h-360x181.zarr \
    # --input_path=$DATAROOT/datasets/era5/1959-2022-6h-64x32_equiangular_conservative.zarr \
    # --output_path=$DATAROOT/datasets/regrid/tmp_${YEAR}.zarr \
fi

# YEAR=$1
# [ -z $YEAR ] && YEAR=2021
# echo "YEAR=$YEAR"

# if [ ! -d datasets/regrid/${YEAR}-6h-360x181.zarr ]; then
#   python ./scripts/regrid.py \
#     --input_path=datasets/era5/1959-2022-6h-1440x721.zarr \
#     --output_path=datasets/regrid/${YEAR}-6h-360x181.zarr \
#     --output_chunks="time=1" \
#     --latitude_nodes=181 \
#     --longitude_nodes=360 \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=conservative \
#     --runner=DirectRunner \
#     --year=$YEAR
#     # --num_threads=16
#     # --input_path=$DATAROOT/datasets/era5/1959-2022-6h-1440x721.zarr \
#     # --output_path=$DATAROOT/datasets/regrid/1959-2022-6h-360x181.zarr \
#     # --input_path=$DATAROOT/datasets/era5/1959-2022-6h-64x32_equiangular_conservative.zarr \
#     # --output_path=$DATAROOT/datasets/regrid/tmp_${YEAR}.zarr \
# fi

