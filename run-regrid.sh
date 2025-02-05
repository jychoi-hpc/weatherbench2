#!/bin/bash
#SBATCH -A LRN036
#SBATCH -J global
#SBATCH -N 1
#SBATCH -t 24:00:00

DATAROOT=/lustre/orion/lrn036/world-shared/jyc/frontier/weatherbench2

YEAR=$1
MONTH=$2
echo "YEAR: $YEAR"
echo "MONTH: $MONTH"

# time python -u ./scripts/regrid.py \
#   --input_path=datasets/prism/prism_1981-2020-1d-1405x621.zarr \
#   --output_path=datasets/regrid/prism_$YEAR-%{grid_shape}-bilinear.zarr \
#   --output_chunks="time=100" \
#   --scale=4 --longitude_nodes=360 --latitude_nodes=180 \
#   --latitude_spacing=equiangular_without_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner \
#   --year=$YEAR

# time python -u ./scripts/regrid.py \
#   --input_path=datasets/daymet/daymet_1980-2023-1d-1405x697.zarr \
#   --output_path=datasets/regrid/daymet_$YEAR-%{grid_shape}-bilinear.zarr \
#   --output_chunks="time=100" \
#   --scale=4 --longitude_nodes=360 --latitude_nodes=180 \
#   --latitude_spacing=equiangular_without_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner \
#   --year=$YEAR

time python -u ./scripts/regrid.py \
  --input_path=datasets/era5/1959-2022-6h-1440x721.zarr \
  --output_path=datasets/regrid/era5_$YEAR-$MONTH-%{grid_shape}-bilinear.zarr \
  --output_chunks="time=100" \
  --scale=4 --longitude_nodes=360 --latitude_nodes=180 \
  --latitude_spacing=equiangular_with_poles \
  --regridding_method=bilinear \
  --runner=DirectRunner \
  --year=$YEAR --month=$MONTH

# time python -u ./scripts/regrid.py \
#   --input_path=datasets/era5/1959-2022-1h-1440x721.zarr \
#   --output_path=datasets/regrid/era5_$YEAR-$MONTH-1h-%{grid_shape}.zarr \
#   --output_chunks="time=100" \
#   --longitude_nodes=64 --latitude_nodes=33 \
#   --latitude_spacing=equiangular_with_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner \
#   --year=$YEAR

# time python -u ./scripts/regrid.py \
#   --input_path=datasets/era5/1959-2022-1h-1440x721.zarr \
#   --output_path=datasets/regrid/$YEAR-$MONTH-1h-%{grid_shape}.zarr \
#   --output_chunks="time=1" \
#   --longitude_nodes=360 --latitude_nodes=181 \
#   --latitude_spacing=equiangular_with_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner \
#   --year=$YEAR --month=$MONTH

# if [ ! -d datasets/regrid/${YEAR}-6h-256x128.zarr ]; then
#   python ./scripts/regrid.py \
#     --input_path=datasets/era5/1959-2022-6h-1440x721.zarr \
#     --output_path=datasets/regrid/${YEAR}-6h-256x128.zarr \
#     --output_chunks="time=100" \
#     --latitude_nodes=128 \
#     --longitude_nodes=256 \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     --year=$YEAR
# fi

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
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     --year=$YEAR
#     # --num_threads=16
#     # --input_path=$DATAROOT/datasets/era5/1959-2022-6h-1440x721.zarr \
#     # --output_path=$DATAROOT/datasets/regrid/1959-2022-6h-360x181.zarr \
#     # --input_path=$DATAROOT/datasets/era5/1959-2022-6h-64x32_equiangular_conservative.zarr \
#     # --output_path=$DATAROOT/datasets/regrid/tmp_${YEAR}.zarr \
# fi

# ## 64x33, 256x129
# time python -u ./scripts/regrid.py \
#   --input_path=datasets/era5/1959-2022-6h-1440x721.zarr \
#   --output_path=datasets/regrid/$YEAR-%{grid_shape}.zarr \
#   --output_chunks="time=100" \
#   --longitude_nodes=256 --latitude_nodes=129 \
#   --latitude_spacing=equiangular_with_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner \
#   --year=$YEAR

# ## 360x181, 1440x721
# time python -u ./scripts/regrid.py \
#   --input_path=datasets/era5/1959-2022-6h-1440x721.zarr \
#   --output_path=datasets/regrid/$YEAR-%{grid_shape}.zarr \
#   --output_chunks="time=1" \
#   --longitude_nodes=360 --latitude_nodes=181 \
#   --latitude_spacing=equiangular_with_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner \
#   --year=$YEAR

# # 1440x721
# for MONTH in `seq 12`; do
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/era5/1959-2022-6h-1440x721.zarr \
#     --output_path=datasets/regrid/$YEAR-$MONTH-%{grid_shape}.zarr \
#     --output_chunks="time=1" \
#     --longitude_nodes=1440 --latitude_nodes=721 \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     --year=$YEAR --month=$MONTH
# done

# for scale in 1 4; do
# for YEAR in `seq 1980 2022`; do
#   python -u ./scripts/regrid.py \
#     --input_path=datasets/daymet/daymet_1980-2023-1d-1405x697.zarr \
#     --output_path=datasets/regrid2/daymet_$YEAR-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=10" \
#     --scale=$scale \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     --year=$YEAR
# done
# done
# python scripts/merge-zarr.py datasets/regrid2/daymet_*-1400x697-bilinear.zarr --outfile datasets/daymet/daymet_1980-2023-1d-%{grid_shape}-bilinear.zarr --chunk_time 10
# python scripts/merge-zarr.py datasets/regrid2/daymet_*-350x175-bilinear.zarr --outfile datasets/daymet/daymet_1980-2023-1d-%{grid_shape}-bilinear.zarr --chunk_time 10

# for scale in 1 4; do
# for YEAR in `seq 1981 2019`; do
#   python -u ./scripts/regrid.py \
#     --input_path=datasets/prism/prism_1981-2020-1d-1405x621.zarr \
#     --output_path=datasets/regrid2/prism_$YEAR-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=10" \
#     --scale=$scale \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     --year=$YEAR
# done
# done
# python scripts/merge-zarr.py datasets/regrid2/prism_*-1400x617-bilinear.zarr --outfile datasets/prism/prism_1981-2020-1d-%{grid_shape}-bilinear.zarr --chunk_time 10
# python scripts/merge-zarr.py datasets/regrid2/prism_*-350x155-bilinear.zarr --outfile datasets/prism/prism_1981-2020-1d-%{grid_shape}-bilinear.zarr --chunk_time 10
