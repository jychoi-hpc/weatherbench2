#!/bin/bash
#SBATCH -A LRN036
#SBATCH -J global
#SBATCH -N 1
#SBATCH -t 24:00:00

DATAROOT=/lustre/orion/lrn036/world-shared/jyc/frontier/weatherbench2

ARG1=$1 ## reserved
YEAR=$2
MONTH=$3
DAY_BEGIN=$4
DAY_END=$5
TIME_OPTION=
[ ! -z $YEAR ] && TIME_OPTION="--year=$YEAR"
[ ! -z $MONTH ] && TIME_OPTION="$TIME_OPTION --month=$MONTH"
[ ! -z $DAY_BEGIN ] && TIME_OPTION="$TIME_OPTION --day_begin=$DAY_BEGIN --day_end=$DAY_END"
echo "YEAR: $YEAR"
echo "MONTH: $MONTH"
echo "DAYS: $DAY_BEGIN $DAY_END"

for UNIT in 0.25_deg 0.0625_deg; do
  NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
  NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
  time python -u ./scripts/regrid.py \
    --input_path=datasets/era5/2020-2023-1h-1440x721.zarr \
    --output_path=datasets/regrid_era5-imerg/era5-imerg_%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
    --output_chunks="time=1" \
    --regridding_unit=$UNIT --longitude_nodes=$NLON --latitude_nodes=$((NLAT+1)) \
    --latitude_spacing=equiangular_with_poles \
    --regridding_method=bilinear \
    --runner=DirectRunner \
    $TIME_OPTION
done

# for UNIT in 0.25_deg 0.0625_deg; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/era5/1959-2022-1h-1440x721.zarr \
#     --output_path=datasets/regrid_era5-imerg/era5-imerg_%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$NLON --latitude_nodes=$((NLAT+1)) \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done


# for UNIT in 0.0625_deg; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   echo time python -u ./scripts/regrid.py \
#     --input_path=datasets/chirps/chirps-202007.zarr \
#     --output_path=datasets/chirps/chirps_%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$NLON --latitude_nodes=$((NLAT+1)) \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
#   echo time python -u ./scripts/regrid.py \
#     --input_path=datasets/persiann/persiann-202007.zarr \
#     --output_path=datasets/persiann/persiann_%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$NLON --latitude_nodes=$((NLAT+1)) \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done


# for UNIT in 1.0_deg 0.25_deg; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/era5/1959-2022-1h-1440x721.zarr \
#     --output_path=datasets/regrid_era5-imerg/era5-imerg_%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$NLON --latitude_nodes=$((NLAT+1)) \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done

# for UNIT in 3.75_arcmin 15.0_arcmin; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/era5/1959-2022-1h-1440x721.zarr \
#     --output_path=datasets/regrid_era5-usa/era5-usa_%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$((NLON/6)) --latitude_nodes=$((NLAT/6)) --usa \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done

# for UNIT in 1.0_deg 0.25_deg 0.0625_deg; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/IMERG/IMERG-1998-2025-1d-3600x1800.zarr \
#     --output_path=datasets/regrid_imerg/IMERG-%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$NLON --latitude_nodes=$((NLAT+1)) \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done

# # for DSET in ACCESS-CM2 BCC-CSM2-MR CNRM-ESM2-1 MPI-ESM1-2-HR MRI-ESM2-0 NorESM2-MM; do
# DSET=$1
# for UNIT in 10.0_arcmin 2.5_arcmin; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/gcm/RegCM-$DSET-1980-2015-1405x697.zarr \
#     --output_path=datasets/regrid_regcm_%{grid_shape}/$DSET-%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$((NLON/6)) --latitude_nodes=$((NLAT/6)) --usa \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done
# # done

# for DSET in MPI; do
# for UNIT in 15.0_arcmin 60.0_arcmin; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/cmip6/$DSET-usa-1980-2015-240x120.zarr \
#     --output_path=datasets/regrid_cmip6-usa_%{grid_shape}/$DSET-usa_%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$((NLON/6)) --latitude_nodes=$((NLAT/6)) --usa \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done
# done

# # for UNIT in 2.0_arcmin 0.5_arcmin; do
# for UNIT in 3.75_arcmin 15.0_arcmin; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/daymet/daymet_1980-2023-1d-7200x3600.zarr \
#     --output_path=datasets/regrid_daymet/daymet_%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$((NLON/6)) --latitude_nodes=$((NLAT/6)) --usa \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done

# # for UNIT in 1.0_deg; do
# for UNIT in 1.40625_deg; do
# # for UNIT in 5.625_deg; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/era5/1959-2022-1h-1440x721.zarr \
#     --output_path=datasets/regrid_$UNIT/era5-%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$NLON --latitude_nodes=$((NLAT+1)) \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done

# for UNIT in 0.25_deg 1.0_deg 1.40625_deg 5.625_deg; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/era5/1959-2022-1h-1440x721.zarr \
#     --output_path=datasets/regrid_era5/era5_%{yearmonday_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=1" \
#     --regridding_unit=$UNIT --longitude_nodes=$NLON --latitude_nodes=$((NLAT+1)) \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     $TIME_OPTION
# done

# for UNIT in 2.0_arcmin 0.5_arcmin; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/contants/landcover_43200x21600.zarr \
#     --output_path=datasets/contants/landcover-usa_%{grid_shape}-bilinear.zarr \
#     --regridding_unit=$UNIT --longitude_nodes=$((NLON/6)) --latitude_nodes=$((NLAT/6)) --usa \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner
# done

# # for UNIT in 2.0_arcmin 0.5_arcmin; do
# # for UNIT in 15.0_arcmin 60.0_arcmin; do
# for UNIT in 3.75_arcmin 15.0_arcmin; do
#   NLON=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(360/regrid_unit/4)*4)")
#   NLAT=$(python -c "from math import ceil; regrid_unit, unit = '$UNIT'.split('_'); regrid_unit = float(regrid_unit) if unit == 'deg' else float(regrid_unit)/60.0; print(ceil(180/regrid_unit/4)*4)")
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/contants/orography_43200x21600.zarr \
#     --output_path=datasets/contants/orography-usa_%{grid_shape}-bilinear.zarr \
#     --regridding_unit=$UNIT --longitude_nodes=$((NLON/6)) --latitude_nodes=$((NLAT/6)) --usa \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/contants/landcover_43200x21600.zarr \
#     --output_path=datasets/contants/landcover-usa_%{grid_shape}-bilinear.zarr \
#     --regridding_unit=$UNIT --longitude_nodes=$((NLON/6)) --latitude_nodes=$((NLAT/6)) --usa \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/contants/land_sea_mask_7200x3600.zarr/ \
#     --output_path=datasets/contants/land_sea_mask-usa_%{grid_shape}-bilinear.zarr \
#     --regridding_unit=$UNIT --longitude_nodes=$((NLON/6)) --latitude_nodes=$((NLAT/6)) --usa \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner
# done

# for LON in 64 256 360 1440 5760; do
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/contants/landcover_43200x21600.zarr \
#     --output_path=datasets/contants/landcover_%{grid_shape}-bilinear.zarr \
#     --longitude_nodes=$LON --latitude_nodes=$((LON/2+1)) \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner
# done

# for LON in 64 256 360 1440 3600 5760; do 
#   LAT=$((LON/2+1))
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/contants/era5-constant_1440x721.zarr \
#     --output_path=datasets/contants/era5-constant_%{grid_shape}-bilinear.zarr \
#     --longitude_nodes=$LON --latitude_nodes=$LAT \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner
# done

# for LON in 64 256 360 1440 5760; do
#   LAT=$((LON/2+1))
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/contants/orography_43200x21600.zarr \
#     --output_path=datasets/contants/orography_%{grid_shape}-bilinear.zarr \
#     --longitude_nodes=$LON --latitude_nodes=$LAT \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner
# done

# for DSET in MPI-ESM AWI-ESM HAMMOZ CMCC TaiESM1; do
# for NLON in 64 256; do
#   NLAT=$((NLON/2 + 1))
#   time python -u ./scripts/regrid.py \
#     --input_path=datasets/cmip6/$DSET-1980-2015-360x180.zarr \
#     --output_path=datasets/regrid/CMIP6-${DSET}_%{year_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=100" \
#     --longitude_nodes=$NLON --latitude_nodes=$NLAT \
#     --latitude_spacing=equiangular_with_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     --year=$YEAR
# done
# done

# time python -u ./scripts/regrid.py \
#   --input_path=datasets/orography/orography_43200x21600.zarr \
#   --output_path=datasets/orography/orography-usa_%{grid_shape}-bilinear.zarr \
#   --scale=1 --longitude_nodes=1440 --latitude_nodes=720 \
#   --latitude_spacing=equiangular_without_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner

# time python -u ./scripts/regrid.py \
#   --input_path=datasets/orography/orography_43200x21600.zarr \
#   --output_path=datasets/orography/orography-usa_%{grid_shape}-bilinear.zarr \
#   --scale=4 --longitude_nodes=360 --latitude_nodes=180 \
#   --latitude_spacing=equiangular_without_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner



# time python -u ./scripts/regrid.py \
#   --input_path=datasets/era5/1959-2022-6h-1440x721.zarr \
#   --output_path=datasets/regrid/era5_%{yearmon_range}-%{grid_shape}-bilinear.zarr \
#   --output_chunks="time=100" \
#   --scale=4 --longitude_nodes=360 --latitude_nodes=180 \
#   --latitude_spacing=equiangular_with_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner \
#   --year=$YEAR --month=$MONTH

# time python -u ./scripts/regrid.py \
#   --input_path=datasets/era5/1959-2022-6h-1440x721.zarr \
#   --output_path=datasets/regrid/era5_%{yearmon_range}-%{grid_shape}-bilinear.zarr \
#   --output_chunks="time=100" \
#   --scale=1 --longitude_nodes=1440 --latitude_nodes=720 \
#   --latitude_spacing=equiangular_with_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner \
#   --year=$YEAR --month=$MONTH

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
# time python -u ./scripts/regrid.py \
#   --input_path=datasets/daymet/daymet_1980-2023-1d-1405x697.zarr \
#   --output_path=datasets/regrid/daymet_%{yearmon_range}-%{grid_shape}-bilinear.zarr \
#   --output_chunks="time=100" \
#   --scale=$scale --longitude_nodes=$((1440/scale)) --latitude_nodes=$((720/scale)) \
#   --latitude_spacing=equiangular_without_poles \
#   --regridding_method=bilinear \
#   --runner=DirectRunner \
#   --year=$YEAR
# done
# done
# for gridshape in 1440x720 360x180; do
#     python scripts/merge-zarr.py \
#         datasets/regrid/daymet_*-$gridshape-bilinear.zarr \
#         datasets/orography/orography-usa_$gridshape-bilinear.zarr \
#         --outfile datasets/daymet/daymet_%{year_range}-1d-%{grid_shape}-bilinear.zarr
# done

# for scale in 1 4; do
# for YEAR in `seq 1981 2019`; do
#   python -u ./scripts/regrid.py \
#     --input_path=datasets/prism/prism_1981-2020-1d-1405x621.zarr \
#     --output_path=datasets/regrid/prism_%{yearmon_range}-%{grid_shape}-bilinear.zarr \
#     --output_chunks="time=100" \
#     --scale=$scale --longitude_nodes=$((1440/scale)) --latitude_nodes=$((720/scale)) \
#     --latitude_spacing=equiangular_without_poles \
#     --regridding_method=bilinear \
#     --runner=DirectRunner \
#     --year=$YEAR
# done
# done
# for gridshape in 1440x720 360x180; do
#     python scripts/merge-zarr.py \
#         datasets/regrid/prism_*-$gridshape-bilinear.zarr \
#         datasets/orography/orography-usa_$gridshape-bilinear.zarr \
#         --outfile datasets/prism/prism_%{year_range}-1d-%{grid_shape}-bilinear.zarr
# done
