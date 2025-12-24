import xarray as xr
import numpy as np

def arcmin_to_km(arcmin, distance_km=6371):
    """Convert arcminutes to km based on observer's distance."""
    radians = (arcmin / 60) * (np.pi / 180)  # Convert arcmin to radians
    arc_length = radians * distance_km  # Compute arc length in km
    return arc_length
    
if __name__ == '__main__':
    ## orography
    # ds = xr.open_dataset("datasets/contants/GTOPO_DEM_30s.nc")
    # ds = ds.rename({"lat": "latitude", "lon": "longitude", "z": "orography"})
    # ds["orography"] = ds["orography"].astype(np.float32)
    # ds.to_zarr("datasets/orography/orography_43200x21600.zarr", mode="w")

    ## landuse
    # ds = xr.open_dataset("datasets/contants/GLCC_BATS_30s.nc")
    # ds = ds.rename({"lat": "latitude", "lon": "longitude", "landcover": "landcover"})
    # ds["landcover"] = ds["landcover"].astype(np.float32)
    # ds.to_zarr("datasets/contants/landcover_43200x21600.zarr", mode="w")

    ## extract all
    ds = xr.open_zarr("datasets/daymet/daymet_1980-2023-1d-360x180-bilinear.zarr")
    ds = ds[["land_sea_mask", "landcover", "orography"]]
    ds.to_zarr("datasets/contants/daymet-constant-360x180-bilinear.zarr", mode="w")
    print("Done.")

    ds = xr.open_zarr("datasets/daymet/daymet_1980-2023-1d-1440x720-bilinear.zarr")
    ds = ds[["land_sea_mask", "landcover", "orography"]]
    ds.to_zarr("datasets/contants/daymet-constant-1440x720-bilinear.zarr", mode="w")
    print("Done.")
