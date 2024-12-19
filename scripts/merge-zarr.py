import xarray as xr
import os

if __name__ == "__main__":

    outdir = "/lustre/orion/world-shared/lrn036/jyc/frontier/weatherbench2/datasets/regrid"
    outfile = os.path.join(outdir, f"1979-2022-6h-360x181.zarr")
    isfirst = True
    ds_list = list()
    for year in range(1979, 2022):
        fname = os.path.join(outdir, f"{year}-6h-360x181.zarr")
        if os.path.exists(fname):
            print("Read:", fname)
            ds = xr.open_zarr(fname)
            ds_list.append(ds)
            if isfirst:
                ds.to_zarr(outfile, mode="w")
                isfirst = False
            else:
                ds.to_zarr(outfile, append_dim="time")

    # ds_concat = xr.concat(ds_list, dim="time")
    # ds_concat = ds_concat.unify_chunks()
    # print(ds_concat.chunks)

    # fname = os.path.join(outdir, f"1979-2022-6h-360x181.zarr")
    # ds_concat.to_zarr(fname, mode="w")
    # print(f"Saved: {fname}")

    # fname = os.path.join(outdir, f"1979-2022-6h-360x181.zarr")
    # for i, ds in enumerate(ds_list):
    #     print()
    #     if i == 0:
    #         ds.to_zarr(fname, mode="w")
    #     else:
    #         ds.to_zarr(fname, append_dim="time")

