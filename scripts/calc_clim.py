import numpy as np
import argparse
import glob
import os
from mpi4py import MPI
from tqdm import tqdm


skip_var_lsit = [
    "days_of_year",
    "time_of_day",
    "hrs_each_step",
    "num_steps_per_shard",
    "extra_steps",
]

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Check for NaNs in an NPZ file")
    parser.add_argument("inputdir", type=str, help="Path to the .npz file")
    parser.add_argument("--output", help="output", default="climatology.npz")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    is_first = True
    clim = dict()
    clim2 = dict()
    count = 0

    if rank == 0:
        files = glob.glob(os.path.join(args.inputdir, "**/????_*.*"), recursive=True)
        chunks = np.array_split(files, size)
    else:
        chunks = None

    local_files = comm.scatter(chunks, root=0)
    print(rank, "local_files:", local_files)
    assert len(local_files) > 0

    for file in tqdm(local_files, disable=rank > 0):
        if os.path.isfile(file):
            print(rank, "reading:", file)
            data = np.load(file)
            count += data[data.files[0]].shape[0]
            for var_name in tqdm(data.files, desc="variables", leave=False, disable=rank > 0):
                if var_name in skip_var_lsit:
                    continue

                val = data[var_name].astype(np.float64)
                if var_name not in clim:
                    clim[var_name] = np.sum(val, axis=0, dtype=np.float64)
                    clim2[var_name] = np.sum(val**2, axis=0, dtype=np.float64)
                else:
                    clim[var_name] = clim[var_name] + np.sum(
                        val, axis=0, dtype=np.float64
                    )
                    clim2[var_name] = clim2[var_name] + np.sum(
                        val**2, axis=0, dtype=np.float64
                    )
                # print("var:", var_name, clim[var_name].shape, clim2[var_name].sum()/val.size - (clim[var_name].sum()/val.size)**2, val.var())
                # print("var:", var_name, clim[var_name].shape, clim2[var_name].sum()/val.size - (clim[var_name].sum()/val.size)**2, val.var())

    clim_all = dict()
    clim2_all = dict()

    mean2d_dict = dict()
    mean_dict = dict()
    std_dict = dict()

    global_count = comm.reduce(count, op=MPI.SUM, root=0)
    if rank == 0:
        print("global_count:", global_count)

    for var in clim:
        if rank == 0:
            sum_arr = np.zeros_like(clim[var])
            sum2_arr = np.zeros_like(clim2[var])
        else:
            sum_arr = None
            sum2_arr = None

        comm.Reduce(clim[var], sum_arr, op=MPI.SUM, root=0)
        comm.Reduce(clim2[var], sum2_arr, op=MPI.SUM, root=0)

        if rank == 0:
            clim_all[var] = sum_arr
            clim2_all[var] = sum2_arr

            mean2d = clim_all[var] / global_count
            mean = np.expand_dims(
                clim_all[var].sum() / global_count / sum_arr.size, axis=0
            )
            std = np.expand_dims(
                np.sqrt(
                    clim2_all[var].sum() / global_count / sum_arr.size
                    - (clim_all[var].sum() / global_count / sum_arr.size) ** 2
                ),
                axis=0,
            )
            mean2d_dict[var] = mean2d.astype(np.float32)
            std_dict[var] = std.astype(np.float32)
            mean_dict[var] = mean.astype(np.float32)

            # print(mean2d.shape, mean.shape, std.shape)

            print("var:", var, mean2d.shape, mean2d.mean(), mean, std)
            # print("var:", var, clim_all[var].shape, clim_all[var].dtype, clim_all[var].mean(), clim2_all[var].mean() - (clim_all[var].mean())**2)

    np.savez("climatology.npz", **mean2d_dict)
    np.savez("normalize_mean.npz", **mean_dict)
    np.savez("normalize_std.npz", **std_dict)
