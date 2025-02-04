import numpy as np
import glob
import argparse
import os
import shutil
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from filelock import FileLock, SoftFileLock, Timeout
import sys

def conv_npz(npz_obj):
    npz_obj = dict(npz_obj)
    if "latitude" in npz_obj:
        npz_obj["lattitude"] = npz_obj["latitude"]
        del npz_obj["latitude"]

    extra_steps = 0
    if "extra_steps" in npz_obj:
        extra_steps = npz_obj["extra_steps"].item()
        del npz_obj["extra_steps"]

    for k in npz_obj:
        dims = npz_obj[k].shape
        if len(dims) > 1:
            lon, lat = dims[-2], dims[-1]

            ## swap lon-lat dimension
            if lon > lat:
                npz_obj[k] = np.swapaxes(npz_obj[k], -2, -1)
            if lat % 2 == 1:
                lat = lat - 1
            ## remove extra steps
            if len(dims) == 4:
                n = dims[0]
                npz_obj[k] = npz_obj[k][:n-extra_steps,:,:lat,:lon]
            else:
                npz_obj[k] = npz_obj[k][...,:lat,:lon]

    return npz_obj

def dojob(input_filename, output_filename):
    processed = False
    if os.path.exists(output_filename):
        ## skip as it is already done
        return output_filename, processed
    
    dirname = os.path.dirname(output_filename)
    basename = os.path.basename(output_filename)
    lock_path = os.path.join(dirname, "." + basename + ".lock")
    lock = SoftFileLock(lock_path, timeout=0)
    try:
        with lock:
            if os.path.splitext(input_filename)[1] == ".npz":
                f = np.load(input_filename)
                f = conv_npz(f)            
                np.savez(output_filename, **f)
            elif input_filename == "lat.npy":
                lat = np.load(input_filename)
                nlat = len(lat)
                if nlat % 2 == 1:
                    lat = lat[:nlat-1]
                with open(output_filename, "wb") as f:
                    np.save(f, lat)
            else:
                shutil.copyfile(input_filename, output_filename)
            processed = True
    except Timeout:
        pass
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    return output_filename, processed

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir")
    parser.add_argument("output_dir")
    parser.add_argument("--max_workers", type=int, default=8)
    args = parser.parse_args()
    input_dir = args.input_dir
    output_dir = args.output_dir

    if args.max_workers == 0:
        for file in glob.glob("**/*.*", root_dir=input_dir, recursive=True):
            input_filename = os.path.join(input_dir, file)
            output_filename = os.path.join(output_dir, file)
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            dojob(input_filename, output_filename)
    else:
        with ProcessPoolExecutor(
            max_workers=args.max_workers
        ) as executor:
            future_list = list()
            for file in glob.glob("**/*.*", root_dir=input_dir, recursive=True):
                input_filename = os.path.join(input_dir, file)
                output_filename = os.path.join(output_dir, file)
                os.makedirs(os.path.dirname(output_filename), exist_ok=True)

                future = executor.submit(dojob, input_filename, output_filename)
                future_list.append(future)

            # for future in tqdm(concurrent.futures.as_completed(futures), total=len(future_list)):
            for future in tqdm(future_list):
                res = future.result()
                # print(res)




