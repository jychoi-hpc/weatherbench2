import numpy as np
import argparse
import glob
import os
import sys

def check_npz_for_nans(file_path, fast=False, verbose=True):
    """Check if there are any NaNs in the variables of a .npz file."""

    status = 0
    # Load the .npz file
    try:
        print(file_path)
        data = np.load(file_path)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return 1

    if fast:
        return

    if not hasattr(data, "files"):
        print(len(data))
        return

    # Iterate over variables
    for var_name in data.files:
        variable = data[var_name]
        if verbose:
            print(f"{var_name}:", variable.shape, variable.mean(), variable.dtype)
        
        # Check for NaN values
        if np.isnan(variable).any():
            print(f"{var_name}  - Found NaNs", variable.shape)
            status += 1

    # Close the .npz file
    data.close()

    return status

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Check for NaNs in an NPZ file")
    parser.add_argument("file", type=str, help="Path to the .npz file")
    parser.add_argument("--fast", action='store_true', help="fast check")
    parser.add_argument("-v", "--verbose", action='store_true', help="verbose")
    parser.add_argument("-i", "--timeinfo", action='store_true', help="time info")
    args = parser.parse_args()

    if os.path.isfile(args.file):
        # Call the function to check the file
        status = check_npz_for_nans(args.file, args.fast, args.verbose)
    else:
        files = glob.glob(os.path.join(args.file, "**/*.*"), recursive=True)
        files = sorted(files)
        for file in files:
            if os.path.isfile(file):
                status += check_npz_for_nans(file, args.fast, args.verbose)    
    sys.exit(status)

