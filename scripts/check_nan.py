import numpy as np
import argparse

def check_npz_for_nans(file_path, fast=False):
    """Check if there are any NaNs in the variables of a .npz file."""
    # Load the .npz file
    print(file_path)
    data = np.load(file_path)

    if fast:
        return

    if not hasattr(data, "files"):
        print(len(data))
        return

    # Iterate over variables
    for var_name in data.files:
        variable = data[var_name]
        # print(f"{var_name}:", variable.shape)
        
        # Check for NaN values
        if np.isnan(variable).any():
            print(f"{var_name}  - Found NaNs", variable.shape)
    
    # Close the .npz file
    data.close()

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Check for NaNs in an NPZ file")
    parser.add_argument("file", type=str, help="Path to the .npz file")
    parser.add_argument("--fast", action='store_true', help="fast check")
    args = parser.parse_args()

    # Call the function to check the file
    check_npz_for_nans(args.file, args.fast)
