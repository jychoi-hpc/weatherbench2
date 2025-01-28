#!/bin/bash

module reset
module load PrgEnv-gnu
module unload darshan-runtime

source /sw/frontier/miniforge3/23.11.0-0/bin/activate
conda activate /lustre/orion/world-shared/lrn036/jyc/frontier/sw/envs/climax-py3.12
which python

export PATH=/lustre/orion/lrn036/world-shared/jyc/frontier/sw/google-cloud-sdk/bin:$PATH

# python -u climatology_simple.py --grid=256x128 --varname $1
python -u climatology_simple.py --grid=360x181 --chunk_size=32 --varname $1
