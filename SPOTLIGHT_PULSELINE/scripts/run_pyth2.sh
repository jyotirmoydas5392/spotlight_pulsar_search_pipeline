#!/bin/bash
set -e

export PYTHONNOUSERSITE=1
export PYTHONPATH=/lustre_archive/apps/tdsoft/psrchive_shared/site-packages:/lustre_archive/apps/tdsoft/usr/src/presto_old/lib/python${PYTHONPATH:+:$PYTHONPATH}
export LD_LIBRARY_PATH=/lustre_archive/apps/miniconda3/envs/rutvik-anuga/lib:/lustre_archive/apps/tdsoft/psrchive_shared/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

export CC=/lustre_archive/apps/tdsoft/conda/envs/py2_env/bin/x86_64-conda-linux-gnu-gcc
export CXX=/lustre_archive/apps/tdsoft/conda/envs/py2_env/bin/x86_64-conda-linux-gnu-g++

exec /lustre_archive/apps/tdsoft/conda/envs/py2_env/bin/python "$@"