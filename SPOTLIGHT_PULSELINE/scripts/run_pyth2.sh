#!/bin/bash
set -e

export PYTHONNOUSERSITE=1
export PYTHONPATH=/usr/local/lib/python2.7/site-packages:/lustre_archive/apps/tdsoft/usr/src/presto_old/lib/python
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

exec /lustre_archive/apps/tdsoft/conda/envs/py2_env/bin/python "$@"