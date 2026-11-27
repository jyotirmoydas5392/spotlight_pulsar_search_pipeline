"""
Central configuration for the SPHERE / PULSELINE pipeline.

Defines the pipeline base directory (from the PULSELINE_VER1_DIR environment
variable) and registers all module directories on sys.path -- once.

Every script does:
    from pulseline_config import base_dir

This requires the pipeline root directory to be on PYTHONPATH, e.g. in the
same environment script that exports PULSELINE_VER1_DIR:
    export PULSELINE_VER1_DIR=/path/to/pipeline
    export PYTHONPATH=$PULSELINE_VER1_DIR:$PYTHONPATH
"""
import os
import sys
import logging

# Get the base directory from the environment variable
base_dir = os.getenv("PULSELINE_VER1_DIR")
if not base_dir:
    logging.error("Error: PULSELINE_VER1_DIR environment variable is not set.")
    sys.exit(1)

# Module directories (relative to base_dir) added to sys.path
required_paths = [
    "scripts",
    "SPOTLIGHT_PULSELINE",
    "input_file_dir_init",
    "SPOTLIGHT_PULSELINE/scripts",
    "input_file_dir_init/scripts",
]
for _relative_path in required_paths:
    _full_path = os.path.join(base_dir, _relative_path)
    if os.path.exists(_full_path) and _full_path not in sys.path:
        sys.path.insert(0, _full_path)
