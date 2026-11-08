import os
import shutil
import argparse
from multiprocessing import Pool

def remove_path(path):
    """Remove a file or directory."""
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
            print("Removed directory: %s" % path)
        elif os.path.isfile(path):
            os.remove(path)
            print("Removed file: %s" % path)
    except Exception as e:
        print("Error removing %s: %s" % (path, e))


def clean_directory_parallel(target_dir, workers):
    """
    Remove all files and subdirectories under target_dir using multiprocessing.
    If target_dir does not exist, create it.
    """

    if not os.path.exists(target_dir):
        print("%s does not exist. Creating directory." % target_dir)
        os.makedirs(target_dir)
        return

    if not os.path.isdir(target_dir):
        print("Error: %s is not a directory." % target_dir)
        return

    items_to_remove = [
        os.path.join(target_dir, item)
        for item in os.listdir(target_dir)
    ]

    if not items_to_remove:
        print("No files or directories to remove in %s." % target_dir)
        return

    pool = Pool(processes=workers)
    try:
        pool.map(remove_path, items_to_remove)
    finally:
        pool.close()
        pool.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parallel clean-up of directory contents."
    )
    parser.add_argument(
        "target_dir",
        type=str,
        help="Target directory whose contents will be cleaned."
    )
    parser.add_argument(
        "workers",
        type=int,
        help="Number of parallel worker processes."
    )

    args = parser.parse_args()
    clean_directory_parallel(args.target_dir, args.workers)