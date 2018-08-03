#!/usr/bin/env python3
import os
import time
import shutil
import glob


def remove_empty_file():
    """
    Function:
        remove the empty file in tq-data  
    """
    # deleted the empty dir
    all_tmp = glob.glob(r"/home/tq/tq-data*/landsat_sr/*/01/*/*/*")
    print(len(all_tmp))
    for tmp in all_tmp:
        if (
            not os.listdir(tmp)
            or len([fps for fps in os.listdir(tmp) if "_sr_" in fps]) < 16
        ):
            shutil.rmtree(tmp)
            # os.rmdir(tmp)
    a_tmp = glob.glob(r"/home/tq/tq-data*/landsat_sr/*/01/*/*/*")
    print(len(a_tmp))


if __name__ == "__main__":

    start = time.time()
    remove_empty_file()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
