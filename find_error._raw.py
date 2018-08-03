#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json


def find_error_raw():
    """
       find the error's raw data
    """

    # extract the process list\
    result_json = "/home/tq/data_pool/test_data/landsat/error_list.json.json"
    with open(result_json, "r") as fp:
        processed_list = json.load(fp)

    for path in processed_list:
        file_name = path.split("landsat_sr")[1]
        path_name = glob.glob(os.path.join("/home/tq/*", file_name))
        print(path_name)


if __name__ == "__main__":

    start = time.time()
    find_error_raw()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
