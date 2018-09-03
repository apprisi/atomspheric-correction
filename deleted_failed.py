#!/usr/bin/#!/usr/bin/env python3
import os
import time
import json
import shutil


def deleted_failed():
    """
       deledted failed data
    """

    # extract the process list\
    result_json = (
        "/home/tq/data_pool/test_data/landsat_20180811/processed_list_fail.json"
    )
    with open(result_json, "r") as fp:
        processed_failed = json.load(fp)

    processed_failed = ["/home/tq/" + f for f in processed_failed]

    print(len(processed_failed))
    count = 0
    for tmp_path in processed_failed:
        if os.path.exists(tmp_path):
            count += 1
            print(count)
            shutil.rmtree(tmp_path)


if __name__ == "__main__":

    start = time.time()
    deleted_failed()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
