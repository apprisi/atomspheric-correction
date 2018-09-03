#!/usr/bin/#!/usr/bin/env python3
import glob
import time
import json
import os


def find_name_error():
    """
       find name error data
    """
    server_list = ["tq-data01", "tq-data02", "tq-data03", "tq-data04"]
    landsat_list = ["LC08", "LE07", "LT05"]

    falied_list = []
    for s in server_list:
        for l in landsat_list:
            falied_list += glob.glob(
                "/home/tq/" + s + "/landsat_sr/" + l + "/01/*/*/*/_sr_band1.tif"
            )
    print(len(falied_list))

    out_file = "/home/tq/data_pool/test_data/landsat_failed_name.json"
    with open(out_file, "w") as fp:
        json.dump(falied_list, fp, ensure_ascii=False, indent=2)


def rename_error():
    out_file = "/home/tq/data_pool/test_data/landsat_failed_name.json"
    with open(out_file, "r") as fp:
        process_list = json.load(fp)
    process_list = [os.path.split(f)[0] for f in process_list]

    count = 0
    for tmp_path in process_list:
        count += 1
        print(f"------> now process file {count} : {tmp_path}")
        files = sorted(os.listdir(tmp_path))
        for tmp_file in files:
            file_old_name = os.path.join(tmp_path, tmp_file)
            file_new_name = os.path.join(
                tmp_path, os.path.split(tmp_path)[1] + tmp_file
            )
            os.rename(file_old_name, file_new_name)


if __name__ == "__main__":

    start = time.time()
    find_name_error()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
