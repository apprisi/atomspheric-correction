#!/usr/bin/#!/usr/bin/env python3
import time
import json


def convert_json():
    # """
    # Funciton:
    #       convert txt to json

    file = "/home/tq/data_pool/test_data/processed_list_sucess.txt"

    with open(file, "r") as fp:
        process_dict = fp.readlines()
    process_dict = process_dict[0].split(", ")
    print(len(process_dict))
    print(process_dict[0])
    print(process_dict[1])

    out_tq = "/home/tq/data_pool/test_data/landsat_list_20180718.json"

    with open(out_tq, "w") as fp8:
        json.dump(process_dict, fp8, ensure_ascii=False, indent=2)
        print("data2: Valid data total %s" % len(process_dict))


if __name__ == "__main__":

    start = time.time()
    convert_json()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
