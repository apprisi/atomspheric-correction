
#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json
import shutil


def find_error_raw():
    """
       find the error data
    """

    # extract the process list\
    result_json = "/home/jason/data_pool/sample_data/processed_list.json"
    with open(result_json, "r") as fp:
        processed_list = json.load(fp)

    le07_list = processed_list["LE07"]["Processed_list"]["2017"]
    print("total processed list: %d" % len(le07_list))

    le07_list_path = [dp[dp.find("landsat_sr") + 11 :] for dp in le07_list]
    print(le07_list_path[0])
    print("path total : %d" % len(le07_list_path))

    le07_set = set(le07_list_path)  # to set
    print("convert set: %d" % len(le07_set))

    # find the same data
    a = {}
    for i in le07_list_path:
        if le07_list_path.count(i) == 2:
            a[i] = le07_list_path.count(i)

    # copy the same to list
    L = ["/home/jason/tq-data04/landsat_sr/" + key for key in a.keys()]
    print("total: %d ,test: %s" % (len(L), L[0]))
    for tmp in L:
        shutil.rmtree(tmp)

    # process_json = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2017_le07_final.json'
    # with open(process_json, 'r') as fp:
    #     process_list = json.load(fp)

    # print("process list: %d" % len(process_list))
    # le07_process_list = [dp[dp.find('landsat')+8:-1] for dp in process_list]
    # print(le07_process_list[0])
    # process_set = set(le07_process_list)

    # print("process set: %d" % len(process_set))

    # diff =  le07_set - process_set
    # print(len(diff))
    # # for tmp in diff:
    # #     print(tmp)


if __name__ == "__main__":

    start = time.time()
    find_error()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
