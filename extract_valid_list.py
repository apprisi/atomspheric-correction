#!/usr/bin/#!/usr/bin/env python3
import os
import time
import json
import gc
import xlrd


def extract_vaild_path(imput_json):
    # """
    # Funciton:
    #     1. when cloud is more than 80, remove the data

    # input:
    #     input_json: json file path, such as '/home/tq/data_pool/2017-le07.json'

    # output:
    #     valid_path: json file path, such as '/home/tq/data_pool/2017-le07_valid.json'

    # """

    # input process
    if not os.path.exists(imput_json):
        print("%s file path does not exist!\n" % imput_json)
        return -1
    elif not os.path.isfile(imput_json):
        print("%s is not a file.\n" % imput_json)
        return -1
    else:
        print("extracting the data.......\n")

    output = "/home/tq/data_pool/test_data/valid_list_2018_USA.json"

    if not os.path.exists(imput_json):
        print("%s file path does not exist!\n" % imput_json)
        return -1
    elif not os.path.isfile(imput_json):
        print("%s is not a file.\n" % imput_json)
        return -1
    else:
        print("extracting the data.......\n")

        with open(imput_json, "r") as fp:
            process_dict = json.load(fp)

        # extra path to list and remove cloudiness more than 80%
        valid_list = []
        for tmp_data in process_dict["scenes"]:
            if tmp_data["cloud_perc"] <= 80:
                print(tmp_data["relative_path"] + "add to the list")
                valid_list.append(tmp_data["relative_path"])
            else:
                print(tmp_data["relative_path"] + "cloudiness more than 80%")
                continue

        # sort the list
        valid_list.sort()

        if len(valid_list) > 0:
            with open(output, "w") as fp:
                json.dump(valid_list, fp, ensure_ascii=False, indent=2)
            print("Valid data total %s in %s" % (len(valid_list), imput_json))
        else:
            print("no data in %s" % imput_json)
    return 1


def extract_corp_pr() -> str:
    """
        extract crop mask pr
    """
    index_list = []  # only save the corn belt

    # load the index 0f corn belt
    xls_file = "/home/tq/workspace/super-octo-bassoon/Cropmask_landsat.xlsx"
    book = xlrd.open_workbook(xls_file)
    sheet1 = book.sheet_by_name("Sheet1")
    index_tmp = sheet1.col_values(7)
    index_list = index_tmp[1:]
    index_list = [
        "{:0>6}".format(int(pr)) for pr in index_list
    ]  # 26034.0 convert to '026034'
    print(index_list)
    del (book)
    del (sheet1)
    gc.collect()
    return index_list


def extract_list():
    """
        extract the data by server
    """

    # set the root path
    json_l = "/home/tq/data_pool/test_data/valid_list_2018_USA.json"

    # find the file landsat
    process_list = []
    with open(json_l, "r") as fp:
        process_list = json.load(fp)
    print(len(process_list))

    # find the path in pr
    process_list_pr = []
    for tmp in process_list:
        tmp_pr = "".join(tmp.split("/")[4:6])

        if tmp_pr in pr:
            print(tmp_pr)
            process_list_pr.append(tmp)
        else:
            print("no in crop mask!")
            continue

    # find the file
    valid_list_data2 = [pl for pl in process_list_pr if "data2" in pl]
    valid_list_tq01 = [pl for pl in process_list_pr if "tq-data01" in pl]
    valid_list_tq02 = [pl for pl in process_list_pr if "tq-data02" in pl]
    valid_list_tq03 = [pl for pl in process_list_pr if "tq-data03" in pl]
    valid_list_tq04 = [pl for pl in process_list_pr if "tq-data04" in pl]

    # save the file
    out_data2 = "/home/tq/data_pool/test_data/US/valid_list_data2.json"
    out_tq1 = "/home/tq/data_pool/test_data/US/valid_list_tq1.json"
    out_tq2 = "/home/tq/data_pool/test_data/US/valid_list_tq2.json"
    out_tq3 = "/home/tq/data_pool/test_data/US/valid_list_tq3.json"
    out_tq4 = "/home/tq/data_pool/test_data/US/valid_list_tq4.json"

    with open(out_data2, "w") as fp8:
        json.dump(valid_list_data2, fp8, ensure_ascii=False, indent=2)
        print("data2: Valid data total %s" % len(valid_list_data2))

    with open(out_tq1, "w") as fp8:
        json.dump(valid_list_tq01, fp8, ensure_ascii=False, indent=2)
        print("tq01: Valid data total %s" % len(valid_list_tq01))

    with open(out_tq2, "w") as fp8:
        json.dump(valid_list_tq02, fp8, ensure_ascii=False, indent=2)
        print("tq02: Valid data total %s" % len(valid_list_tq02))

    with open(out_tq3, "w") as fp8:
        json.dump(valid_list_tq03, fp8, ensure_ascii=False, indent=2)
        print("tq03: Valid data total %s" % len(valid_list_tq03))

    with open(out_tq4, "w") as fp8:
        json.dump(valid_list_tq04, fp8, ensure_ascii=False, indent=2)
        print("tq04: Valid data total %s" % len(valid_list_tq04))


if __name__ == "__main__":
    start = time.time()
    imput_json = "/home/tq/data_pool/test_data/LE07-LC08-2018-0401-0801-USA.json"
    pr = extract_corp_pr()
    extract_list()
    # extract_vaild_path(imput_json)

    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
