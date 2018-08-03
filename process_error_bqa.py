#!/usr/bin/#!/usr/bin/env python3
import os
import subprocess
import time
import json


def generate_pixel(file_path: str) -> bool:
    """
    Function:
        convert bqa to pixel_qa
    """
    # check the XML
    os.chdir(file_path)
    xml = os.path.split(file_path)[1] + ".xml"
    print(xml)

    # produce the file
    ret1 = subprocess.run(["generate_pixel_qa", "--xml", xml])
    if ret1.returncode == 0:
        print("%s generate pixel_qa sucessfully!" % file_path)

        # dilate_pixel_qa --xml --bit 5 --distance 3
        ret1 = subprocess.run(
            ["dilate_pixel_qa", "--xml", xml, "--bit=5", "--distance=3"]
        )  # need check
        if ret1.returncode == 0:
            print("%s dilate_pixel_qa sucessfully!" % file_path)
            return True
        else:
            print("%s failed to dilate_pixel_qa!" % file_path)
            return False
    else:
        print("%s failed to generate pixel_qa!" % file_path)
        return False


def process_tif_bqa(file_path: str) -> bool:
    """
       process the tif error data
    """
    bqa_tif = os.path.join(file_path, os.path.split(file_path)[1] + "_bqa.tif")
    bqa_img = os.path.join(file_path, os.path.split(file_path)[1] + "_bqa.img")

    ret1 = subprocess.run(["gdal_translate", bqa_tif, bqa_img])
    if ret1.returncode == 0:
        print("%s gdal_translate sucessfully!" % file_path)
        return True
    else:
        print("%s gdal_translate failed!" % file_path)
        return False


# ge

if __name__ == "__main__":

    start = time.time()
    # out_r1 = "/home/tq/data_pool/test_data/landsat/error_list.json"
    # with open(out_r1, "r") as fp:
    #     tif_list = json.load(fp)
    # # tif_list = tif_list[0:1]

    # for tmp in tif_list:
    #     process_flag = process_tif_bqa(tmp)
    #     if process_flag:
    #         pixel_flag = generate_pixel(tmp)
    #         if pixel_flag:
    #             print("process sucess!")
    #         else:
    #             print(tmp)
    #             print("process falied!")
    #     else:
    #         print(tmp)
    #         continue

    out_r2 = "/home/tq/data_pool/test_data/landsat/img_list.json"
    with open(out_r2, "r") as fp:
        img_list = json.load(fp)

    for tmp in img_list:
        pixel_flag = generate_pixel(tmp)
        if pixel_flag:
            print("process sucess!")
            continue
        else:
            print(tmp)
            print("process falied!")
            continue

    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
