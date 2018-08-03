#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json


def find_error():
    """
       find the error data
    """
    all_tmp = glob.glob("/home/tq/tq-data*/landsat_sr/*/01/*/*/*")
    print(len(all_tmp))
    img_list = []
    tif_list = []
    error_list = []
    all_tmp = all_tmp
    for tmp in all_tmp:
        bqa_tif = os.path.join(tmp, os.path.split(tmp)[1] + "_bqa.tif")
        pixel_tif = os.path.join(tmp, os.path.split(tmp)[1] + "_pixel_qa.tif")
        bqa_img = os.path.join(tmp, os.path.split(tmp)[1] + "_bqa.img")
        pixel_img = os.path.join(tmp, os.path.split(tmp)[1] + "_pixel_qa.img")
        if os.path.exists(bqa_tif) and os.path.exists(pixel_tif):
            if (
                abs(os.path.getsize(bqa_tif) - os.path.getsize(pixel_tif)) > 58064 * 2
            ):  # 0.2M
                tif_list.append(tmp)
                continue
            elif (
                abs(os.path.getsize(bqa_tif) - os.path.getsize(pixel_tif)) <= 58064 * 2
            ):
                continue
        elif os.path.exists(bqa_img) and os.path.exists(pixel_img):
            if abs(os.path.getsize(bqa_img) - os.path.getsize(pixel_img)) > 58064 * 2:
                img_list.append(tmp)
                continue
            elif (
                abs(os.path.getsize(bqa_img) - os.path.getsize(pixel_img)) <= 58064 * 2
            ):
                continue
        elif os.path.exists(pixel_tif) or os.path.exists(pixel_img):
            if os.path.getsize(pixel_tif) < 100000000:
                print(pixel_tif)
        elif os.path.exists(pixel_img):
            if os.path.getsize(pixel_img) < 100000000:
                print(pixel_img)
        else:
            print("fuck!")
            error_list.append(tmp)

    out_r1 = "/home/tq/data_pool/test_data/landsat/tif_list.json"
    out_r2 = "/home/tq/data_pool/test_data/landsat/img_list.json"
    out_r3 = "/home/tq/data_pool/test_data/landsat/error_list.json"

    with open(out_r1, "w") as fp1:
        json.dump(tif_list, fp1, ensure_ascii=False, indent=2)
        print("tif data total %s" % len(tif_list))

    with open(out_r2, "w") as fp2:
        json.dump(img_list, fp2, ensure_ascii=False, indent=2)
        print("img data total %s" % len(img_list))

    with open(out_r3, "w") as fp2:
        json.dump(error_list, fp2, ensure_ascii=False, indent=2)
        print("error data total %s" % len(error_list))


if __name__ == "__main__":

    start = time.time()
    find_error()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
