#!/usr/bin/#!/usr/bin/env python3

import os
import time
import glob
import shutil
import subprocess
from joblib import Parallel, delayed


def convert_to_gtif(process_path):
    """
        convert img to gtif
    """
    print("{:*^80}".format(process_path))
    os.chdir(process_path)
    if ".tif" in "".join(os.listdir(process_path)):
        return False
    elif ".img" in "".join(os.listdir(process_path)):
        # IMG to TIF
        try:
            xml = glob.glob(process_path + "/*.xml")[0]
            _, base_name = os.path.split(process_path)
            ret1 = subprocess.run(
                ["convert_espa_to_gtif", "--xml", xml, "--gtif", base_name]
            )
            if ret1.returncode != 0:
                print("%s data conversion failed!" % xml)
            else:
                print("%s data conversion completed!" % xml)
        except Exception as e:
            print(e)
            return False
        try:
            # deleted
            img_list = glob.glob(os.path.join(process_path, "*.img"))
            hdr_list = glob.glob(os.path.join(process_path, "*.hdr"))
            txt_list = glob.glob(os.path.join(process_path, "*.txt"))
            for deleted_list in img_list + hdr_list + txt_list:
                if os.path.exists(deleted_list):
                    os.remove(deleted_list)
                else:
                    print("no such file:%s" % deleted_list)
            return True
        except Exception as e:
            print(e)
            return False


if __name__ == "__main__":
    start = time.time()
    home_dir = os.path.expanduser("~")
    process_file = os.path.join(
        home_dir, "data_pool/test_data/landsat_processed_list_sucess.txt"
    )

    # load process sucess list
    with open(process_file) as f:
        process_list = f.readlines()
    process_list = process_list[0].split(", ")

    # # save
    # out_tq4 = "/home/tq/data_pool/test_data/landsat_process_list.json"
    # with open(out_tq4, "w") as fp8:
    #     json.dump(process_list, fp8, ensure_ascii=False, indent=2)
    #     print("tq04: Valid data total %s" % len(process_list))

    hostname = "tq-data04"
    process_list = [tmp for tmp in process_list if hostname in tmp]

    Parallel(n_jobs=8)(
        delayed(convert_to_gtif)(os.path.join(home_dir, tmp)) for tmp in process_list
    )
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
