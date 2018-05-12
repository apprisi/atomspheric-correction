#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import subprocess


def atomCorrectPost(result_path):
    """
    Function:
             deleted the atomospheric correction temporary data

    input:
        result_path is as follows
        */landsat_sr/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2

    output:
        return 0, process sucess
        return -1, data not exists
    """

    if not os.path.exists(result_path):
        print("%s file path does not exist!" % result_path)
        return -1
    elif not os.path.isdir(result_path):
        print("%s is not a file directory" % result_path)
        return -1
    else:
        print("deleted the data.......\n")

        tif_list = glob.glob(os.path.join(result_path, '*_B*.TIF'))
        img_list = glob.glob(os.path.join(result_path, '*_b[1-9]*'))
        toa_list = glob.glob(os.path.join(result_path, '*_toa_*'))
        for deleted_list in tif_list + img_list + toa_list:
            if os.path.exists(deleted_list):	            
                os.remove(deleted_list)
            else:
                print("no such file:%s" % deleted_list)
    return 0 
        

if __name__ == '__main__':

    test_path = r'/home/jason/data_pool/test_data/landsat_sr/LC08/01/013/027/LC08_L1GT_013027_20161008_20170220_01_T2'
 
    start = time.time()
    flags = atomCorrectPost(test_path)
    print("Process status:%s" % flags)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))

