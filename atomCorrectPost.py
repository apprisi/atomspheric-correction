#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import subprocess
from joblib import Parallel,delayed

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
        print(result_path + "deleted the data.......\n")
        os.chdir(result_path)

    if 'LC08' in result_path:
        tif_list = glob.glob('*_B*.TIF')
        img_list = glob.glob('*_b[1-9]*')
        toa_list = glob.glob('*_toa_*')
        for deleted_list in tif_list + img_list + toa_list:
            if os.path.exists(deleted_list):	            
                os.remove(deleted_list)
            else:
                print("no such file:%s" % deleted_list)
    elif 'LE07' in result_path:
        tif_list = glob.glob('*_B*.TIF')
        img_list = glob.glob('*_b[1-9]*')
        toa_list = glob.glob('*_toa_*')
        txt_list = glob.glob('ln*.txt')
        for deleted_list in tif_list + img_list + toa_list + txt_list:
            if os.path.exists(deleted_list):	            
                os.remove(deleted_list)
            else:
                print("no such file:%s" % deleted_list)
    else:
        print(result_path + "no file neesd to remove!")
    return 0 
        

if __name__ == '__main__':
    # remove the file  
    test_path = glob.glob(r'/home/jason/tq-data03/landsat_sr/01/*/*/*')
    Parallel(n_jobs=5)(delayed(atomCorrectPost)(tmp_path) for tmp_path in test_path)

"""
    start = time.time()
    flags = atomCorrectPost(test_path)
    print("Process status:%s" % flags)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
"""
