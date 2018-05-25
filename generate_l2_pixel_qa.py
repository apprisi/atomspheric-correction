#!/usr/bin/#!/usr/bin/env python3
import time
import os
import glob
import subprocess


def generate_l2_pixel_qa(data_path):
    """
    Function:
        to process the l2 pixel_qa by bqa

    input:
        data_path is as follows
        */landsat_sr/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2/

    output:
        0 sucess
        1 fail

    """

    if not os.path.exists(data_path):
        print("%s file path does not exist!" % data_path)
        return 1
    elif not os.path.isdir(data_path):
        print("%s is not a file directory" % data_path)
        return 1
    else:
        print("process the data.......\n")

        os.chdir(data_path)  # change the directory

        # produce the file
        xml = data_path[data_path.find('landsat_sr') + 38:-1] + '.xml'
        if os.path.isfile(xml):
            ret1 = subprocess.run(['generate_pixel_qa', '--xml', xml])
            if (ret1.returncode == 0):
                print("%s generate pixel_qa sucessfully!" % data_path)
                return 0
            else:
                print("%s failed to generate pixel_qa!" % data_path)
                return 1
        else:
            print("%s has no file %s", (data_path, xml))
            return 1
        
           
if __name__ == '__main__':
    
    data_path = '/home/jason/data_pool/test_data/USGS/LC08_L1TP_124033_20170615_20170628_01_T1/'
    start = time.time()
    flags = generate_l2_pixel_qa(data_path)
    print("The opuput path:%s" % flags)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
