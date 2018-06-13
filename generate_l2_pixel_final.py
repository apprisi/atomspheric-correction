#!/usr/bin/#!/usr/bin/env python3
import time
import os
import glob
import subprocess
from joblib import Parallel,delayed


def generate_l2_pixel_final(data_path):
    """
    Function:
        genera_pixel_qa --xml
        dilate_pixel_qa --xml --bit 5 --distance 3
        to process the l2 pixel_qa by bqa 

    input:
        data_path is as follows
        */landsat_sr/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2/

    output:
        0 sucess
        1 fail

    """

    # check the input
    if not os.path.exists(data_path):
        print("%s file path does not exist!" % data_path)
        return 1
    elif not os.path.isdir(data_path):
        print("%s is not a file directory" % data_path)
        return 1
    else:
        print("process the data.......\n")

    # check the result
    if len([ fps for fps in os.listdir(data_path) if '_sr_' in fps]) == 16:
        print(data_path + ' has been procesed by atmospheric correction!')
    else:
        print(data_path + 'atmospheric correction failed!')
        return 1

    # print the wrking path 
    os.chdir(data_path)  # change the directory
    print('Working path: %s' % os.getcwd()) # print woring path

    # check the XML
    f = open('/home/jason/data_pool/sample_data/process_no_xml.txt', 'a')
    xml_list = glob.glob('*.xml')
    if len(xml_list) < 1:
        print(data_path + "NO XML!")
        print(data_path + "NO XML!", file = f)
        f.close()
        return 1
    elif len(xml_list)==1:
        xml = xml_list[0]

    # check the pixel_qa 
    pixel_qa = glob.glob('*_pixel_qa.img')  # read XML
    if len(pixel_qa) < 1:
        print(data_path + ' will be generate the pixel_qa!')
    elif len(pixel_qa)==1:
        print(data_path + ' has been processed!')
        return 0
        # # dilate_pixel_qa --xml --bit 5 --distance 3
        # ret1 = subprocess.run(['dilate_pixel_qa', '--xml', xml ,'--bit=5', '--distance=3']) # need check
        # if (ret1.returncode == 0):
        #     print("%s dilate_pixel_qa sucessfully!" % data_path)
        #     return 0
        # else:
        #     print("%s failed to dilate_pixel_qa!" % data_path)
        #     return 1

    # produce the file
    ret1 = subprocess.run(['generate_pixel_qa', '--xml', xml])
    if (ret1.returncode == 0):
        print("%s generate pixel_qa sucessfully!" % data_path)

        # dilate_pixel_qa --xml --bit 5 --distance 3
        ret1 = subprocess.run(['dilate_pixel_qa', '--xml', xml ,'--bit=5', '--distance=3']) # need check
        if (ret1.returncode == 0):
            print("%s dilate_pixel_qa sucessfully!" % data_path)
            return 0
        else:
            print("%s failed to dilate_pixel_qa!" % data_path)
            return 1
    else:
        print("%s failed to generate pixel_qa!" % data_path)
        return 1
        
if __name__ == '__main__':
    
    start = time.time()
    L5 = '/home/jason/tq-data*/landsat_sr/LT05/01/*/*/*'
    L5_path =glob.glob(L5)
    L8 = '/home/jason/tq-data*/landsat_sr/LC08/01/*/*/*'
    L8_path =glob.glob(L8)
    data_path = L5_path + L8_path

    Parallel(n_jobs=4)(delayed(generate_l2_pixel_final)(tmp) for tmp in data_path)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
