#!/usr/bin/env python3
import os
import time
import glob
import json
import shutil
import subprocess
from joblib import Parallel,delayed


def atomCorrectPre(data_path):
    """
    Function:
        copy the raw data to result path and covert tiled tiff to stripped tiff

    input:
        result_root = /home/jason/data_pool/test_data/landat_sr  # set the result root path
        data_path is as follows
        /../../landsat/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2

    output:
        process sucess, and return the result path
        return -1, data path not exists or data path exists maybe has been processed
        return 0, data  has been processed
    """

    # check the data path
    if not os.path.exists(data_path):
        print("%s file path does not exist!" % data_path)
        return -1
    elif not os.path.isdir(data_path):
        print("%s is not a file directory" % data_path)
        return -1
    else:
        print("Preprocess the data: %s\n" % data_path)

    os.chdir(data_path)  # change the directory
    print("woring path is " + os.getcwd())

    result_path = os.path.join(data_path, 'sr')
    print(result_path)

    if not os.path.exists(result_path):
        os.makedirs(result_path) 
        print("Creat folder is ok!")  

    # copy the txt to result path and convert the TIFF
    txt_list = glob.glob('*.txt')
    tif_list = glob.glob('*.TIF')

    for tif in tif_list:
        _, tif_name = os.path.split(tif)
        ret1 = subprocess.run(['gdal_translate', '-co', 'TILED=NO', tif, os.path.join(result_path, tif_name)])
        if (ret1.returncode == 0):
            print("%s conversion finished!" % tif) 
        else:
            print("%s conversion failed!" % tif)

    for txt in txt_list:
        _, txt_name = os.path.split(txt)
        shutil.copyfile(txt, os.path.join(result_path, txt_name))               

    # change the directory, remove the IMD file
    os.chdir(result_path)  
    IMD_list    = glob.glob('*.IMD')
    for imd in IMD_list:
        os.remove(imd)
        print(imd + " file is deleted!")

    return result_path


def atomCorrectProcess(data_path):
    """
    Function:
        process the landsat 7&8 data atomospheric correction
    input:
        data_path is as follows
       /../../landsat/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2
    output:
        return 0, process sucess
        return 1, process failure
        return -1, data not exists
    """

    if not os.path.exists(data_path):
        print("%s file path does not exist!" % data_path)
        return -1
    elif not os.path.isdir(data_path):
        print("%s is not a file directory" % data_path)
        return -1
    else:
        print("Converting the data.......\n")

    # change the directory, convert to ESPA
    os.chdir(data_path)
    mtl_txt = glob.glob('*_MTL.txt')[0] # find MTL filename
    print(mtl_txt)
    ret1 = subprocess.run(['convert_lpgs_to_espa', '--mtl', mtl_txt])

    if (ret1.returncode != 0):
        print("%s data format conversion failed!\n" % mtl_txt)
        return 1
    else:
        print("%s data format conversion completed!\n" % mtl_txt)

    # atomspheric correct
    mtl_xml = glob.glob('*.xml')[0] # read XML
    if 'LC08' in mtl_xml:
        ret2 = subprocess.run(['do_lasrc.py', '-i', mtl_xml, '-s', 'False'])
        if ret2.returncode == 0:
            print("%s data atomospheric correction processing finished!\n" % mtl_xml)
            return 0
        else:
            print("%s data atomospheric correction processing failure!\n" % mtl_xml)
            return 1
    elif 'LE07' in mtl_xml or 'LT05' in mtl_xml:
        ret2 = subprocess.run(['do_ledaps.py', '-f', mtl_xml, '-s', 'Flase'])
        if ret2.returncode == 0:
            print("%s data atomospheric correction processing finished!\n" % mtl_xml)
            return 0
        else:
            print("%s data atomospheric correction processing failure!\n" % mtl_xml)
            return 1
    else:
        print("%s format is wrong!\n" % mtl_xml)
        return 1

def batch_process(data_path):
    """
    Function:
             batch process the atomspheric correction

    input:
         data_path: whre is the data such as
         */landsat_sr/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2

    output:
         return 0, process sucess
         return 1, process failed
    """

  
    flag = atomCorrectPre(data_path)
    if flag == -1:
        print(data_path + " preprocess failed!\n")
        return 1
    elif flag == 0:
        print(data_path + " has been processed.\n")
        return 0
    else:
	    # atomspheric correct process
        print("%s will be process.\n" % flag)
        flag1 = atomCorrectProcess(flag)
        if flag1 == 0:
            print("%s atomspheric correction processing is successful!\n" % flag)
        elif flag1 == -1:
            print("%s does not exist!\n" % flag)
            return 1
        elif flag1 == 1:
            print("%s atomspheric correction is failure!\n" % flag)
            return 1

if __name__ == '__main__':

    start = time.time()
    data_path = glob.glob('/home/jason/data_pool/test_data/L1C/*')

    for tmp in data_path: 
        flag = batch_process(tmp)
        print(flag)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))

