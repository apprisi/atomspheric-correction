#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import shutil
import subprocess


def atomCorrectPre(result_root, data_path):
    """
    Function:
        copy the raw data to result path and covert tiled tiff to stripped tiff 
       
    input:
        result_root = /home/jason/data_pool/test_data  # set the result root path	
        data_path is as follows
        /../../landsat/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2

    output:
        process sucess, and return the result path
        return -1, data path not exists
    """

    if not os.path.exists(data_path):
        print("%s file path does not exist!" % data_path)
        return -1
    elif not os.path.isdir(data_path):
        print("%s is not a file directory" % data_path)
        return -1
    else:
        print("preprocess the data.......\n")

        os.chdir(data_path)  # change the directory

        # covert the data_path to lsit and extract some string and make the result path
        path_list   = data_path.split('/')  
        tmp_path    = os.path.join(path_list[-5], path_list[-4], path_list[-3], path_list[-2],
                           path_list[-1]) # 'LE07', '01', 'path' ,'row', 'name'
        result_path = os.path.join(result_root, tmp_path)
        if os.path.exists(result_path):
            print("Folder is exist!")
        else:
            os.makedirs(result_path)
            print("Creat folder is ok!")

        # copy the txt to result path and convert the TIFF
        txt_list    = glob.glob(os.path.join(data_path, '*.txt'))
        tif_list   = glob.glob(os.path.join(data_path, '*.TIF'))

        for txt in txt_list:
            _, txt_name = os.path.split(txt)
            shutil.copyfile(txt, os.path.join(result_path, txt_name))

        for tif in tif_list:
            start = time.time()
            _, tif_name = os.path.split(tif)
            ret1     = subprocess.run(['gdal_translate', '-co', 'TILED=NO', tif, os.path.join(result_path, tif_name)])
            end      = time.time()
            print("%s executed using time %.2f seconds" % (ret1.args, (end - start)))
            if (ret1.returncode == 0):
                print("%s conversion finished!" % tif)
            else:
                print("%s conversion failed!" % tif)
    
        os.chdir(result_path)  # change the directory, remove the IMD file
        IMD_list    = glob.glob(os.path.join(result_path, '*.IMD'))
        for imd in IMD_list:
            os.remove(imd)
            print(imd + " file is deleted!")
    return result_path

if __name__ == '__main__':
    
    """
    data_path = r'/home/jason/data_pool/test_data/LC08/LC08_L1GT_013027_20161008_20170220_01_T2'
    test_LE07 = r'/home/jason/data_pool/test_data/LE07/LE07_L1GT_010028_20021122_20160928_01_T2'
    test_LT05 = r'/home/jason/data_pool/test_data/LT05/LT05_L1GS_010029_20100221_20160901_01_T2'
    """
    result_root = r'/home/jason/data_pool/test_data/landsat_sr'
    data_path = r'/home/jason/data_pool/test_data/LC08/01/013/027/LC08_L1GT_013027_20161008_20170220_01_T2'
    start = time.time()
    flags = atomCorrectPre(result_root, data_path)
    print("The opuput path:%s" % flags)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))

