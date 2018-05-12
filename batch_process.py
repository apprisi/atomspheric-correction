#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import shutil
import subprocess

def atomCorrectPre(data_path, result_root):
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
            print("Folder is exist and it maybe has been processed!")
            return -1 
        else:
            os.makedirs(result_path)
            print("Creat folder is ok!")

            # copy the txt to result path and convert the TIFF
            txt_list    = glob.glob(os.path.join(data_path, '*.txt'))
            tif_list   = glob.glob(os.path.join(data_path, '*.TIF'))

            for txt in txt_list:
                txt_dir, txt_name = os.path.split(txt)
                shutil.copyfile(txt, os.path.join(result_path, txt_name))

            for tif in tif_list:
                start = time.time()
                tif_dir, tif_name = os.path.split(tif)
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

        mtl_txt = os.path.split(data_path)[-1] + "_MTL.txt" # creat MTL filename
        os.chdir(data_path)  # change the directory

        start = time.time()
        ret1 = subprocess.run(['convert_lpgs_to_espa', '--mtl', mtl_txt])
        end = time.time()
        print("%s executed using time %.2f seconds" % (ret1.args, (end - start)))
        if (ret1.returncode != 0):
            print("%s data format conversion failed!" % mtl_txt)
            return 1
        else:
            print("%s data format conversion completed!" % mtl_txt)

            mtl_xml = os.path.split(data_path)[-1] + ".xml"  # atomospheric
            if ('LC08' in mtl_xml):
                start = time.time()
                ret2 = subprocess.run(['do_lasrc.py', '--xml', mtl_xml])
                end = time.time()
                print("%s executed using time %.2f seconds" % (ret2.args, (end - start)))
                if (ret2.returncode == 0):
                    print("%s data atomospheric correction finished!" % mtl_xml)
                    return 0
                else:
                    print("%s data atomospheric correction failure!" % mtl_xml)
                    return 1
            if ('LE07' or 'LT05' in mtl_xml):
                start = time.time()
                ret2 = subprocess.run(['do_ledaps.py', '--xml', mtl_xml])
                end = time.time()
                print("%s executed using time %.2f seconds" % (ret2.args, (end - start)))
                if (ret2.returncode == 0):
                    print("%s data atomospheric correction finished!" % mtl_xml)
                    return 0
                else:
                    print("%s data atomospheric correction failure!" % mtl_xml)
                    return 1
            else:
                print("%s format is wrong!" % mtl_xml)
                return 1

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

def batch_process(data_path, result_root):
    """
    Function:
             batch process the atomspheric correction

    input:
        data_path: whre is the data such as 
        */landsat_sr/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2

    output:
        return 0, process sucess--
        return 1, data not exists--
    """



    start = time.time()
    
    # atomspheric correct preprocess 
    flag = atomCorrectPre(data_path, result_root)
    if flag == -1:
        print(data_path + " preprocess failed!")
    else:

	# atomspheric correct process
        flag1 = atomCorrectProcess(flag)
        if flag1 == 0:
            print(flag + "Atomspheric correction is successful!")

            # atomspheric correct postprocess
            flag2 = atomCorrectPost(flag)
            if flag2 == 0:
                print(flag + "Atomspheric correction is OK!")
            else:
                print(flag + "Delete data exception!")

        else:
            print(flag + "Atomspheric correction is failure!")
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))


if __name__ == '__main__':
   
    data_path = r'/home/jason/tq-data01/landsat/LE07/01/123/033/LE07_L1TP_123033_20170616_20170712_01_T1'
    result_root = r'/home/jason/data_pool/test_data/landsat_sr' 
    start = time.time()
    flag = atomCorrectPre(data_path, result_root)
    if flag == -1:
        print(data_path + " preprocess failed!")
    else:
        flag1 = atomCorrectProcess(flag)
        if flag1 == 0:
            print(flag + "Atomspheric correction is successful!")
            flag2 = atomCorrectPost(flag)
            if flag2 == 0:
                print(flag + "Atomspheric correction is OK!")
            else:
                print(flag + "Delete data exception!")
        else:
            print(flag + "Atomspheric correction is failure!")
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))

