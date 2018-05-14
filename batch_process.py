#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json
import shutil
import subprocess
from joblib import Parallel,delayed

sr_status = {'satellite': 'landsat 7&8',
             'sence_sr_status': {'sucess': [], 'fail': [], 'cloud': []},
             'versionifo': {'ESPA': '1.15.0', 'LEDAPS': '3.3.0',
                            'LaSRC': '1.4.0'}}

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
            sr_status['sence_sr_status']['sucess'].append(result_path)
            return -1
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
            sr_status['sence_sr_status']['sucess'].append(result_path)
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

        os.chdir(data_path)  # change the directory
        mtl_txt = glob.glob('*_MTL.txt')[0] # find MTL filename
        start = time.time()
        ret1 = subprocess.run(['convert_lpgs_to_espa', '--mtl', mtl_txt])
        end = time.time()
        print("%s executed using time %.2f seconds" % (ret1.args, (end - start)))
        if (ret1.returncode != 0):
            print("%s data format conversion failed!" % mtl_txt)
            return 1
        else:
            print("%s data format conversion completed!" % mtl_txt)

            mtl_xml = glob.glob('*.xml')[0]  # atomospheric
            if ('LC08' in mtl_xml):
                start = time.time()
                ret2 = subprocess.run(['do_lasrc.py', '--xml', mtl_xml])
                end = time.time()
                print("%s executed using time %.2f seconds" % (ret2.args, (end - start)))
                if (ret2.returncode == 0):
                    print("%s data atomospheric correction finished!" % mtl_xml)
                    if data_path in sr_status['sence_sr_status']['sucess']:
                        print(data_path + 'processing status has update')
                    else:
                        sr_status['sence_sr_status']['sucess'].append(data_path)
                    return 0
                else:
                    print("%s data atomospheric correction failure!" % mtl_xml)
                    if data_path in sr_status['sence_sr_status']['fail']:
                        print(data_path + 'processing status has update')
                    else:
                        sr_status['sence_sr_status']['fail']..append(data_path)
                    return 1
            if ('LE07' or 'LT05' in mtl_xml):
                start = time.time()
                ret2 = subprocess.run(['do_ledaps.py', '--xml', mtl_xml])
                end = time.time()
                print("%s executed using time %.2f seconds" % (ret2.args, (end - start)))
                if (ret2.returncode == 0):
                    print("%s data atomospheric correction finished!" % mtl_xml)
                    if data_path in sr_status['sence_sr_status']['sucess']:
                        print(data_path + 'processing status has update')
                    else:
                        sr_status['sence_sr_status']['sucess'].append(data_path)
                    return 0
                else:
                    print("%s data atomospheric correction failure!" % mtl_xml)
                    if data_path in sr_status['sence_sr_status']['fail']:
                        print(data_path + 'processing status has update')
                    else:
                        sr_status['sence_sr_status']['fail'].append(data_path)
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
        elif 'LE07' or 'LT05' in result_path:
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

def batch_process(data_path, result_root):
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

    # atomspheric correct preprocess
    flag = atomCorrectPre(data_path, result_root)
    if flag == -1:
        print(data_path + " preprocess failed!")
        return 1
    else:

	# atomspheric correct process
        flag1 = atomCorrectProcess(flag)
        if flag1 == 0:
            print(flag + "Atomspheric correction is successful!")

            # atomspheric correct postprocess
            flag2 = atomCorrectPost(flag)
            if flag2 == 0:
                print(flag + "Atomspheric correction is OK!")
                return 0
            else:
                print(flag + "Delete data exception!")
                return 1
        else:
            print(flag + "Atomspheric correction is failure!")
            return 1

if __name__ == '__main__':

    start = time.time()

    # creat the result path
    result_root = r'/home/jason/tq-data03/landsat_sr'
    if os.path.exists(result_root):
        print("%s result root is ok." % result_root)
    else:
        os.makedirs(result_root)

    # load the scence of path
    with open(r'/home/jason/data_pool/2017-le07.json', 'r') as fp:
        process_dict = json.load(fp)

    # load the processing sr_status
    with open(r'/home/jason/tq-data03/landsat_sr/sr_status.json, 'w') as sr:
        sr_status = json.load(sr)

    # process the data
    Parallel(n_jobs=3)(delayed(batch_process)(os.path.join(r'/home/jason', data_path['relative_path']), result_root) for data_path in process_dict['scenes'])

    # save processing status
    with open(r'/home/jason/tq-data03/landsat_sr/sr_status.json, 'w') as sr:
        json.dump(sr_status, sr, ensure_ascii=False, indent=2)
    # for index in range(3): #len(process_dict['scenes'])
    #     data_path = process_dict['scenes'][index]['relative_path']
    #     process_path = os.path.join(r'/home/jason', data_path)
    #     flag = batch_process(process_path, result_root)
    #     if flag == 1:
    #         print("%s sences atomsphere maybe fail" % data_path)
    #         continue
    #     else:
    #         print("%s sences atomsphere process successfully." % data_path)

    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
