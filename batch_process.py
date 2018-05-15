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
        return 0, data  has been processed
    """

    if not os.path.exists(data_path):
        print("%s file path does not exist!" % data_path)
        return -1
    elif not os.path.isdir(data_path):
        print("%s is not a file directory" % data_path)
        return -1
    else:
        print("Preprocess the data: %s\n" % data_path)

    os.chdir(data_path)  # change the directory

    # covert the data_path to lsit and extract some string and make the result path
    path_list   = data_path.split('/')
    tmp_path    = os.path.join(path_list[-5], path_list[-4], path_list[-3], path_list[-2],
                        path_list[-1]) # 'LE07', '01', 'path' ,'row', 'name'
    result_path = os.path.join(result_root, tmp_path)
    if not os.path.exists(result_path):
        os.makedirs(result_path)
        print("Creat folder is ok!")  
    else:
        print("Folder is exist and it will be check!")
        if os.listdir(result_path):
            print("%s maybe has been processed!" % result_path)
        else:
            print("%s is empty!" % result_path)

            # copy the txt to result path and convert the TIFF
            txt_list = glob.glob('*.txt')
            for txt in txt_list:
                _, txt_name = os.path.split(txt)
                shutil.copyfile(txt, os.path.join(result_path, txt_name))
                
            tif_list   = glob.glob('*.TIF')
            for tif in tif_list:
                _, tif_name = os.path.split(tif)
                ret1     = subprocess.run(['gdal_translate', '-co', 'TILED=NO', tif, os.path.join(result_path, tif_name)])

                if (ret1.returncode == 0):
                    print("%s conversion finished!" % tif) 
                else:
                    print("%s conversion failed!" % tif)

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
    mtl_txt = glob.glob(os.path.join(data_path, '*_MTL.txt'))[0] # find MTL filename
    ret1 = subprocess.run(['convert_lpgs_to_espa', '--mtl', mtl_txt])

    if (ret1.returncode != 0):
        print("%s data format conversion failed!" % mtl_txt)
        return 1
    else:
        print("%s data format conversion completed!" % mtl_txt)

    # atomspheric correct
    mtl_xml = glob.glob('*.xml')[0] # read XML
    if ('LC08' in mtl_xml):
        ret2 = subprocess.run(['do_lasrc.py', '--xml', mtl_xml])
        if (ret2.returncode == 0):
            print("%s data atomospheric correction finished!" % mtl_xml)
            return 0
        else:
            print("%s data atomospheric correction failure!" % mtl_xml)
            return 1
    if ('LE07' or 'LT05' in mtl_xml):
        ret2 = subprocess.run(['do_ledaps.py', '--xml', mtl_xml])
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
        print(result_path + "no file need to remove!")
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
    elif flag == 0:
        print(data_path + " has been process.")
        return 0
    else:
        print("sdhjshkj ")
	    # atomspheric correct process
        flag1 = atomCorrectProcess(flag)
        if flag1 == 0:
            print("%s atomspheric correction is successful!" % flag)

            # atomspheric correct postprocess
            flag2 = atomCorrectPost(flag)
            if flag2 == 0:
                print("%s atomspheric correction is OK!" % flag)
                return 0
            else:
                print("%s delete data exception!" % flag)
                return 1
        else:
            print("%s atomspheric correction is failure!" % flag)
            return 1

def extract_vaild_path(imput_json):
    """
    Funciton:
        a. when RT and T1 are exitence, remove the RT
        b. when cloud is 100, remove the data 

    input:    
        input_json: json file path, such as '/home/jason/data_pool/2017-le07.json'

    output:    
        valid_path: json file path, such as '/home/jason/data_pool/2017-le07_valid.json'

    """
    
    valid_path_list = []  # save the data path
    output_file = r'/home/jason/data_pool/2017-le07_valid.json'
    if not os.path.exists(imput_json):
        print("%s file path does not exist!" % imput_json)
        return -1
    elif not os.path.isfile(imput_json):
        print("%s is not a file" % imput_json)
        return -1
    else:
        print("extracting the data.......\n")

        with open(imput_json, 'r') as fp:
            process_dict = json.load(fp)

        # extra path to list and remove cloudiness more than 80%
        for tmp_data in process_dict['scenes']:
            if tmp_data['cloud_perc'] <= 80:
                print(tmp_data['relative_path'] + "add to the list" )
                valid_path_list.append(tmp_data['relative_path'])
            else:
                print(tmp_data['relative_path'] + "cloudiness more than 80%")
                file_name = tmp_data['relative_path'].split('landsat')[-1]
                print(file_name)
                sr_status['sence_sr_status']['cloud'].append(file_name)
                continue    
        
        # remove the RT data
        valid_result_list = []
        valid_path_list.sort() # sort the list
        for tmp_valid in valid_path_list:
            if '_RT' in tmp_valid:
                T1_file = os.path.split(os.path.split(tmp_valid)[0])[-1][1:-2] + 'T1'
                T2_file = os.path.split(os.path.split(tmp_valid)[0])[-1][1:-2] + 'T2'
                if T1_file or T2_file in valid_path_list:
                    print(tmp_valid + 'have T1 or T2. It no need to process again!')
                    continue
                else:
                    valid_result_list.append(tmp_valid)
            else:
                valid_result_list.append(tmp_valid)

        # save the result, 多进程对表处理
        # with open(output_file, 'w') as fp:
        #     json.dump(valid_result_list, fp, ensure_ascii=False, indent=2)
        # print('Valid data total %s in %s' % (len(valid_result_list), imput_json))
    return output_file

if __name__ == '__main__':

    start = time.time()

    # # open processing status
    # with open(r'/home/jason/tq-data03/landsat_sr/sr_status.json', 'r') as fp:
    #     sr_status = json.load(fp)
    
    # set the output path
    result_root = r'/home/jason/tq-data03/landsat_sr/LE07'
    if os.path.exists(result_root):
        print("%s result root is ok." % result_root)
    else:
        os.makedirs(result_root)

    # load the scence of path
    with open(r'/home/jason/data_pool/2017-le07_valid.json', 'r') as fp:
        process_dict = json.load(fp)

    #process the data
    Parallel(n_jobs=3)(delayed(batch_process)(os.path.join(r'/home/jason', data_path), result_root) for data_path in process_dict)
    
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))