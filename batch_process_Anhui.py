#!/usr/bin/env python3
import os
import time
import glob
import json
import shutil
import subprocess
from joblib import Parallel,delayed


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

    result_path = os.path.join(result_root, data_path[data_path.find('landsat/')+8:])
    print(result_path)

    # check the path and file
    tq_tmp = result_path.replace('tq-data04', 'tq-tmp')
    if not os.path.exists(tq_tmp):
        os.makedirs(tq_tmp)
        print(tq_tmp)

    if not os.path.exists(result_path):
        os.makedirs(result_path) 
        print("Creat folder is ok!")  

    elif os.path.exists(result_path):
        print("Folder is exist and it will be check!")
        if len([ fps for fps in os.listdir(result_path) if '_sr_' in fps]) == 16:
            print("%s maybe has been processed!" % result_path)
            return 0
        else:
            print("%s is empty or not converted!" % result_path)

    # copy the txt to result path and convert the TIFF
    txt_list = glob.glob('*.txt')
    tif_list = glob.glob('*.TIF')

    for tif in tif_list:
        _, tif_name = os.path.split(tif)
        ret1 = subprocess.run(['gdal_translate', '-co', 'TILED=NO', tif, os.path.join(tq_tmp, tif_name)])
        if (ret1.returncode == 0):
            print("%s conversion finished!" % tif) 
        else:
            print("%s conversion failed!" % tif)

    for txt in txt_list:
        _, txt_name = os.path.split(txt)
        shutil.copyfile(txt, os.path.join(tq_tmp, txt_name))               

    # change the directory, remove the IMD file
    os.chdir(tq_tmp)  
    IMD_list    = glob.glob('*.IMD')
    for imd in IMD_list:
        os.remove(imd)
        print(imd + " file is deleted!")

    return tq_tmp


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

    if ret1.returncode != 0:
        print("%s data format conversion failed!\n" % mtl_txt)
        return 1
    else:
        print("%s data format conversion completed!\n" % mtl_txt)

    # atomspheric correct
    mtl_xml = glob.glob('*.xml')[0] # read XML
    if 'LC08' in mtl_xml:
        ret2 = subprocess.run(['do_lasrc.py', '--xml', mtl_xml])
        if (ret2.returncode == 0):
            print("%s data atomospheric correction processing finished!\n" % mtl_xml)
            return 0
        else:
            print("%s data atomospheric correction processing failure!\n" % mtl_xml)
            return 1
    elif 'LE07' in mtl_xml or 'LT05' in mtl_xml:
        ret2 = subprocess.run(['do_ledaps.py', '--xml', mtl_xml])
        if (ret2.returncode == 0):
            print("%s data atomospheric correction processing finished!\n" % mtl_xml)
            return 0
        else:
            print("%s data atomospheric correction processing failure!\n" % mtl_xml)
            return 1
    else:
        print("%s format is wrong!\n" % mtl_xml)
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
    remote_path = result_path.replace('tq-tmp','tq-data04')
    if not os.path.exists(result_path):
        print("%s file path does not exist!\n" % result_path)
        return -1
    elif not os.path.isdir(result_path):
        print("%s is not a file directory.\n" % result_path)
        return -1
    else:
        print("deleted the data.......\n")

    os.chdir(result_path)

    # deleted the temp data
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
                print("no such file:%s\n" % deleted_list)
    else:
        print(result_path + "no file need to remove!\n")
   
    # generate the pixel_qa
    print('Working path: %s' % os.getcwd()) # print woring path

    pixel_qa = glob.glob('*_pixel_qa.img')  # check
    if len(pixel_qa) < 1:
        print(result_path + ' will be generate the pixel_qa!')
    elif len(pixel_qa)==1:
        print(result_path + ' has been processed!')
        return 0

    xml_list = glob.glob('*.xml') # check
    if len(xml_list) < 1:
        print("NO XML!")
        return 1
    elif len(xml_list)==1:
        xml = xml_list[0]
    
    ret1 = subprocess.run(['generate_pixel_qa', '--xml', xml])
    if (ret1.returncode == 0):
        print("%s generate pixel_qa sucessfully!" % result_path)
    else:
        print("%s failed to generate pixel_qa!" % result_path)
        return 1


    # copy the data 
    if remote_path[-1] == '/':
        remote_path = remote_path[:-1]
    print(remote_path)    
    shutil.rmtree(remote_path)
    print(result_path)
    shutil.move(result_path,'/'.join(remote_path.split('/')[:-1]))
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
    # process_list = glob.glob(os.path.join(result_root,'landsat_sr', '01', '*', '*', '*'))
    print("------>Total:%s " % len(process_dict)) 
    
    flag = atomCorrectPre(data_path, result_root)
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
            # atomspheric correct postprocess
            flag2 = atomCorrectPost(flag)
            if flag2 == 0:
                print("%s atomspheric correction postprocessing is OK!\n" % flag)
                return 0
            elif flag2==-1:
                print("%s delete data exception!\n" % flag)
                return 1
        elif flag1 == -1:
            print("%s does not exist!\n" % flag)
            return 1
        elif flag1 == 0:
            print("%s atomspheric correction is failure!\n" % flag)
            return 1

if __name__ == '__main__':

    start = time.time()    
    # set the output path
    result_root = r'/home/jason/tq-data04/Anhui/landsat_sr'
    if os.path.exists(result_root):
        print("%s result root is ok." % result_root)
    else:
        os.makedirs(result_root)

    # load the scence of path
    with open(r'/home/jason/data_pool/sample_data/SRC_DATA_JSON/Anhui/valid_list_2016_Anhui_lc08.json', 'r') as fp:
        process_dict = json.load(fp)
    with open(r'/home/jason/data_pool/sample_data/SRC_DATA_JSON/Anhui/valid_list_2015_Anhui_lc08.json', 'r') as fp:
        process_dict += json.load(fp)
    with open(r'/home/jason/data_pool/sample_data/SRC_DATA_JSON/Anhui/valid_list_2014_Anhui_lc08.json', 'r') as fp:
        process_dict += json.load(fp)
    with open(r'/home/jason/data_pool/sample_data/SRC_DATA_JSON/Anhui/valid_list_2013_Anhui_lc08.json', 'r') as fp:
        process_dict += json.load(fp)

    # find the list
    # process_list = [] # save the path
    # for tmp_data in process_dict['scenes']:
    #         if tmp_data['cloud_perc'] <= 9:
    #             print(tmp_data['relative_path'] + "add to the list" )
    #             process_list.append(tmp_data['relative_path'])

    #with open('/etc/hosts','r') as hp:
    #hostname = 'data2'
    #process_dict = [data_path for data_path in process_dict if hostname in data_path]
    #process the data
    Parallel(n_jobs=3)(delayed(batch_process)(os.path.join(r'/home/jason', data_path), result_root) for data_path in process_dict)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))

