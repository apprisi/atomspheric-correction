#!/usr/bin/#!/usr/bin/env python3
import time
import os
import glob
import subprocess
import gdal
import numpy as np


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
        print('woring path: %s' % os.getcwd()) # print woring path
        
        # cheack atomspheric process sucess
        if os.path.exists(data_path):
            print(data_path + ' will be check!')
            if len([fps for fps in os.listdir(data_path) if '_sr_' in fps]) == 16:
                print(data_path + 'to process the BQA!')
            else:
                return 1
        else:
            print(data_path + ' is wrong!')
            return 1

        # produce the file
        xml = os.path.split(data_path)[-1] + '.xml'
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

def creat_clear_mask(data_path):
    '''
    Funciton: 
        In data_path to find pixel_qa and creat clear mask
    
    Input:
        data_path:*/data/028/LE07_L1GT_010028_20040503_20160926_01_T2

    Output:
        0: sucess
        1: fail
    '''        
    if not os.path.exists(data_path):
        print("%s file path does not exist!" % data_path)
        return 1
    elif not os.path.isdir(data_path):
        print("%s is not a file directory" % data_path)
        return 1
    else:
        print("process the data.......\n") 

    # change the path
    os.chdir(data_path)

    #check the file 
    clear_qa = glob.glob(data_path + '*_clear_qa*')
    if len(clear_qa)>0:
        clear_qa = clear_qa[0]
    else:
        print(data_path + ' not processed!')
        
    if not os.path.isfile(clear_qa):
        print("%s file does not exist!" % clear_qa)
        return 1


    # find the pixel qa file
    pixel_qa = glob.glob(data_path + '/*_pixel_qa*')[0] 

    if not os.path.isfile(pixel_qa):
        print("%s file does not exist!" % pixel_qa)
        return 1

    # register
    gdal.AllRegister()
    gdal.SetConfigOption("gdal_FILENAME_IS_UTF8", "YES")

    BQA = gdal.Open(pixel_qa)
    if BQA is None:
        print("Failed to open {0} !".format(pixel_qa))
        return 1
    else:
        geotrans = BQA.GetGeoTransform()
        proj = BQA.GetProjection()
        BQA_Arrary = BQA.ReadAsArray() # read the data

        # creat mask 
        if 'LC08' in pixel_qa:
            mask1 = np.equal(BQA_Arrary, 322)
            mask2 = np.equal(BQA_Arrary, 386)
            mask3 = np.equal(BQA_Arrary, 834)
            mask4 = np.equal(BQA_Arrary, 898)
            mask5 = np.equal(BQA_Arrary, 1346)
            mask12 = np.logical_or(mask1, mask2)
            mask34 = np.logical_or(mask3, mask4)
            mask1234 = np.logical_or(mask12, mask34)
            mask = np.logical_or(mask1234, mask5)

        elif 'LT05' in pixel_qa or 'LE07' in pixel_qa:
            mask1 = np.equal(BQA_Arrary, 66)
            mask2 = np.equal(BQA_Arrary, 130)
            mask = np.logical_or(mask1, mask2)

        # save the file
        [cols, rows] = mask.shape
        if 'tif' in pixel_qa:
            driver = gdal.GetDriverByName('GTiFF')
            output_name = os.path.join(data_path, pixel_qa.replace('pixel_qa', 'clear_qa'))
        elif 'img' in pixel_qa:
            driver = gdal.GetDriverByName('ENVI')
            output_name = os.path.join(data_path, pixel_qa.replace('pixel_qa', 'clear_qa'))
        
        pixel_qa_clear = driver.Create(output_name, rows, cols, 1, gdal.GDT_Byte)
        pixel_qa_clear.SetGeoTransform(geotrans)
        pixel_qa_clear.SetProjection(proj)
        pixel_qa_clear.GetRasterBand(1).WriteArray(mask)
        pixel_qa_clear.FlushCache()
        pixel_qa_clear = None
        BQA =None
    return 0

    
if __name__ == '__main__':
    
    data_path = glob.glob(os.path.join('/home/jason/data_pool/test_data' , '*', '*'))
    start = time.time()

    for tmp in [dp for dp in data_path if '.tar' not in dp]:
        flags = generate_l2_pixel_qa(tmp)
        if flags == 0:
            print(tmp + 'generate l2 pixel_qa sucess.')
        elif flags == 1:
            print(tmp + 'generate l2 pixel_qa failed!')
            continue

    # for tmp in data_path:
    #     print(tmp)
    #     flag = creat_clear_mask(tmp)
    #     if flag == 0:
    #         print(tmp + ' generate clear mask sucess.')
    #     elif flag ==1:
    #         print(tmp + ' failed to generate clear mask!')
    #         continue
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))