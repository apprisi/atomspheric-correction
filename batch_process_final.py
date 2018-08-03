#!/usr/bin/env python3
import os
import time
import glob
import json
import shutil
import subprocess
from joblib import Parallel
from joblib import delayed


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
        print("Preprocess the data: %s" % data_path)

    # check the result
    tq_data03 = os.path.join(result_root, data_path[data_path.find("landsat/") + 8 :])
    if os.path.exists(tq_data03):  # check tq03
        print(tq_data03 + " folder is exist and it will be check!")
        if len([fps for fps in os.listdir(tq_data03) if "_sr_" in fps]) == 16:
            print(data_path + " has been procesed!")
            return 0
        else:
            shutil.rmtree(tq_data03)

    tq_data01 = tq_data03.replace("tq-data03", "tq-data01")
    if os.path.exists(tq_data01):  # check tq01
        print(tq_data01 + " folder is exist and it will be check!")
        if len([fps for fps in os.listdir(tq_data01) if "_sr_" in fps]) == 16:
            print(data_path + " has been procesed!")
            return 0
        else:
            shutil.rmtree(tq_data01)

    tq_data02 = tq_data03.replace("tq-data03", "tq-data02")
    if os.path.exists(tq_data02):  # check tq02
        print(tq_data02 + "folder is exist and it will be check!")
        if len([fps for fps in os.listdir(tq_data02) if "_sr_" in fps]) == 16:
            print(data_path + " has been procesed!")
            return 0
        else:
            shutil.rmtree(tq_data02)

    tq_data04 = tq_data03.replace("tq-data03", "tq-data04")
    if os.path.exists(tq_data04):  # check tq04
        print(tq_data04 + "folder is exist and it will be check!")
        if len([fps for fps in os.listdir(tq_data04) if "_sr_" in fps]) == 16:
            print(data_path + " has been procesed!")
            return 0
        else:
            shutil.rmtree(tq_data04)

    # after check, then can run the proprecess
    tq_tmp = tq_data03.replace("tq-data03", "tq-tmp")
    if not os.path.exists(tq_tmp):
        os.makedirs(tq_tmp)
    if not os.path.exists(tq_data03):
        os.makedirs(tq_data03)
        print("Creat folder is ok!")

    # change the path
    os.chdir(data_path)
    print("Working path is " + os.getcwd())

    # copy the txt to result path and convert the TIFF
    txt_list = glob.glob("*.txt")
    tif_list = glob.glob("*.TIF")

    for tif in tif_list:
        _, tif_name = os.path.split(tif)
        ret1 = subprocess.run(
            ["gdal_translate", "-co", "TILED=NO", tif, os.path.join(tq_tmp, tif_name)]
        )
        if ret1.returncode == 0:
            print("%s conversion finished!" % tif)
        else:
            print("%s conversion failed!" % tif)

    for txt in txt_list:
        _, txt_name = os.path.split(txt)
        shutil.copyfile(txt, os.path.join(tq_tmp, txt_name))

    # change the directory, remove the IMD file
    os.chdir(tq_tmp)
    IMD_list = glob.glob("*.IMD")
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
        print("Converting the data.......")

    # change the directory, convert to ESPA
    os.chdir(data_path)
    print("[AtomCorrectProcess]:Working path is " + os.getcwd())

    # check the MTL.txt
    f = open("/home/jason/data_pool/sample_data/process_no_MTL.txt", "a")
    mtl_txt = glob.glob("*_MTL.txt")
    if len(mtl_txt) < 1:
        print(data_path + " NO MTL.txt!", file=f)
        f.close()
        return 1
    elif len(mtl_txt) == 1:
        mtl_txt = mtl_txt[0]

    # convert lpgs to espa
    ret1 = subprocess.run(["convert_lpgs_to_espa", "--mtl", mtl_txt])

    if ret1.returncode != 0:
        print("%s data format conversion failed!" % mtl_txt)
        return 1
    else:
        print("%s data format conversion completed!" % mtl_txt)

    # atomspheric correct
    mtl_xml = glob.glob("*.xml")[0]  # read XML
    if "LC08" in mtl_xml:
        ret2 = subprocess.run(["do_lasrc.py", "--xml", mtl_xml])
        if ret2.returncode == 0:
            print("%s data atomospheric correction processing finished!" % mtl_xml)
            return 0
        else:
            print("%s data atomospheric correction processing failure!" % mtl_xml)
            return 1
    if "LE07" in mtl_xml or "LT05" in mtl_xml:
        ret2 = subprocess.run(["do_ledaps.py", "--xml", mtl_xml])
        if ret2.returncode == 0:
            print("%s data atomospheric correction processing finished!" % mtl_xml)
            return 0
        else:
            print("%s data atomospheric correction processing failure!" % mtl_xml)
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
    remote_path = result_path.replace("tq-tmp", "tq-data03")
    if not os.path.exists(result_path):
        print("%s file path does not exist!" % result_path)
        return -1
    elif not os.path.isdir(result_path):
        print("%s is not a file directory." % result_path)
        return -1
    else:
        print("postprocess the data.......")

    # check the pixel_qa
    pixel_qa = glob.glob("*_pixel_qa.img")  # read XML
    if len(pixel_qa) < 1:
        print(result_path + " will generate the pixel_qa!")
    elif len(pixel_qa) == 1:
        print(result_path + " has generated the pixel_qa!")

    # check the XML
    f = open("/home/jason/data_pool/sample_data/process_no_xml.txt", "a")
    xml_list = glob.glob("*.xml")
    if len(xml_list) < 1:
        print(data_path + "NO XML!", file=f)
        f.close()
        return 1
    elif len(xml_list) == 1:
        xml = xml_list[0]

    # process BQA to pixel_qa
    ret1 = subprocess.run(["generate_pixel_qa", "--xml", xml])
    if ret1.returncode == 0:
        print("%s generate pixel_qa sucessfully!" % result_path)

        # dilate_pixel_qa --xml --bit 5 --distance 3
        ret1 = subprocess.run(
            ["dilate_pixel_qa", "--xml", xml, "--bit=5", "--distance=3"]
        )
        if ret1.returncode == 0:
            print("%s dilate_pixel_qa sucessfully!" % result_path)
        else:
            print("%s failed to dilate_pixel_qa!" % result_path)
            return 1
    else:
        print("%s failed to generate pixel_qa!" % result_path)
        return 1

    # deleted the tmp data
    os.chdir(result_path)
    if "LC08" in result_path:
        tif_list = glob.glob("*_B*.TIF")
        img_list = glob.glob("*_b[1-9]*")
        toa_list = glob.glob("*_toa_*")
        for deleted_list in tif_list + img_list + toa_list:
            if os.path.exists(deleted_list):
                os.remove(deleted_list)
            else:
                print("no such file:%s" % deleted_list)
    elif "LE07" in result_path or "LT05" in result_path:
        tif_list = glob.glob("*_B*.TIF")
        img_list = glob.glob("*_b[1-9]*")
        toa_list = glob.glob("*_toa_*")
        txt_list = glob.glob("ln*.txt")
        for deleted_list in tif_list + img_list + toa_list + txt_list:
            if os.path.exists(deleted_list):
                os.remove(deleted_list)
            else:
                print("no such file:%s" % deleted_list)
    else:
        print(result_path + "no file need to remove!")

    # copy the file from local to tq-data03
    if remote_path[-1] == "/":
        remote_path = remote_path[:-1]
    shutil.rmtree(remote_path)
    shutil.move(result_path, "/".join(remote_path.split("/")[:-1]))
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
    print("------>Total:%s" % len(process_dict))
    flag = atomCorrectPre(data_path, result_root)
    if flag == -1:
        print(data_path + " preprocess failed!")
        return 1
    elif flag == 0:
        print(data_path + " has been processed.")
        return 0
    else:

        # atomspheric correct process
        print("%s will be process." % flag)
        flag1 = atomCorrectProcess(flag)
        if flag1 == 0:
            print("%s atomspheric correction processing is successful!" % flag)

            # atomspheric correct postprocess
            flag2 = atomCorrectPost(flag)
            if flag2 == 0:
                print("%s atomspheric correction postprocessing is OK!" % flag)
                return 0
            elif flag2 == -1:
                print("%s delete data exception!" % flag)
                return 1
        elif flag1 == -1:
            print("%s does not exist!" % flag)
            return 1
        elif flag1 == 1:
            print("%s atomspheric correction is failure!" % flag)
            return 1
        else:
            print(flag1)


if __name__ == "__main__":

    start = time.time()
    result_root = r"/home/jason/tq-data03/landsat_sr"

    if os.path.exists(result_root):
        print("%s result root is ok." % result_root)
    else:
        os.makedirs(result_root)

    with open(
        r"/home/jason/data_pool/sample_data/SRC_DATA_JSON/LT05/valid_list_2011_lt05.json",
        "r",
    ) as fp:
        process_dict = json.load(fp)

    Parallel(n_jobs=20)(
        delayed(batch_process)(os.path.join(r"/home/jason", data_path), result_root)
        for data_path in process_dict
    )
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))

