#!/usr/bin/#!/usr/bin/env python3
import os
import time
import subprocess


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

if __name__ == '__main__':
    """
    test_LC08 = r'/home/jason/data_pool/test_data/LC08/LC08_L1GT_013027_20161008_20170220_01_T2'
    test_LE07 = r'/home/jason/data_pool/test_data/LE07/LE07_L1GT_010028_20021122_20160928_01_T2'
    test_LT05 = r'/home/jason/data_pool/test_data/LT05/LT05_L1GS_010029_20100221_20160901_01_T2'

    start = time.time()
    for index in [test_LC08, test_LE07, test_LT05]:
    	flags = atomCorrectProcess(index)
    	print("Process status:%s" % flags)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
    """
    test_LE07 = r'/home/jason/data_pool/test_data/LE07/LE07_L1TP_123033_20170616_20170712_01_T1'
    test_LC08 = r'/home/jason/data_pool/test_data/landsat_sr/LC08/01/013/027/LC08_L1GT_013027_20161008_20170220_01_T2'
    start = time.time()
    for index in [test_LC08]:
        flags = atomCorrectProcess(index)
        print("Process status:%s" % flags)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))

