#!/usr/bin/env python3
import os
import subprocess
import glob
import time


def download_convert_ozone(download_path, result_root):

    """
    FUNCTION: 
        download the ozone file and convert to hdf
    INPUT:
        download_path: /home/jason/Downloads/omi/Y2016
        result_root:  /home/jason/Downloads/omi/Y2016/Result  
    """

    # input process
    if not os.path.exists(download_path):
        print("%s file path does not exist!\n" % download_path)
        return -1
    else:
        print("extracting the data.......\n")

    # creat the path
    if not os.path.exists(result_root):
        os.makedirs(result_root)
    else:
        print("%s path exists" % result_root)

    # creat process list
    process_list = glob.glob(os.path.join(download_path,'L3*.txt'))
    print("Total %d need to convert." % len(process_list))
    for tmp in process_list:
        # find yday
        date_tuple = time.strptime(tmp[tmp.find('omi_') + 4:-4], "%Y%m%d")
        tm_yday = date_tuple.tm_yday
        if tm_yday < 10:
            tm_yday_str = '00' + str(tm_yday)
        elif tm_yday >=10 and tm_yday<=99:
            tm_yday_str = '0' + str(tm_yday)
        else:
            tm_yday_str = str(tm_yday)
        
        # creat result file name
        result_tmp = os.path.join(result_root,'TOMS_2016' + tm_yday_str + '.hdf')

        # cinvert the ozone
        ret = subprocess.run(['convert_ozone', tmp, result_tmp,'OMI'])
        if (ret.returncode == 0):
            print("%s data coversion finished!" % tmp)
        else:
            print("%s data conversion failed!" % tmp)

if __name__ == '__main__':
    start = time.time()
    download_path = '/home/jason/Downloads/omi/Y2016'
    result_root = '/home/jason/Downloads/omi/Y2016/Result' 
    download_convert_ozone(download_path, result_root)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))