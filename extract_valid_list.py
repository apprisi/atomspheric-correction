#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json


def extract_vaild_path(imput_json):
    """
    Funciton:
        1. when cloud is more than 80, remove the data 

    input:    
        input_json: json file path, such as '/home/jason/data_pool/2017-le07.json'

    output:    
        valid_path: json file path, such as '/home/jason/data_pool/2017-le07_valid.json'

    """
    
    # save the data path
    valid_list_2017_lc08 = []
    valid_list_2017_le07 = []

    output_file1 = r'/home/jason/data_pool/valid_list_2017_lc08.json'
    output_file2 = r'/home/jason/data_pool/valid_list_2017_le07.json'
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
            if tmp_data['cloud_perc'] <= 80 and tmp_data['sat'] == 'LC08':
                print(tmp_data['relative_path'] + "add to the list 2017_LC08" )
                valid_list_2017_lc08.append(tmp_data['relative_path'])
            elif tmp_data['cloud_perc'] <= 80 and tmp_data['sat'] == 'LE07':
                print(tmp_data['relative_path'] + "add to the list 2017_LE07" )
                valid_list_2017_le07.append(tmp_data['relative_path'])
            elif tmp_data['cloud_perc'] <= 80 and tmp_data['sat'] == 'LC05':
                print(tmp_data['relative_path'] + "add to the list 2017_LC05" )
                valid_list_2017_le07.append(tmp_data['relative_path'])
            else:
                print(tmp_data['relative_path'] + "cloudiness more than 80%")
                continue  
              
        
        # sort the list
        
        valid_list_2017_lc08.sort()
        valid_list_2017_le07.sort()
        
        # save the result, 多进程对表处理
        with open(output_file1, 'w') as fp1:
            json.dump(valid_list_2017_lc08, fp1, ensure_ascii=False, indent=2)
        print('Valid data total %s in %s' % (len(valid_list_2017_lc08), imput_json))

        with open(output_file2, 'w') as fp2:
            json.dump(valid_list_2017_le07, fp2, ensure_ascii=False, indent=2)
        print('Valid data total %s in %s' % (len(valid_list_2017_le07), imput_json))

    return 1

if __name__ == '__main__':
    start = time.time()
    # # deleted the empty dir and RT dir
    # all_tmp = glob.glob(r'/home/jason/tq-data03/landsat_sr/LE07/*/*/*/*')
    # print(len(all_tmp))
    # for tmp in all_tmp:
    #     if not os.listdir(tmp):
    #         os.rmdir(tmp)
    # a_tmp = glob.glob(r'/home/jason/tq-data03/landsat_sr/LE07/*/*/*/*')
    # print(len(a_tmp))
    imput_json = r'/home/jason/data_pool/sample_data/SRC_DATA_JSON/2017.json'
    flag = extract_vaild_path(imput_json)
    print(flag)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))