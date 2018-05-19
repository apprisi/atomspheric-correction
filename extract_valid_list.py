#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json


# def extract_vaild_path(imput_json):
    # """
    # Funciton:
    #     1. when cloud is more than 80, remove the data 

    # input:    
    #     input_json: json file path, such as '/home/jason/data_pool/2017-le07.json'

    # output:    
    #     valid_path: json file path, such as '/home/jason/data_pool/2017-le07_valid.json'

    # """
    
    # # input process
    # if not os.path.exists(imput_json):
    #     print("%s file path does not exist!\n" % imput_json)
    #     return -1
    # elif not os.path.isfile(imput_json):
    #     print("%s is not a file.\n" % imput_json)
    #     return -1
    # else:
    #     print("extracting the data.......\n")

    # output_lc08 = os.path.join(result_root, 'LC08', 'valid_list_2017_lc08.json')
    # output_le07 = os.path.join(result_root, 'LE07', 'valid_list_2017_le07.json')
    # output_lt05 = os.path.join(result_root, 'LT05', 'valid_list_2017_lt05.json')

    # if not os.path.exists(imput_json):
    #     print("%s file path does not exist!\n" % imput_json)
    #     return -1
    # elif not os.path.isfile(imput_json):
    #     print("%s is not a file.\n" % imput_json)
    #     return -1
    # else:
    #     print("extracting the data.......\n")

    #     with open(imput_json, 'r') as fp:
    #         process_dict = json.load(fp)

    #    # extra path to list and remove cloudiness more than 80%
    #     for tmp_data in process_dict['scenes']:
    #         if tmp_data['cloud_perc'] <= 80 and tmp_data['sat'] == 'LC08':
    #             print(tmp_data['relative_path'] + "add to the list 2017_LC08" )
    #             valid_list_2017_lc08.append(tmp_data['relative_path'])
    #         elif tmp_data['cloud_perc'] <= 80 and tmp_data['sat'] == 'LE07':
    #             print(tmp_data['relative_path'] + "add to the list 2017_LE07" )
    #             valid_list_2017_le07.append(tmp_data['relative_path'])
    #         elif tmp_data['cloud_perc'] <= 80 and tmp_data['sat'] == 'LC05':
    #             print(tmp_data['relative_path'] + "add to the list 2017_LC05" )
    #             valid_list_2017_lt05.append(tmp_data['relative_path'])
    #         else:
    #             print(tmp_data['relative_path'] + "cloudiness more than 80%")
    #             continue  
              
        
    #     # sort the list
    #     valid_list_2017_lc08.sort()
    #     valid_list_2017_le07.sort()
    #     valid_list_2017_lt05.sort()

    #     if len(valid_list_2017_lc08) > 0:
    #         with open(output_lc08, 'w') as fp8:
    #             json.dump(valid_list_2017_lc08, fp8, ensure_ascii=False, indent=2)
    #         print('Valid data total %s in %s' % (len(valid_list_2017_lc08), imput_json))
    #     else:
    #         print('LC08 is no data in %s' % imput_json)

    #     if  len(valid_list_2017_le07) > 0:   
    #         with open(output_le07, 'w') as fp7:
    #             json.dump(valid_list_2017_le07, fp7, ensure_ascii=False, indent=2)
    #         print('Valid data total %s in %s' % (len(valid_list_2017_le07), imput_json))

    #     if  len(valid_list_2017_lt05) > 0:
    #         with open(output_lt05, 'w') as fp5:
    #             json.dump(valid_list_2017_lt05, fp5, ensure_ascii=False, indent=2)
    #         print('Valid data total %s in %s' % (len(valid_list_2017_lt05), imput_json))
    # return 1


def extract_list():
    """
        extract the data by server
    """

    # set the root path
    json_list7 = glob.glob('/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/*.json')

    # find the file landsat
    process_list =[]
    for tmp in json_list7:
        if '2017_le07' in tmp:
            continue
        else:
            with open(tmp, 'r') as fp:
                process_tmp = json.load(fp)
                process_list.extend(process_tmp)

    # find the file landsat 5
    # tmp =  '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LT05/valid_list_2011_lt05.json'
    # with open(tmp, 'r') as fp:
    #             process_tmp = json.load(fp)
    #             process_list.extend(process_tmp)
   
    
    print(len(process_list))

    # find the file
    valid_list_data2 = [pl for pl in process_list if 'data2' in pl]
    valid_list_tq01 =  [pl for pl in process_list if 'tq-data01' in pl]
    valid_list_tq02 =  [pl for pl in process_list if 'tq-data02' in pl]
    valid_list_tq03 =  [pl for pl in process_list if 'tq-data03' in pl]
    valid_list_tq04 =  [pl for pl in process_list if 'tq-data04' in pl]
 
    # save the file
    out_data2 =  '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_le07_data2.json'
    out_tq1 =  '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_le07_tq1.json'
    out_tq2 =  '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_le07_tq2.json'
    out_tq3 =  '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_le07_tq3.json'
    out_tq4 =  '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_le07_tq4.json'
    
    with open(out_data2, 'w') as fp8:
        json.dump(valid_list_data2, fp8, ensure_ascii=False, indent=2)
        print('data2: Valid data total %s' % len(valid_list_data2))

    with open(out_tq1, 'w') as fp8:
        json.dump(valid_list_tq01, fp8, ensure_ascii=False, indent=2)
        print('tq01: Valid data total %s' % len(valid_list_tq01))

    with open(out_tq2, 'w') as fp8:
        json.dump(valid_list_tq02, fp8, ensure_ascii=False, indent=2)
        print('tq02: Valid data total %s' % len(valid_list_tq02))

    with open(out_tq3, 'w') as fp8:
        json.dump(valid_list_tq03, fp8, ensure_ascii=False, indent=2)
        print('tq03: Valid data total %s' % len(valid_list_tq03))

    with open(out_tq4, 'w') as fp8:
        json.dump(valid_list_tq04, fp8, ensure_ascii=False, indent=2)
        print('tq04: Valid data total %s' % len(valid_list_tq04))


if __name__ == '__main__':
    start = time.time()
    extract_list()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))