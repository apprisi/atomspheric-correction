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
    
    # input process
    if not os.path.exists(imput_json):
        print("%s file path does not exist!\n" % imput_json)
        return -1
    elif not os.path.isfile(imput_json):
        print("%s is not a file.\n" % imput_json)
        return -1
    else:
        print("extracting the data.......\n")

    # save as tq03 and tq04
    output_le07 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2010_2017_le07.json'
    output_le07_tq03 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2010_2017_le07_tq03.json'
    output_le07_tq04 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2010_2017_le07_tq04.json'
    
    if not os.path.exists(imput_json):
        print("%s file path does not exist!\n" % imput_json)
        return -1
    elif not os.path.isfile(imput_json):
        print("%s is not a file.\n" % imput_json)
        return -1
    else:
        print("extracting the data.......\n")

        with open(imput_json, 'r') as fp:
            process_dict = json.load(fp)
            print(len(process_dict['scenes']))

       # extra path to list and remove cloudiness more than 80%
        valid_list_2010_2017_le07 = []
        for tmp_data in process_dict['scenes']:
            if tmp_data['cloud_perc'] <= 80:
                print(tmp_data['relative_path'] + "add to the list" )
                valid_list_2010_2017_le07.append(tmp_data['relative_path'])
            else:
                print(tmp_data['relative_path'] + "cloudiness more than 80%")
                continue  
              
        valid_list_2010_2017_le07_tq03 =  [pl for pl in valid_list_2010_2017_le07 if 'tq-data03' in pl]
        valid_list_2010_2017_le07_tq04 =  [pl for pl in valid_list_2010_2017_le07 if 'tq-data04' in pl]
        
        # sort the list
        valid_list_2010_2017_le07.sort()
        print(len(valid_list_2010_2017_le07))
        valid_list_2010_2017_le07_tq04.sort()
        valid_list_2010_2017_le07_tq03.sort()

        if len(valid_list_2010_2017_le07) > 0:
            with open(output_le07, 'w') as fp8:
                json.dump(valid_list_2010_2017_le07, fp8, ensure_ascii=False, indent=2)
            print('Valid data total %s' % (len(valid_list_2010_2017_le07)))
        else:
            print('LE07 is no data in %s' % imput_json)

        if  len(valid_list_2010_2017_le07_tq03) > 0:   
            with open(output_le07_tq03, 'w') as fp7:
                json.dump(valid_list_2010_2017_le07_tq03, fp7, ensure_ascii=False, indent=2)
            print('Valid data total %s in %s' % (len(valid_list_2010_2017_le07_tq03), imput_json))

        if  len(valid_list_2010_2017_le07_tq04) > 0:
            with open(output_le07_tq04, 'w') as fp5:
                json.dump(valid_list_2010_2017_le07_tq04, fp5, ensure_ascii=False, indent=2)
            print('Valid data total %s in %s' % (len(valid_list_2010_2017_le07_tq04), imput_json))
        print(len(process_dict['scenes']))
    return 1


if __name__ == '__main__':

    start = time.time()

    # open the file
    imput_json = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2013_le07_final.json'
    with open(imput_json, 'r') as fp:
        process_dict = json.load(fp)
    
    # dispart
    valid_list_2013_tq01 =  [pl for pl in  process_dict if 'tq-data01' in pl]
    process_dict_part1 = valid_list_2013_tq01[0:1500]
    process_dict_part2 = valid_list_2013_tq01[1500:]
    
    # output
    output_part1 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2013_le07_final_part1.json'
    output_part2 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2013_le07_final_part2.json'    

    if  len(process_dict_part1) > 0:   
            with open(output_part1, 'w') as fp7:
                json.dump(process_dict_part1, fp7, ensure_ascii=False, indent=2)
            print('Valid data total %s in %s' % (len(process_dict_part1), imput_json))

    if  len(process_dict_part2) > 0:
        with open(output_part2, 'w') as fp5:
            json.dump(process_dict_part2, fp5, ensure_ascii=False, indent=2)
        print('Valid data total %s in %s' % (len(process_dict_part2), imput_json))

    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))