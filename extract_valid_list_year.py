#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json


def extract_vaild_path_year(imput_json):
    """
    Funciton:
        1. extract the data for every year 

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

    # creat outfile
    output_2010_le07 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2010_le07_final.json'
    output_2011_le07 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2011_le07_final.json'
    output_2012_le07 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2012_le07_final.json'
    output_2013_le07 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2013_le07_final.json'
    output_2014_le07 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2014_le07_final.json'
    output_2015_le07 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2015_le07_final.json'
    output_2016_le07 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2016_le07_final.json'
    output_2017_le07 = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2017_le07_final.json'

    # open the json
    
    with open(imput_json, 'r') as fp:
        process_dict = json.load(fp)

    # extract the data
    valid_list_2010_le07_final = [pl for pl in process_dict if '2010' in pl[:-18]]
    valid_list_2011_le07_final = [pl for pl in process_dict if '2011' in pl[:-18]]
    valid_list_2012_le07_final = [pl for pl in process_dict if '2012' in pl[:-18]]
    valid_list_2013_le07_final = [pl for pl in process_dict if '2013' in pl[:-18]]
    valid_list_2014_le07_final = [pl for pl in process_dict if '2014' in pl[:-18]]
    valid_list_2015_le07_final = [pl for pl in process_dict if '2015' in pl[:-18]]
    valid_list_2016_le07_final = [pl for pl in process_dict if '2016' in pl[:-18]]
    valid_list_2017_le07_final = [pl for pl in process_dict if '2017' in pl[:-18]]
  
    # save the data         
    if len(valid_list_2010_le07_final) > 0:
        with open(output_2010_le07, 'w') as fp8:
            json.dump(valid_list_2010_le07_final, fp8, ensure_ascii=False, indent=2)
        print('2010 --> Valid data total %s in %s' % (len(valid_list_2010_le07_final), imput_json))
    
    if len(valid_list_2011_le07_final) > 0:
        with open(output_2011_le07, 'w') as fp8:
            json.dump(valid_list_2011_le07_final, fp8, ensure_ascii=False, indent=2)
        print('2011 --> Valid data total %s in %s' % (len(valid_list_2011_le07_final), imput_json))

    if len(valid_list_2012_le07_final) > 0:
        with open(output_2012_le07, 'w') as fp8:
            json.dump(valid_list_2012_le07_final, fp8, ensure_ascii=False, indent=2)
        print('2012 --> Valid data total %s in %s' % (len(valid_list_2012_le07_final), imput_json))
    
    if len(valid_list_2013_le07_final) > 0:
        with open(output_2013_le07, 'w') as fp8:
            json.dump(valid_list_2013_le07_final, fp8, ensure_ascii=False, indent=2)
        print('2013 --> Valid data total %s in %s' % (len(valid_list_2013_le07_final), imput_json))

    if len(valid_list_2014_le07_final) > 0:
        with open(output_2014_le07, 'w') as fp8:
            json.dump(valid_list_2014_le07_final, fp8, ensure_ascii=False, indent=2)
        print('2014 --> Valid data total %s in %s' % (len(valid_list_2014_le07_final), imput_json))
    
    if len(valid_list_2015_le07_final) > 0:
        with open(output_2015_le07, 'w') as fp8:
            json.dump(valid_list_2015_le07_final, fp8, ensure_ascii=False, indent=2)
        print('2015 --> Valid data total %s in %s' % (len(valid_list_2015_le07_final), imput_json))

    if len(valid_list_2016_le07_final) > 0:
        with open(output_2016_le07, 'w') as fp8:
            json.dump(valid_list_2016_le07_final, fp8, ensure_ascii=False, indent=2)
        print('2016 --> Valid data total %s in %s' % (len(valid_list_2016_le07_final), imput_json))
    
    if len(valid_list_2017_le07_final) > 0:
        with open(output_2017_le07, 'w') as fp8:
            json.dump(valid_list_2017_le07_final, fp8, ensure_ascii=False, indent=2)
        print('2017 --> Valid data total %s in %s' % (len(valid_list_2017_le07_final), imput_json))

if __name__ == '__main__':
    start = time.time()
    imput_json = '/home/jason/data_pool/sample_data/SRC_DATA_JSON/LE07/valid_list_2010_2017_le07.json'
    extract_vaild_path_year(imput_json)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))