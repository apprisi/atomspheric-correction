
#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json


def find_error():
    """
       find the error data
    """

    # extract the process list\
    result_json = '/home/jason/data_pool/sample_data/processed_list.json'
    process_list = []
    with open(result_json, 'r') as fp:
        process_dict = json.load(fp)

    for i in ['LE07', 'LT05']:
        for j in ['2010', '2011', '2012']:
            process_list.extend(process_dict[i]['Processed_list'][j])

    for i in ['LE07', 'LC08']:
        for j in ['2013', '2014', '2015', '2016', '2017']:
            process_list.extend(process_dict[i]['Processed_list'][j])

    print(len(process_list)) 

    process_set = set(process_list)

    processed_root = os.path.join('/home/jason', '*', 'landsat_sr', '*', '01', '*', '*', '*')
    processed_list = glob.glob(processed_root)
    print(len(processed_list))
    processed_set = set(processed_list)

    diff =  processed_set - process_set
    print(len(diff))
    for tmp in diff:
        print(tmp)   

if __name__ == '__main__':
    
    start = time.time()
    find_error()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
