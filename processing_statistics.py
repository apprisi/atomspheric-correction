#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json
import datetime

sr_status ={'LT05': 
                {'Processed_list': 
                    {'2010': [],
                     '2011': [],
                     '2012': []}},
            'LE07': 
                {'Processed_list':
                    {'2010': [],
                     '2011': [],
                     '2012': [],
                     '2013': [],
                     '2014': [],
                     '2015': [],
                     '2016': [],
                     '2017': [],
                     '2018': []}},
            'LC08': 
                {'Processed_list':
                    {'2013': [],
                     '2014': [],
                     '2015': [],
                     '2016': [],
                     '2017': [],
                     '2018': []}},
            'VersionInfo':
                {'ESPA' : '1.15.0',
                'LEDAPS' : '3.3.0',
                'LaSRC' : '1.4.0'}}

def processing_statistics():
    """
    ptocessing statistics of landsat
    data save:/haome/jason/*/landsat_sr
    """

    # extract the process path
    processed_root = os.path.join('/home/jason', 'tq-data*', 'landsat_sr', '*', '01', '*', '*', '*')
    processed_list = glob.glob(processed_root)

    # find the sucess and fail
    processed_list_fail = []
    processed_list_sucess = []

    for tmp in processed_list:
        if len([fps for fps in os.listdir(tmp) if '_sr_' in fps ]) == 16:
            processed_list_sucess.append(tmp[tmp.find('jason')+6:])       
        else:
            processed_list_fail.append(tmp[tmp.find('jason')+6:])

    print(len(processed_list_sucess))
    file_sucess = '/home/jason/data_pool/sample_data/processed_list_sucess.txt'
    with open(file_sucess, 'w') as fp:
         fp.write(str(processed_list_sucess) + '\n')

    file_fail = '/home/jason/data_pool/sample_data/processed_list_fail.txt'
    with open(file_fail, 'w') as fp:
         fp.write(str(processed_list_fail) + '\n')

    # print total
    star40 = '*'*75
    time_stamp = datetime.datetime.now()
    f = open('/home/jason/data_pool/sample_data/process_log.txt', 'a')
    print("%s\n*  Total data: 85000, Total processed:%d, Processing Percentage:%.2f  *\n%s" %
         (star40, len(processed_list), 100*len(processed_list)/85000, star40))

    print("%s------->Total data: 85000, Total processed:%d, Processing Percentage:%.2f\n" %
         (time_stamp.strftime("%Y-%m-%d %H:%M:%S %p"), len(processed_list), 100*len(processed_list)/85000), file = f)
    
    f.close()
    # extract the year process list
    year = list(range(2010, 2019))
    for y in year:
        if y < 2013:
            sr_status['LT05']['Processed_list'][str(y)] = [pl for pl in processed_list_sucess if 'LT05_' in pl[:-18] and str(y) in pl[:-18]]
            sr_status['LE07']['Processed_list'][str(y)] = [pl for pl in processed_list_sucess if 'LE07_' in pl[:-18] and str(y) in pl[:-18]]
        elif y >= 2013:
            sr_status['LC08']['Processed_list'][str(y)] = [pl for pl in processed_list_sucess if 'LC08_' in pl[:-18] and str(y) in pl[:-18]]
            sr_status['LE07']['Processed_list'][str(y)] = [pl for pl in processed_list_sucess if 'LE07_' in pl[:-18] and str(y) in pl[:-18]]
    
    # print result
    star18 = '*'*30
    print("%s" % star18)
    print("LC08:\n*---->2017:%d\n*---->2016:%d\n*---->2015:%d\n*---->2014:%d\n*---->2013:%d\n" %
          (len(sr_status['LC08']['Processed_list']['2017']), len(sr_status['LC08']['Processed_list']['2016']),
          len(sr_status['LC08']['Processed_list']['2015']), len(sr_status['LC08']['Processed_list']['2014']),
          len(sr_status['LC08']['Processed_list']['2013'])))
    print("LE07:\n*---->2017:%d\n*---->2016:%d\n*---->2015:%d\n*---->2014:%d\n*---->2013:%d\n*---->2012:%d\n*---->2011:%d\n*---->2010:%d\n" %
          (len(sr_status['LE07']['Processed_list']['2017']), len(sr_status['LE07']['Processed_list']['2016']),
          len(sr_status['LE07']['Processed_list']['2015']), len(sr_status['LE07']['Processed_list']['2014']),
          len(sr_status['LE07']['Processed_list']['2013']), len(sr_status['LE07']['Processed_list']['2012']),
          len(sr_status['LE07']['Processed_list']['2011']), len(sr_status['LE07']['Processed_list']['2010'])))
    print("LT05:\n*---->2012:%d\n*---->2011:%d\n*---->2010:%d\n" %
          (len(sr_status['LT05']['Processed_list']['2012']), len(sr_status['LT05']['Processed_list']['2011']),
          len(sr_status['LT05']['Processed_list']['2010'])))      
    print("%s" % star18)
    
    # save the process
    file_name = '/home/jason/data_pool/sample_data/processed_list.json'
    with open(file_name, 'w') as fp:
        json.dump(sr_status, fp, ensure_ascii=False, indent=4)  

if __name__ == '__main__':
    
    print("Process list will be maked.")
    while 1==1:
        processing_statistics()
        time.sleep(3600)
