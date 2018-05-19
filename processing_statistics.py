
#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import json

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
    processed_root = os.path.join('/home/jason', '*', 'landsat_sr', '*', '01', '*', '*', '*')
    processed_list = glob.glob(processed_root)

    # print total
    star40 = '*'*71
    print("%s\n*Total data: 85000, Total processed:%d, Processing Percentage:%.2f*\n%s" %
         (star40, len(processed_list), 100*len(processed_list)/85000, star40))

    # extract the year process list
    year = list(range(2010, 2019))
    for y in year:
        print("process %s data ...." % str(y))
        if y < 2013:
            sr_status['LT05']['Processed_list'][str(y)] = [pl for pl in processed_list if '/LT05/' in pl[:-15] and str(y) in pl[:-15]]
            sr_status['LE07']['Processed_list'][str(y)] = [pl for pl in processed_list if '/LE07/' in pl[:-15]  and str(y) in pl[:-15]]
        else:
            sr_status['LC08']['Processed_list'][str(y)] = [pl for pl in processed_list if '/LC08/' in pl[:-15] and str(y) in pl[:-15]]
            sr_status['LE07']['Processed_list'][str(y)] = [pl for pl in processed_list if '/LE07/' in pl[:-15] and str(y) in pl[:-15]]
    
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
          len(sr_status['LE07']['Processed_list']['2013']),len(sr_status['LE07']['Processed_list']['2012']),
          len(sr_status['LE07']['Processed_list']['2011']),len(sr_status['LE07']['Processed_list']['2010'])))
    print("LT05:\n*---->2012:%d\n*---->2011:%d\n*---->2010:%d\n" %
          (len(sr_status['LT05']['Processed_list']['2012']), len(sr_status['LT05']['Processed_list']['2011']),
          len(sr_status['LT05']['Processed_list']['2010'])))      
    print("%s" % star18)

    # save the process
    file_name = '/home/jason/data_pool/sample_data/processed_list.json'
    with open(file_name, 'w') as fp:
        json.dump(sr_status, fp, ensure_ascii=False, indent=4)       

if __name__ == '__main__':
    start = time.time()   
    processing_statistics()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
