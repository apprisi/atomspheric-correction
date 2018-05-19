
#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import shutil


def extract_data():

    root = r'/home/jason/tq-data03/landsat_sr/LE07'
    lc08 = os.path.join(root, '*', '*', '*', '*LC08*')
    lc08_path = glob.glob(lc08)
    print(len(lc08_path))
    for tmp in lc08_path:
        print(tmp + "\n")
        result_path = tmp.replace('LE07', 'LC08')
        print(result_path + "\n")
        os.makedirs(result_path)
        shutil.move(tmp, result_path)

def remove_data():
    """
    remove the no process file
    """
    root = r'/home/jason'
    tmp_path = os.path.join(root, '*', 'landsat_sr', '*', '01', '*', '*', 'L*T?', 'L*T?')

    print(tmp_path)
    all_path = glob.glob(tmp_path)
    print(all_path)
    for tmp in all_path:
    #     print(tmp)
    #     if '_sr_' in ''.join(os.listdir(tmp)):
    #         continue
    #     elif not os.listdir(tmp): # deleted the empty data
    #         os.rmdir(tmp)
    #     else:
        shutil.rmtree(tmp)

    # all_path = glob.glob(tmp_path)
    # print(len(all_path))

if __name__ == '__main__':
    start = time.time()   
    remove_data()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
