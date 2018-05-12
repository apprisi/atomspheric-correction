#!/usr/bin/#!/usr/bin/env python3
import os
import time
import glob
import re
import subprocess


def combineL8AuxDataProcess(data_path, output_dir):
    """
    Function:
        combine_l8_aux_data derives auxiliary data from the Aqua/Terra CMG and
        CMA files (for the same day).  The output is a "fused" HDF file
        containing the desired SDSs and data for that day. The application
        fills in the holes of the Terra data with the Aqua data.
    input:
    data_path: it is the data path such as /home/jason/data_pool/modis
       data_path = /home/jason/data_pool/modis
            /MOA09CMA/year/doy/*.hdf
            /MOD09CMG/year/doy/*.hdf
            /MYD09CMA/year/doy/*.hdf
            /MYD09CMG/year/doy/*.hdf
    output:
      	return 0, process sucess
        return 1, process failure
        return -1, data not exists
        output_dir = /home/jason/data_pool/lasrc_aux/LADS/combine_result
    """

    if not os.path.exists(data_path):
        print("%s file path does not exist!" % data_path)
        return -1
    elif not os.path.isdir(data_path):
        print("%s is not a file directory" % data_path)
        return -1
    else:
        print("Combine L8 auxiliary data.......\n")
        terra_cma = glob.glob(os.path.join(data_path, 'MOD09CMA', '*', '*', '*'))
        terra_cma.sort()
        terra_cmg = glob.glob(os.path.join(data_path, 'MOD09CMG', '*', '*', '*'))
        terra_cmg.sort()
        aqua_cma = glob.glob(os.path.join(data_path, 'MYD09CMA', '*', '*', '*'))
        aqua_cma.sort()
        aqua_cmg = glob.glob(os.path.join(data_path, 'MYD09CMG', '*', '*', '*'))
        aqua_cmg.sort()
        for t_cma, t_cmg, a_cma, a_cmg in zip(terra_cma, terra_cmg, aqua_cma, aqua_cmg):
            start = time.time()
            doy   = re.findall(r"\d\d\d+", t_cma)
            result_file = os.path.join(output_dir, doy[0], 'L8ANC' + doy[2] + '.hdf_fused')
            if os.path.exists(result_file):
                print(result_file + " has processed! it no need to process again.")
                continue
            else:
                [o_dir, _] = os.path.split(result_file)
                ret1  = subprocess.run(['combine_l8_aux_data', '--terra_cma', t_cma,
                                    '--terra_cmg', t_cmg, '--aqua_cma', a_cma,
                                    '--aqua_cmg',a_cmg,'--output_dir', o_dir,
                                    '--verbose'])
                if (ret1.returncode != 0):
                    print("%s\n%s\n%s\n%s\n Data conbine failed!" % (t_cma, t_cmg,
                        a_cma, a_cmg))
                else:
                    print("%s\n%s\n%s\n%s\n Data conbine completed!" % (t_cma, t_cmg,
                        a_cma, a_cmg))
                end = time.time()
                print("%s executed using time %.2f seconds" % (ret1.args, (end - start)))

if __name__ == '__main__':
    data_path = r'/home/jason/data_pool/modis'
    output_dir = r'/home/jason/data_pool/lasrc_aux/LADS'
    start = time.time()
    flags = combineL8AuxDataProcess(data_path, output_dir)
    print("Process status:%s" % flags)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
