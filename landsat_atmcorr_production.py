#!/usr/bin/env python3
import os, time, glob
import shutil, json
import subprocess
from datetime import datetime


class LandsatAtmcorr:
    def __init__(self, process_path, result_root):
        self.process_path = process_path
        self.result_root = result_root
        self.home_dir = os.path.expanduser("~")
        self.server_list = ["tq-data03", "tq-data04", "tq-data02", "tq-data01"]
        self.lasrc_aux_dir = os.path.join(self.home_dir, "data_pool/lasrc_aux")
        self.ledaps_aux_dir = os.path.join(self.home_dir, "data_pool/ledaps_aux")

    def check_process_status(self) -> bool:
        """
        Function:
            Check whether the data has been processed
        input:
            process data path
            such as 'tq-data01/landsat/LE07/01/011/028/LE07_L1TP_011028_20010211_20161001_01_T1'
        output:
            True: return result path
            False: return false
        """

        # check the result
        for tmp_server in self.server_list:
            tmp_path = os.path.join(
                self.home_dir,
                tmp_server,
                "landsat_sr",
                self.process_path.split("landsat/")[1],
            )
            if os.path.exists(tmp_path):
                if len([fps for fps in os.listdir(tmp_path) if "_sr_" in fps]) == 16:
                    print(tmp_path + " has been procesed!")
                    return True, tmp_path
                else:
                    shutil.rmtree(tmp_path)
            else:
                continue
        return False, None

    def check_process_data(self) -> bool:
        """
        Function:
            Check that the download data is complete
        Input:
            process data path
            such as 'tq-data01/landsat/LE07/01/011/028/LE07_L1TP_011028_20010211_20161001_01_T1'
        Output:
            True or False
        """

        process__full_path = os.path.join(self.home_dir, self.process_path)
        if not os.path.exists(process__full_path):
            print("%s process path does not exist!" % process__full_path)
            return False

        tif_list = [fps for fps in os.listdir(process__full_path) if ".TIF" in fps]
        txt_list = [fps for fps in os.listdir(process__full_path) if ".txt" in fps]
        if "/LC08/" in self.process_path:
            if len(tif_list) == 12 and len(txt_list) == 2:
                return True
            else:
                print(process__full_path, "download has someting wrong!")
                return False
        elif "/LE07/" in process__full_path and "_T1" in process__full_path:
            if len(tif_list) == 10 and len(txt_list) == 3:
                return True
            else:
                print(process__full_path, "download has someting wrong!")
                return False
        elif "/LE07/" in process__full_path and "_T2" in process__full_path:
            if len(tif_list) == 10 and (len(txt_list) == 2 or len(txt_list) == 3):
                return True
            else:
                print(process__full_path, "download has someting wrong!")
                return False
        elif "/LT05/" in process__full_path and "_T1" in process__full_path:
            if len(tif_list) == 8 and len(txt_list) == 4:
                return True
            else:
                print(process__full_path, "download has someting wrong!")
                return False
        elif "/LT05/" in process__full_path and "_T2" in process__full_path:
            if len(tif_list) == 8 and len(txt_list) == 2:
                return True
            else:
                print(process__full_path, "download has someting wrong!")
                return False
        else:
            print(process__full_path, "is not landsat 5/7/8 and please check the data!")
            return False

    def ymd_to_doy(self) -> str:
        """
        Function:
            get process date and convert string '20010101' to day of year(DOY) '2001001'
        Input:
            date string like '20010101'
        Output:
            day of year '2001001'
        """
        process_date = os.path.split(self.process_path)[1].split("_")[3]  # get date
        time_struct = datetime.strptime(
            process_date, "%Y%m%d"
        ).timetuple()  # date convert to time struct
        return str(time_struct.tm_year) + "{:0>3}".format(time_struct.tm_yday)

    def check_aux_data(self) -> bool:
        """
        Function:
            using DOY to check the auxiliary data
        Input:
            date string DOY like '2010052'
        Output:
            Ture or False
        """

        DOY = self.ymd_to_doy()
        year = DOY[0:4]
        if "/LE07/" in self.process_path:
            rnls_file_name = "REANALYSIS_" + DOY + ".hdf"
            aux_rnls_path = os.path.join(
                self.ledaps_aux_dir, "REANALYSIS/RE_" + year, rnls_file_name
            )
            toms_file_name = "TOMS_" + DOY + ".hdf"
            aux_toms_path = os.path.join(
                self.ledaps_aux_dir, "EP_TOMS/ozone_" + year, toms_file_name
            )
            if os.path.exists(aux_rnls_path) and os.path.exists(aux_toms_path):
                return True
            else:
                print(self.process_path, "has not processed!")
                return False
        elif "/LC08/" in self.process_path:
            aux_file_name = "L8ANC" + DOY + ".hdf_fused"
            aux_file_path = os.path.join(
                self.lasrc_aux_dir, "LADS", year, aux_file_name
            )
            if os.path.exists(aux_file_path):
                return True
            else:
                print(self.process_path, "has not processed!")
                return False
        elif "/LT05/" in self.process_path:
            aux_file_path = os.path.join(self.ledaps_aux_dir, "L5_TM/gnew.dat")
            if os.path.exists(aux_file_path):
                return True
            else:
                print(self.process_path, "has not processed!")
                return False

    def atmos_correct_pre(self) -> (str, bool):
        """
        Function:
            copy the raw data to local path and covert tiled tiff to stripped tiff
        input:
            process_path is as follows
            /../../landsat/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2
        output:
            local_path: process sucess, and return the temporary path
        """

        # after check, then can run the proprecess
        local_path = os.path.join(
            self.home_dir,
            "tq-tmp",
            "landsat_sr",
            self.process_path.split("landsat/")[1],
        )
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        # copy the txt to result path and convert the TIFF
        txt_list = glob.glob(os.path.join(self.home_dir, self.process_path, "*.txt"))
        tif_list = glob.glob(os.path.join(self.home_dir, self.process_path, "*.TIF"))
        try:
            for tif in tif_list:
                _, tif_name = os.path.split(tif)
                ret1 = subprocess.run(
                    [
                        "gdal_translate",
                        "-co",
                        "TILED=NO",
                        tif,
                        os.path.join(local_path, tif_name),
                    ]
                )
                if ret1.returncode == 0:
                    continue
                else:
                    print("%s conversion failed!" % tif)
                    return None, False

            for txt in txt_list:
                _, txt_name = os.path.split(txt)
                shutil.copyfile(txt, os.path.join(local_path, txt_name))

            # change the directory, remove the IMD file
            IMD_list = glob.glob(os.path.join(local_path, "*.IMD"))
            for imd in IMD_list:
                os.remove(imd)
            print("IMD file is deleted!")
        except Exception as e:
            print(e)
            return None, False
        return local_path, True

    def atmos_correct_process(self, local_path: str) -> bool:
        """
        Function:
            process the landsat 7&8 data atomospheric correction
        input:
            local_path is as follows
            /../../landsat_sr/LE07/01/010/028/LE07_L1GT_010028_20040503_20160926_01_T2
        output:
            return ture or false
        """

        # check the MTL.txt
        os.chdir(local_path)  # must be change
        mtl_txt = glob.glob("*_MTL.txt")[0]

        # convert lpgs to espa
        ret1 = subprocess.run(
            ["convert_lpgs_to_espa", "--mtl", mtl_txt, "--del_src_files"]
        )
        if ret1.returncode != 0:
            print("%s data format conversion failed!" % mtl_txt)
            return False
        else:
            print("%s data format conversion completed!" % mtl_txt)

        # atomspheric correct
        mtl_xml = glob.glob("*.xml")[0]  # read XML
        if "LC08" in mtl_xml:
            ret2 = subprocess.run(["do_lasrc.py", "--xml", mtl_xml])
            if ret2.returncode == 0:
                print("%s data atomospheric correction processing finished!" % mtl_xml)
                return True
            else:
                print("%s data atomospheric correction processing failure!" % mtl_xml)
                return False
        elif "LE07" in mtl_xml or "LT05" in mtl_xml:
            ret2 = subprocess.run(["do_ledaps.py", "--xml", mtl_xml])
            if ret2.returncode == 0:
                print("%s data atomospheric correction processing finished!" % mtl_xml)
                return True
            else:
                print("%s data atomospheric correction processing failure!" % mtl_xml)
                return False

    def atmos_correct_post(self, local_path: str) -> (str, bool):
        """
        Function:
            deleted the atomospheric correction temporary data and move the reult data to result path
        input:
            result_root: such as tq-data02
        output:
            return result path
        """

        # check the pixel_qa
        pixel_qa = glob.glob(os.path.join(local_path, "*_pixel_qa.img"))  # read XML
        if len(pixel_qa) < 1:
            print(local_path + " will generate the pixel_qa!")
        elif len(pixel_qa) == 1:
            print(local_path + " has generated the pixel_qa!")

        # check the XML
        os.chdir(local_path)  # must be change
        xml = glob.glob("*.xml")[0]

        # process BQA to pixel_qa
        ret1 = subprocess.run(["generate_pixel_qa", "--xml", xml])
        if ret1.returncode == 0:
            print("%s generate pixel_qa sucessfully!" % local_path)

            # dilate_pixel_qa --xml --bit 5 --distance 3
            ret1 = subprocess.run(
                ["dilate_pixel_qa", "--xml", xml, "--bit=5", "--distance=3"]
            )
            if ret1.returncode == 0:
                print("%s dilate_pixel_qa sucessfully!" % local_path)
            else:
                print("%s failed to dilate_pixel_qa!" % local_path)
                return None, False
        else:
            print("%s failed to generate pixel_qa!" % local_path)
            return None, False

        # IMG to TIF
        base_name = os.path.split(local_path)[1]
        ret1 = subprocess.run(
            [
                "convert_espa_to_gtif",
                "--xml",
                xml,
                "--gtif",
                base_name,
                "--del_src_files",
            ]
        )
        if ret1.returncode != 0:
            print("%s data conversion failed!" % xml)
            return None, False
        else:
            print("%s data conversion completed!" % xml)

        # deleted the tmp data
        tif_list = glob.glob(os.path.join(local_path, "*_b[1-9]*"))
        toa_list = glob.glob(os.path.join(local_path, "*_toa_*"))
        angle_list = glob.glob(os.path.join(local_path, "*_s*r_*th_*"))
        txt_list = glob.glob(os.path.join(local_path, "*.txt"))
        for deleted_list in tif_list + angle_list + toa_list + txt_list:
            if os.path.exists(deleted_list):
                os.remove(deleted_list)
            else:
                print("no such file:%s" % deleted_list)

        # copy the file from local to reulst path
        result_path = local_path.replace("tq-tmp", self.result_root)
        try:
            shutil.move(local_path, result_path)
            shutil.rmtree(os.path.join(self.home_dir, "tq-tmp/landsat_sr"))
        except Exception as e:
            print(e)
            return None, False
        return result_path, True

    def go_process(self) -> int:
        """
        Function:
            ****************atomspheric correction****************
            Step 1: check process data status
            Step 2: check process data
            Step 3: check auxiliary data
            Step 4: atmospheric correction preprocess
            Step 5: atmospheric correction process
            Step 6: atmospheric correction postprocess
        ouput:
            process_status
            0: sucess
            1: data has been processed
            2: donload data has problem
            3: auxiliary data must be download
            4: atmospheric correction preprocess failed
            5: atmospheric correction process failed
            6: atmospheric correction postprocess failed
        """

        # Step 1
        print(self.process_path)
        print("Step 1: will be check process data status!")
        flag, result_path = self.check_process_status()
        if flag:
            process_status = 1
            return result_path, process_status
        else:
            print("Step 2: will be check the download data!")

        # Step 2
        flag = self.check_process_data()
        if not flag:
            process_status = 2
            return None, process_status
        else:
            print("Step 3: will be check the check auxiliary data!")

        # Step 3
        flag = self.check_aux_data()
        if not flag:
            process_status = 3
            return None, process_status
        else:
            print("Step 4: atmospheric correction preprocess.")

        # Step 4
        local_path, flag = self.atmos_correct_pre()
        if not flag:
            process_status = 4
            return None, process_status
        else:
            print("Step 5: atmospheric correction process.")

        # Step 5
        flag = self.atmos_correct_process(local_path)
        if not flag:
            process_status = 5
            return None, process_status
        else:
            print("Step 6: atmospheric correction postprocess.")

        # step 6
        result_path, flag = self.atmos_correct_post(local_path)
        if not flag:
            process_status = 6
            return None, process_status
        else:
            process_status = 0
            print(self.process_path, "finished.")
            return result_path, process_status


if __name__ == "__main__":
    start = time.time()
    process_path = (
        "tq-data01/landsat/LE07/01/037/029/LE07_L1TP_037029_20161211_20170106_01_T1"
    )

    result_root = "tq-data01"
    LA = LandsatAtmcorr(process_path, result_root)
    flag = LA.go_process()
    print("process_status:", flag)
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
