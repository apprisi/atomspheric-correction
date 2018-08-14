#!/usr/bin/#!/usr/bin/env python3
import time
import glob
import json


def find_new_raw():
    """
       find the error data
    """

    landsat = glob.glob("/home/tq/*/landsat/*/01/038/032/*")
    landsat += glob.glob("/home/tq/*/landsat/*/01/032/036/*")

    landsat_T = [
        f
        for f in landsat
        if "RT" not in f
        and (
            "2010" in f[0:-15]
            or "2011" in f[0:-15]
            or "2012" in f[0:-15]
            or "2013" in f[0:-15]
            or "2014" in f[0:-15]
            or "2015" in f[0:-15]
            or "2016" in f[0:-15]
            or "2017" in f[0:-15]
            or "2018" in f[0:-15]
        )
    ]

    # extract the process list\
    result_json = "/home/tq/data_pool/test_data/new_landsat_20180814.json"
    with open(result_json, "w") as fp:
        json.dump(landsat_T, fp, ensure_ascii=False, indent=2)
    print("Valid data total %s" % len(landsat_T))


if __name__ == "__main__":

    start = time.time()
    find_new_raw()
    end = time.time()
    print("Task runs %0.2f seconds" % (end - start))
