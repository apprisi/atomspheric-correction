#!/bin/bash

Infile=$1
ZOOM=$2
if [[ $ZOOM == "" ]]; then
    echo "set default zoom 1-13"
    ZOOM="1-13"
fi
if [[ -f $Infile ]]; then
   echo "" > style.txt
   echo "1    50   205  50   250" >> style.txt 
   echo "2    220  220  220  0  " >> style.txt 
   echo "3    250  250  0    250" >> style.txt 
   echo "0    0    0    0    0  " >> style.txt
   echo "255  0    0    0    0  " >> style.txt
   echo "nv   0    0    0    0  " >> style.txt
   if [[ -f $Infile.rgb.tif ]]; then
       echo "$Infile.rgb.tif already created!!"
   else   
       gdaldem color-relief $Infile style.txt -alpha $Infile.rgb.tif
   fi
   gdal2tiles.py -p mercator -r near -z $ZOOM $Infile.rgb.tif $Infile.dir
   rm style.txt
else  
   echo "input file must a tif-like file!!"
fi

