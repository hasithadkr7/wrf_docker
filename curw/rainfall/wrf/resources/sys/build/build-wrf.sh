#!/bin/bash

mount_dir="wrf-mod"
wrf_home="/mnt/disks/$mount_dir"
echo "----------- wrf_home dir= $wrf_home"

cd $wrf_home

read -p "Enter compilation case name: " -e -i em_real case_name

read -p "Enter compilation file name: " -e -i log.wrf.compile file_name

echo "Using case: $case_name and log: $file_name"

if [ ! -f $wrf_home/src/WRFV3.8.1.TAR.gz ]; then
	echo "--- Downloading wrfv3.8.1"
	wget http://www2.mmm.ucar.edu/wrf/src/WRFV3.8.1.TAR.gz
	mv WRFV3.8.1.TAR.gz $wrf_home/src/
fi

# rm -r WRFV3
if [ ! -d $wrf_home/WRFV3 ]; then
echo "--- unzipping wrf"
	tar -xvzf $wrf_home/src/WRFV3.8.1.TAR.gz
fi

cd $wrf_home/WRFV3

source /etc/profile
echo "--- cleaning wrf"
./clean -a
echo "--- configuring wrf"
./configure
echo "--- compiling wrf"
./compile $case_name | tee $file_name 

echo "--- testing for *.exe"
ls -ls main/*.exe
