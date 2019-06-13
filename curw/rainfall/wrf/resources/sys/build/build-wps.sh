#!/bin/bash

lib_home="/opt/lib"
mount_dir="wrf-mod"
wrf_home="/mnt/disks/$mount_dir"
echo "----------- wrf_home dir= $wrf_home"

cd $wrf_home

read -p "Enter log file name: " -e -i log.wps.compile file_name

echo "Using log: $file_name"

if [ ! -f $wrf_home/src/WPSV3.8.1.TAR.gz ]; then
	echo "--- Downloading WPSV3.8.1.TAR.gz if not exists"
	wget http://www2.mmm.ucar.edu/wrf/src/WPSV3.8.1.TAR.gz
	mv WPSV3.8.1.TAR.gz $wrf_home/src/
fi

echo "--- unzipping WPS"
if [ ! -d $wrf_home/WPS ]; then
	tar -xvzf $wrf_home/src/WPSV3.8.1.TAR.gz
fi

cd $wrf_home/WPS

source /etc/profile

echo "--- cleaning WPS"
./clean
echo "--- configuring WPS"
./configure
echo "--- compling WPS"
./compile | tee $file_name 
echo "--- checking the *.exe"
ls -ls *.exe

