#!/bin/bash

mount_dir="wrf-mod"
wrf_home="/mnt/disks/$mount_dir"
lib_home="/opt/lib"
echo "----------- lib_home dir =" $lib_home

mkdir -p $wrf_home/src

mkdir -p $lib_home
cd $lib_home

echo "--- downloading libraries"
if [ ! -f $wrf_home/src/netcdf-4.1.3.tar.gz ]; then
	echo "netcdf-4.1.3.tar.gz not found! Downloading... "
	wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/netcdf-4.1.3.tar.gz
fi

if [ ! -f $wrf_home/src/jasper-1.900.1.tar.gz ]; then
	echo "jasper-1.900.1.tar.gz not found! Downloading... "
	wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/jasper-1.900.1.tar.gz
fi

if [ ! -f $wrf_home/src/libpng-1.2.50.tar.gz ]; then
	echo "jasper-1.900.1.tar.gz not found! Downloading... "
	wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/libpng-1.2.50.tar.gz
fi

if [ ! -f $wrf_home/src/zlib-1.2.7.tar.gz ]; then
	echo "jasper-1.900.1.tar.gz not found! Downloading... "
	wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/zlib-1.2.7.tar.gz
fi

if [ ! -f $wrf_home/src/mpich-3.0.4.tar.gz ]; then
	echo "mpich-3.0.4.tar.gz not found! Downloading... "
	wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/mpich-3.0.4.tar.gz
fi

mv *.tar.gz $wrf_home/src/

echo "--- installing netcdf"

	export DIR=$lib_home
	export CC=gcc
	export CXX=g++
	export FC=gfortran
	export FCFLAGS=-m64
	export F77=gfortran
	export FFLAGS=-m64


if [ ! -d netcdf-4.1.3 ]; then 
	echo "netcdf not present. installing... "

	#rm -rf netcdf-4.1.3
	tar xzvf $wrf_home/src/netcdf-4.1.3.tar.gz 
	cd netcdf-4.1.3
	./configure --prefix=$DIR/netcdf --disable-dap \
	--disable-netcdf-4 --disable-shared
	make
	make install
	source  /etc/profile
	echo "export PATH=$DIR/netcdf/bin:\$PATH" >> /etc/profile
	echo "export NETCDF=$DIR/netcdf"  >> /etc/profile
	cd ..
fi 

echo "--- installing mpich"
if [ ! -d mpich-3.0.4 ]; then 
	source  /etc/profile
	tar xzvf $wrf_home/src/mpich-3.0.4.tar.gz
	cd mpich-3.0.4
	./configure --prefix=$DIR/mpich
	make
	make install
	echo "export PATH=$DIR/mpich/bin:\$PATH"  >> /etc/profile
	cd ..
fi


echo "--- installing zlib"
if [ ! -d zlib-1.2.7 ]; then 
	source  /etc/profile
	echo "export LDFLAGS=-L$DIR/grib2/lib"  >> /etc/profile
	echo "export CPPFLAGS=-I$DIR/grib2/include"  >> /etc/profile
	echo "export JASPERLIB=$lib_home/grib2/lib"  >> /etc/profile
	echo "export JASPERINC=$lib_home/grib2/include"  >> /etc/profile
	tar xzvf $wrf_home/src/zlib-1.2.7.tar.gz     #or just .tar if no .gz present
	cd zlib-1.2.7
	./configure --prefix=$DIR/grib2
	make
	make install
	cd ..
fi

echo "--- installing libpng"
if [ ! -d libpng-1.2.50 ]; then 
	source  /etc/profile
	tar xzvf $wrf_home/src/libpng-1.2.50.tar.gz     #or just .tar if no .gz present
	cd libpng-1.2.50
	./configure --prefix=$DIR/grib2
	make
	make install
	cd ..
fi

echo "--- installing jasper-1.900.1"
if [ ! -d jasper-1.900.1 ]; then 
	source  /etc/profile
	tar xzvf $wrf_home/src/jasper-1.900.1.tar.gz     #or just .tar if no .gz present
	cd jasper-1.900.1
	./configure --prefix=$DIR/grib2
	make
	make install
	cd ..
fi


