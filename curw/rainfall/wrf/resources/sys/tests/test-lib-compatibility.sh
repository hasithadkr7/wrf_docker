#!/bin/bash

test_home="/mnt/disks/wrf-mod/tests"
echo "----------- test_home dir" 
echo $test_home

cd $test_home/tests

echo "--- Downloading tests if not exists"
if [ ! -f Fortran_C_NETCDF_MPI_tests.tar ]; then
	echo "No test tar. Downloading... "
	wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/Fortran_C_NETCDF_MPI_tests.tar
fi
tar -xf Fortran_C_NETCDF_MPI_tests.tar

source /etc/profile

echo "--- test 1"
cp ${NETCDF}/include/netcdf.inc .
gfortran -c 01_fortran+c+netcdf_f.f
gcc -c 01_fortran+c+netcdf_c.c
gfortran 01_fortran+c+netcdf_f.o 01_fortran+c+netcdf_c.o \
     -L${NETCDF}/lib -lnetcdff -lnetcdf
./a.out

echo "--- test 2"
 cp ${NETCDF}/include/netcdf.inc .
 mpif90 -c 02_fortran+c+netcdf+mpi_f.f
 mpicc -c 02_fortran+c+netcdf+mpi_c.c
 mpif90 02_fortran+c+netcdf+mpi_f.o \
 02_fortran+c+netcdf+mpi_c.o \
      -L${NETCDF}/lib -lnetcdff -lnetcdf
 mpirun ./a.out
