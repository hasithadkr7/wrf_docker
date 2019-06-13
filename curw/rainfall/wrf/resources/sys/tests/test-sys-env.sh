#!/bin/bash  
test_home="/mnt/disks/wrf-mod/tests"
echo "----------- test_home dir" 
echo $test_home


echo "----------- clean up the test dir"
rm -rf $test_home/tests
mkdir -p $test_home/tests
cd $test_home/tests

echo '----------- fortran'
which gfortran

echo '----------- cpp'
which cpp

echo '----------- gcc'
which gcc
gcc --version 

echo '----------- Running Fortran and C Tests'

echo "copying test files"
wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/Fortran_C_tests.tar
tar -xf Fortran_C_tests.tar

echo "### test 1"
gfortran TEST_1_fortran_only_fixed.f
./a.out

echo "### test 2"
gfortran TEST_2_fortran_only_free.f90
./a.out

echo "### test 3"
gcc TEST_3_c_only.c
./a.out

echo "### test 4"
gcc -c -m64 TEST_4_fortran+c_c.c
gfortran -c -m64 TEST_4_fortran+c_f.f90
gfortran -m64 TEST_4_fortran+c_f.o TEST_4_fortran+c_c.o
./a.out

echo "### test csh"
./TEST_csh.csh

echo "### test perl"
./TEST_perl.pl

echo "### test sh"
./TEST_sh.sh

