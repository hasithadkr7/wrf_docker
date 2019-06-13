#!/bin/bash

echo "installing prerequisite software"
apt-get update

apt-get install -y gcc
apt-get install -y g++
apt-get install -y gfortran
apt-get install -y wget
apt-get install -y make
apt-get install -y tcsh
apt-get install -y m4

apt-get install -y libpng-dev
apt-get install -y ncl-ncarg
apt-get install -y imagemagick
apt-get install -y poppler-utils
apt-get install -y nco

apt-get install -y python-pip
export LC_ALL=C
pip install netcdf4
pip install pyshp
pip install shapely
pip install wget
pip install pyyaml
pip install numpy
pip install pandas
pip install joblib