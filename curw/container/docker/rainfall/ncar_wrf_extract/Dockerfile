FROM centos:latest
MAINTAINER niranda perera <niranda.17@cse.mrt.ac.lk>

ENV CURW_VERSION="v2.0.0-snapshot"

#RUN curl -SL https://ral.ucar.edu/sites/default/files/public/projects/ncar-docker-wrf/ucar-bsd-3-clause-license.pdf > /UCAR-BSD-3-Clause-License.pdf
#
RUN yum -y update
RUN yum -y install file gcc gcc-gfortran gcc-c++ glibc libgcc libpng-devel jasper jasper-devel hostname m4 make perl tar tcsh time wget which zlib zlib-devel openssh-clients openssh-server net-tools epel-release
#
# now get 3rd party EPEL builds of netcdf and openmpi dependencies
RUN yum -y install netcdf-openmpi-devel.x86_64 netcdf-fortran-openmpi-devel.x86_64 netcdf-fortran-openmpi.x86_64 hdf5-openmpi.x86_64 openmpi.x86_64 openmpi-devel.x86_64
#
WORKDIR /wrf

# Set environment for interactive container shells
#
RUN echo export LDFLAGS="-lm" >> /etc/bashrc \
 && echo export NETCDF=/wrf/netcdf_links >> /etc/bashrc \
 && echo export JASPERINC=/usr/include/jasper/ >> /etc/bashrc \
 && echo export JASPERLIB=/usr/lib64/ >> /etc/bashrc \
 && echo export LD_LIBRARY_PATH="/usr/lib64/openmpi/lib" >> /etc/bashrc \
 && echo export PATH="/usr/lib64/openmpi/bin:$PATH" >> /etc/bashrc \
 && echo setenv LDFLAGS "-lm" >> /etc/csh.cshrc \
 && echo setenv NETCDF "/wrf/netcdf_links" >> /etc/csh.cshrc \
 && echo setenv JASPERINC "/usr/include/jasper/" >> /etc/csh.cshrc \
 && echo setenv JASPERLIB "/usr/lib64/" >> /etc/csh.cshrc \
 && echo setenv LD_LIBRARY_PATH "/usr/lib64/openmpi/lib" >> /etc/csh.cshrc \
 && echo setenv PATH "/usr/lib64/openmpi/bin:$PATH" >> /etc/csh.cshrc

ENV LD_LIBRARY_PATH /usr/lib64/openmpi/lib
ENV PATH  /usr/lib64/openmpi/bin:$PATH

# install gcsfuse
RUN echo $'[gcsfuse] \nname=gcsfuse (packages.cloud.google.com) \nbaseurl=https://packages.cloud.google.com/yum/repos/gcsfuse-el7-x86_64 \nenabled=1 \ngpgcheck=1 \nrepo_gpgcheck=1 \ngpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg \n       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg '\
       > /etc/yum.repos.d/gcsfuse.repo
RUN yum -y update
RUN yum -y install gcsfuse

# install python3.6
RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm
RUN yum -y install git python36u python36u-pip python36u-devel python36u-tkinter
RUN pip3.6 install joblib shapely netCDF4 matplotlib imageio scipy geopandas numpy pandas pyshp

# install basemap
RUN curl -SL https://github.com/matplotlib/basemap/archive/v1.1.0.tar.gz | tar zxC /
RUN cd /basemap-1.1.0 \
 && cd geos-3.3.3 \
 && export GEOS_DIR=/ \
 && ./configure --prefix=$GEOS_DIR \
 && make; make install \
 && cd /basemap-1.1.0 \
 && python3.6 setup.py install

RUN cd /wrf
RUN mkdir gfs output geog config

RUN git clone -b $CURW_VERSION https://github.com/nirandaperera/models.git curwsl
RUN pip3.6 install -e curwsl/

RUN git clone https://github.com/gihankarunarathne/CurwMySQLAdapter.git curwsl_adapter
RUN pip3.6 install -e curwsl_adapter/

COPY extract_data_wrf.sh /wrf
RUN chmod +x /wrf/extract_data_wrf.sh

# update the git repo everytime a container is initialized
CMD ["/wrf/extract_data_wrf.sh"]

VOLUME /wrf

