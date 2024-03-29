FROM centos:latest
MAINTAINER niranda perera <niranda.17@cse.mrt.ac.lk>
# 
# This Dockerfile compiles WRF from source during "docker build" step
ENV WRF_VERSION 3.9.1.1
ENV WPS_VERSION 3.9.1

ENV CURW_VERSION="v2.0.0-snapshot"

#RUN curl -SL https://ral.ucar.edu/sites/default/files/public/projects/ncar-docker-wrf/ucar-bsd-3-clause-license.pdf > /UCAR-BSD-3-Clause-License.pdf
#
RUN yum -y update
RUN yum -y install file gcc gcc-gfortran gcc-c++ glibc libgcc libpng-devel jasper jasper-devel hostname m4 make perl tar tcsh time wget which zlib zlib-devel openssh-clients openssh-server net-tools epel-release
#
# now get 3rd party EPEL builds of netcdf and openmpi dependencies
RUN yum -y install netcdf-openmpi-devel.x86_64 netcdf-fortran-openmpi-devel.x86_64 netcdf-fortran-openmpi.x86_64 hdf5-openmpi.x86_64 openmpi.x86_64 openmpi-devel.x86_64 nco
#
WORKDIR /wrf
#
# Download original sources
#
RUN curl -SL http://www2.mmm.ucar.edu/wrf/src/WRFV$WRF_VERSION.TAR.gz | tar zxC /wrf \
 && curl -SL http://www2.mmm.ucar.edu/wrf/src/WPSV$WPS_VERSION.TAR.gz | tar zxC /wrf
#
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
#
# Build WRF first
# 
# input 34 and 1 to configure script alternative line = && echo -e "34\r1\r" | ./configure
RUN mkdir netcdf_links \
 && ln -sf /usr/include/openmpi-x86_64/ netcdf_links/include \
 && ln -sf /usr/lib64/openmpi/lib netcdf_links/lib \
 && export NETCDF=/wrf/netcdf_links \
 && export JASPERINC=/usr/include/jasper/ \
 && export JASPERLIB=/usr/lib64/ \
 && cd ./WRFV3 \
 && ./configure <<< $'34\r1\r' \
 && sed -i -e '/^DM_CC/ s/$/ -DMPI2_SUPPORT/' ./configure.wrf \
 && /bin/csh ./compile em_real
#
# Build WPS second
#
# input 1 to configure script
RUN cd ./WPS \
 && export NETCDF=/wrf/netcdf_links \
 && export JASPERINC=/usr/include/jasper/ \
 && export JASPERLIB=/usr/lib64/ \
 && ./configure <<< $'1\r' \
 && sed -i -e 's/-L$(NETCDF)\/lib/-L$(NETCDF)\/lib -lnetcdff /' ./configure.wps \
 && /bin/csh ./compile
#
ENV LD_LIBRARY_PATH /usr/lib64/openmpi/lib
ENV PATH  /usr/lib64/openmpi/bin:$PATH
#
# copy in a couple custom scripts
#COPY run-wrf /wrf
#COPY docker-clean /wrf
#RUN chmod +x /wrf/run-wrf \
# && chmod +x /wrf/docker-clean
COPY docker-clean /wrf
RUN chmod +x /wrf/docker-clean
#
# set up ssh configuration
COPY ssh_config /root/.ssh/config
RUN mkdir -p /root/.openmpi
COPY default-mca-params.conf /root/.openmpi/mca-params.conf
RUN yum clean all \
    && /wrf/docker-clean \
    && mkdir -p /var/run/sshd \
    && ssh-keygen -A \
    && sed -i 's/#PermitRootLogin yes/PermitRootLogin yes/g' /etc/ssh/sshd_config \
    && sed -i 's/#RSAAuthentication yes/RSAAuthentication yes/g' /etc/ssh/sshd_config \
    && sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/g' /etc/ssh/sshd_config \
    && ssh-keygen -f /root/.ssh/id_rsa -t rsa -N '' \
    && chmod 600 /root/.ssh/config \
    && chmod 700 /root/.ssh \
    && cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
#

RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm
RUN yum -y install git python36u python36u-pip python36u-devel
RUN pip3.6 install joblib shapely

# install gcsfuse
RUN echo $'[gcsfuse] \nname=gcsfuse (packages.cloud.google.com) \nbaseurl=https://packages.cloud.google.com/yum/repos/gcsfuse-el7-x86_64 \nenabled=1 \ngpgcheck=1 \nrepo_gpgcheck=1 \ngpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg \n       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg '\
       > /etc/yum.repos.d/gcsfuse.repo
RUN yum -y update
RUN yum -y install gcsfuse

# curwsl git repo
RUN cd /wrf
RUN mkdir gfs output geog archive config

#RUN git clone https://github.com/nirandaperera/models.git && cd models && git checkout airflow-integ
RUN git clone -b $CURW_VERSION https://github.com/nirandaperera/models.git curwsl
RUN pip3.6 install -e curwsl/

COPY run_wrf.sh /wrf
RUN chmod +x /wrf/run_wrf.sh

# update the git repo everytime a container is initialized
CMD ["/wrf/run_wrf.sh"]

VOLUME /wrf

