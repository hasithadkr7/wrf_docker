FROM centos:latest
MAINTAINER niranda perera <niranda.17@cse.mrt.ac.lk>

ENV CURW_VERSION="v2.0.0-snapshot"

#RUN curl -SL https://ral.ucar.edu/sites/default/files/public/projects/ncar-docker-wrf/ucar-bsd-3-clause-license.pdf > /UCAR-BSD-3-Clause-License.pdf
#
RUN yum -y update
RUN yum -y install file gcc gcc-gfortran gcc-c++
#RUN yum -y install net-tools epel-release

WORKDIR /work

RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm
RUN yum -y install git python36u python36u-pip python36u-devel mysql-devel

# install gcsfuse
RUN echo $'[gcsfuse] \nname=gcsfuse (packages.cloud.google.com) \nbaseurl=https://packages.cloud.google.com/yum/repos/gcsfuse-el7-x86_64 \nenabled=1 \ngpgcheck=1 \nrepo_gpgcheck=1 \ngpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg \n       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg '\
       > /etc/yum.repos.d/gcsfuse.repo
RUN yum -y update
RUN yum -y install gcsfuse

RUN pip3.6 install airflow airflow[mysql] airflow[celery]

# curwsl git repo
RUN cd /work

#RUN git clone https://github.com/nirandaperera/models.git && cd models && git checkout airflow-integ
RUN git clone -b $CURW_VERSION https://github.com/nirandaperera/models.git curwsl
RUN pip3.6 install -e curwsl/

COPY init_script.sh /work
RUN chmod +x /work/init_sript.sh

# update the git repo everytime a container is initialized
CMD ["/work/init_sript.sh"]

VOLUME /work

