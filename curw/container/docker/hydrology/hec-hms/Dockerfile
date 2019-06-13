FROM i386/ubuntu:16.04
LABEL authors="hasitha adikari <hasitha.10@cse.mrt.ac.lk>, niranda perera <niranda.17@cse.mrt.ac.lk>"

RUN apt -y update && apt -y upgrade && apt -y autoremove

RUN DEBIAN_FRONTEND=noninteractive apt -y install \
 libc6:i386 \
 libstdc++6:i386 \
 libxtst6:i386 \
 libxrender1:i386 \
 libgcc1:i386 \
 libxi6:i386 \
 software-properties-common python-software-properties zip unzip wget build-essential net-tools gcc git jq \
 python3 python3-pip python3-psycopg2 \
 xorg openbox \
 jq \
 mysql-client \
 vim

RUN git clone https://github.com/hasithadkr7/udp_150.git /hec-hms \
 && pip3 install git+https://github.com/gihankarunarathne/CurwMySQLAdapter.git

RUN pip3 install google-cloud-storage

#RUN git clone https://github.com/hasithadkr7/udp_150.git /hec-hms \
# && pip3 install git+https://github.com/gihankarunarathne/CurwMySQLAdapter.git


COPY run.sh /run.sh
RUN chmod a+x /run.sh

#COPY config.json /hec-hms/CONFIG.json

WORKDIR /hec-hms

# Download hec-hms421-linux.tar.gz
RUN wget 'http://www.hec.usace.army.mil/software/hec-hms/downloads/hec-hms421-linux.tar.gz' \
 && tar -xzf hec-hms421-linux.tar.gz \
 && rm -f hec-hms421-linux.tar.gz

#-----------Uncomment later usage---------#
RUN wget 'http://www.hec.usace.army.mil/software/hec-dssvue/downloads/hec-dssvue201-linux.bin.zip' \
 && unzip hec-dssvue201-linux.bin.zip \
 && yes | ./hec-dssvue201.bin \
 && rm -f hec-dssvue201-linux.bin.zip hec-dssvue201.bin

RUN wget 'https://excellmedia.dl.sourceforge.net/project/jython/jython/2.5.0/jython_installer-2.5.0.jar' \
 &&  /hec-hms/hec-dssvue201/java/bin/java -jar jython_installer-2.5.0.jar -s -d /hec-hms/jython

RUN cd /hec-hms/jython \
 && cp jython.jar jythonlib.jar \
 && zip -r jythonlib.jar Lib \
 && mv /hec-hms/jython/jythonlib.jar /hec-hms/hec-dssvue201/jar/sys \
 && rm -f /hec-hms/jython_installer-2.5.0.jar \
 && rm -rf /hec-hms/jython

RUN wget 'https://pypi.python.org/packages/fc/b0/847668a90540ad96d88a8adf48d489468fbb6dbb219567b7ed8fd4d9b8b5/simplejson-2.5.0.tar.gz' \
 && tar -xzf simplejson-2.5.0.tar.gz \
 && rm -f simplejson-2.5.0.tar.gz

#RUN mkdir model git input 2008_2_Events_Hack 2008_2_Events
RUN mkdir Rainfall Raincell Meanref Subref config
RUN chmod +x FilesFromBucket.py
RUN chmod +x FilesToBucket.py
RUN chmod +x BucketConfig.py
COPY hec-dssvue.sh /hec-hms/hec-dssvue201/hec-dssvue.sh
COPY uwcc-admin.json /hec-hms/uwcc-admin.json

CMD ["/run.sh"]
