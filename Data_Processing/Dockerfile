FROM amazoncorretto:17

# Install Anaconda
RUN yum -y update && yum install -y wget tar make
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    chmod +x Miniconda3-latest-Linux-x86_64.sh && \
    bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda && \
    rm Miniconda3-latest-Linux-x86_64.sh && \
    /opt/conda/bin/conda init && \
    echo "conda activate" >> ~/.bashrc


# ENV variables
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=hadoop3
ENV SPARK_DIR=/opt/spark

# Set up environment variables
ENV PATH $SPARK_DIR/bin:/opt/conda/bin:$PATH

# Create a conda environment and install required packages
# Install required packages using conda
RUN source /opt/conda/bin/activate && \
    conda install -y -c conda-forge bertopic scipy



# ...

# ENV PATH $SPARK_DIR/bin:$PATH

ADD setup/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz /opt

RUN yum -y update && yum install -y procps gcc openssl-devel bzip2-devel libffi-devel wget tar make

RUN yum -y install python3

RUN yum -y install glibc

ADD code/requirements.txt /code/requirements.txt

# Or else there will be build failures
RUN yum -y install python3-devel

# RUN pip3 install -r /code/requirements.txt
RUN conda install -y --file /code/requirements.txt

RUN conda install -c conda-forge kafka-python

# RUN pip3 install pyspark==3.4.0 elasticsearch==8.2.0 kafka-python==2.0.2 

RUN python3 -m spacy download en_core_web_sm

RUN ln -s /opt/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} ${SPARK_DIR}

ADD code /opt/tap/code

WORKDIR ${SPARK_DIR}