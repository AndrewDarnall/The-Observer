FROM amazoncorretto:17
LABEL maintainer="Salvo Nicotra"
# The container has trouble adding the $PATH
# ENV PATH /opt/kafka/bin:$PATH
ENV KAFKA_DIR "/opt/kafka"
ARG KAFKA_VERSION="2.13-3.4.1"

# Installing Kafka
# ADD will automatically extract the file
ADD setup/kafka_${KAFKA_VERSION}.tgz /opt

# Create Sym Link 
RUN ln -s /opt/kafka_${KAFKA_VERSION} ${KAFKA_DIR} 

ADD kafka-manager.sh ${KAFKA_DIR}/bin/

# Postponed
ENV PATH="$PATH:/opt/kafka/bin"
# Fixed with this, the docker container gave some issues without it, and made it hard to debug!
RUN chmod +x /opt/kafka/bin/kafka-manager.sh

# Copy All conf here
ADD conf/* ${KAFKA_DIR}/config/

ENTRYPOINT [ "kafka-manager.sh" ]