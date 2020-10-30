FROM ubuntu:20.04
MAINTAINER Yong Wook Kim <ywkim@illinois.edu>

ARG VERSION="unknown"
ARG BUILDNUMBER="unknown"
ARG GITSHA1="unknown"

# copy requirements file
COPY requirements.txt /home/clowder/

# Install any programs needed
RUN apt-get update && \
       DEBIAN_FRONTEND=noninteractive apt-get install -y \
       file \
       python3 \
       python-is-python3 \
       python3-pip \
       python3-gdal \
       python3-rtree \
       python3-scipy \
       p7zip-full && \
       pip3 install -r /home/clowder/requirements.txt && \
       rm -rf /var/lib/apt/lists/*

# Switch to clowder, copy files and be ready to run
#USER clowder

# command to run when starting docker
ENTRYPOINT ["/home/clowder/entrypoint.sh"]
CMD ["extractor"]

# Setup environment variables. These are passed into the container. You can change
# these to your setup. If RABBITMQ_URI is not set, it will try and use the rabbitmq
# server that is linked into the container. MAIN_SCRIPT is set to the script to be
# executed by entrypoint.sh
ENV VERSION=${VERSION} \
    BUILDNUMBER=${BUILDNUMBER} \
    GITSHA1=${GITSHA1} \
    RABBITMQ_URI="" \
    RABBITMQ_EXCHANGE="clowder" \
    RABBITMQ_VHOST="%2F" \
    RABBITMQ_QUEUE="ncsa.geoshp.preview" \
    MAIN_SCRIPT="ncsa.geo.shp.py" \
    GEOSERVER_URL="" \
    GEOSERVER_USERNAME="admin" \
    GEOSERVER_PASSWORD="geoserver" \
    PROXY_URL='http://localhost:9000/api/proxy/' \
    PROXY_ON='false'

# copy rest of the files needed
COPY entrypoint.sh *.py extractor_info.json rasterTemplate.xml /home/clowder/
