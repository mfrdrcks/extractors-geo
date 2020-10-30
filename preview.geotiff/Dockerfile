FROM alpine:3.12

MAINTAINER Yong Wook Kim <ywkim@illinois.edu>

ARG VERSION="unknown"
ARG BUILDNUMBER="unknown"
ARG GITSHA1="unknown"

# Install any programs needed
RUN apk add --no-cache python3 py3-pip py3-gdal p7zip gdal-tools

# copy requirements file
COPY requirements.txt /home/clowder/
RUN pip3 install --upgrade  -r /home/clowder/requirements.txt

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
    RABBITMQ_QUEUE="ncsa.geotiff.preview" \
    MAIN_SCRIPT="ncsa.geo.tiff.py" \
    GEOSERVER_URL="" \
    GEOSERVER_USERNAME="admin" \
    GEOSERVER_PASSWORD="geoserver" \
    PROXY_URL='http://localhost:9000/api/proxy/' \
    PROXY_ON='false' \
    GDALADDO_ARGS='--config COMPRESS_OVERVIEW JPEG --config JPEG_QUALITY_OVERVIEW 75 -r cubic' \
    GDALADDO_LEVELS='2 4 8 16 32 64 128 256 512 1024'

# copy rest of the files needed
COPY  *.py extractor_info.json rasterTemplate.xml /home/clowder/

# command to run when starting docker
CMD ["python3", "ncsa.geo.tiff.py"]
