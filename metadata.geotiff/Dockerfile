FROM alpine:edge

MAINTAINER Yong Wook Kim <ywkim@illinois.edu>

ARG VERSION="unknown"
ARG BUILDNUMBER="unknown"
ARG GITSHA1="unknown"

# copy requirements file
COPY requirements.txt /home/clowder/

RUN apk update && \
    apk add make automake gcc g++ subversion python3-dev && \
    apk add --no-cache python3 py3-pip py3-gdal py3-numpy && \
    pip3 install -r /home/clowder/requirements.txt


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
    RABBITMQ_QUEUE="ncsa.geotiff.metadata" \
    MAIN_SCRIPT="ncsa.image.geotiff.py"

# copy rest of the files needed
COPY entrypoint.sh *.py extractor_info.json /home/clowder/
