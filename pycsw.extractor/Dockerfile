FROM clowder/pyclowder:2
MAINTAINER Yong Wook Kim <ywkim@illinois.edu>

# copy requirements file
COPY requirements.txt /home/clowder/

# Install any programs needed
#USER root
RUN apt-get update && apt-get install -y \
       file \
       gdal-bin \
       python-gdal \
       p7zip-full \
       python-pyproj \
       git \
    && pip install --upgrade pip==9.0.3 \
    && pip install -r /home/clowder/requirements.txt \
    && rm -rf /var/lib/apt/lists/*

# Switch to clowder, copy files and be ready to run
#USER clowder

# command to run when starting docker
ENTRYPOINT ["/home/clowder/entrypoint.sh"]
CMD ["extractor"]

# Setup environment variables. These are passed into the container. You can change
# these to your setup. If RABBITMQ_URI is not set, it will try and use the rabbitmq
# server that is linked into the container. MAIN_SCRIPT is set to the script to be
# executed by entrypoint.sh
ENV RABBITMQ_URI="" \
    RABBITMQ_EXCHANGE="clowder" \
    RABBITMQ_VHOST="%2F" \
    RABBITMQ_QUEUE="ncsa.pycsw.extractor" \
    MAIN_SCRIPT="ncsa.geo.pycsw.py" \
    GEOSERVER_URL="" \
    GEOSERVER_WORKSPACE="clowder" \
    PYCSW_URL="http://localhost:8000/pycsw" \
    PROXY_URL='http://localhost:9000/api/proxy/' \
    PROXY_ON='false'

# copy rest of the files needed
COPY entrypoint.sh *.py extractor_info.json pycsw_insert_template.xml pycsw_remove_template.xml /home/clowder/
