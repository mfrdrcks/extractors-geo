# Clowder Geoshp/GeoTiff Metadata for PyCSW

Overview

pycsw extractor takes .zip or geotiff input file and communicates with PyCSW server to retrieve CSW metadata.

## Build a docker image
      docker build -t clowder/extractors-geo-pycsw .

## Test the docker container image:
      docker run --name=geo-pycsw -d --restart=always -e 'RABBITMQ_URI=amqp://user1:pass1@rabbitmq.ncsa.illinois.edu:5672/clowder-dev' -e 'RABBITMQ_EXCHANGE=clowder' -e 'TZ=/usr/share/zoneinfo/US/Central' -e 'REGISTRATION_ENDPOINTS=http://dts-dev.ncsa.illinois.edu:9000/api/extractors?key=key1'  -e 'GEOSERVER_URL=geoserver url' -e 'GEOSERVER_PASSWORD=passwd' -e 'docker exec -it -u bdcli d47316abcb19 bashGEOSERVER_WORKSPACE=testing' -e 'GEOSERVER_USERNAME=username' clowder/extractors-geoshp-preview

## To run without docker

This extractor uses the python modules GDAL.
Installing GDAL requires gdal libraries.
On Ubunut, do "sudo apt-get install python-gdal"; on Mac OS X, do
"brew install gdal". Then install the modules in the requirements.txt file.

While following the instructions below, please note that
on Ubuntu, installing gdal in a virtualenv seems
problematic, and using the system environment could prove easier.
The other steps are the same.

To install and run the python extractor, do the following:

1. Setup a [virtualenv](https://virtualenv.pypa.io), e.g., named "geopycsw":

   `virtualenv geopycsw`
2. Activate the virtualenv

   `source geopycsw/bin/activate`
3. Install required python packages using *pip*

   `pip install -r requirements.txt`
4. Install pyclowder if it is not installed yet.

   `pip install git+https://opensource.ncsa.illinois.edu/stash/scm/cats/pyclowder.git`

   or if you have pyclowder checked out as well (useful when developing)

   `ln -s ../../pyClowder/pyclowder pyclowder`
5. Modify config.py 
6. Start extractor

   `./ncsa.geo.pycsw.py`

# Setting up pycsw server as a docker service
1. Download pycsw.cfg locally: https://github.com/geopython/pycsw/blob/master/docker/pycsw.cfg

2. Modify following section:
```
    [manager]
    transactions=true
    allowed_ips=127.0.0.1,0.0.0.0/0
```

3. Create a docker secret with the pycsw config file
    `docker secret create pycsw-config <path-to-local-pycsw.cfg>`
    
    
4. Create a docker volume for persisting the database
    `docker volume create pycsw-db-data`

5. Run the docker service and persist the sqlite database
```
docker service create \
    --name ncsa_pycsw_server \
    --mount type=volume,source=db-data,destination=/var/lib/pycsw,volume-label="pycsw-db-data" \
    --mode replicated \
    --replicas 1 \
    --secret source=pycsw-config,target=/etc/pycsw/pycsw.cfg \
    --publish 8000:8000 \
    --constraint 'node.role == worker' \
    geopython/pycsw
 ```
