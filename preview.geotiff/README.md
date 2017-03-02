GeoTiff Extractor

Overview

GeoTIFF is a public domain metadata standard which allows georeferencing information to be embedded within a TIFF file.

GeoTiff extractor communicates with GeoServer (https://geoserver.ncsa.illinois.edu/geoserver/web/) to get WMS metadata.

## Build a docker image
      docker build -t clowder/extractors-geotiff-preview .

## Test the docker container image:
      docker run --name=geotiff-preview -d --restart=always -e 'RABBITMQ_URI=amqp://user1:pass1@rabbitmq.ncsa.illinois.edu:5672/clowder-dev' -e 'RABBITMQ_EXCHANGE=clowder' -e 'TZ=/usr/share/zoneinfo/US/Central' -e 'REGISTRATION_ENDPOINTS=http://dts-dev.ncsa.illinois.edu:9000/api/extractors?key=key1' -e 'GEOSERVER_URL=geoserver url' -e 'GEOSERVER_PASSWORD=passwd' -e 'docker exec -it -u bdcli d47316abcb19 bashGEOSERVER_WORKSPACE=testing' -e 'GEOSERVER_USERNAME=username' clowder/extractors-geotiff-preview
