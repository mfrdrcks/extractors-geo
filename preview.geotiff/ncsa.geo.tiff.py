#!/usr/bin/env python

import logging
import os
import tempfile

from urlparse import urlparse
from urlparse import urljoin

from pyclowder.extractors import Extractor
from pyclowder.utils import StatusMessage
import pyclowder.files
from osgeo import gdal

import geotiffutils as gu
import gsclient as gs

from geoserver.catalog import Catalog

class ExtractorsGeotiffPreview(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    # ----------------------------------------------------------------------
    # Process the file and upload the results
    def process_message(self, connector, host, secret_key, resource, parameters):
        self.extractorName = os.getenv('RABBITMQ_QUEUE', "ncsa.geoshp.preview")
        self.messageType = ["*.file.image.tiff", "*.file.image.tif"]
        self.geoServer = os.getenv('GEOSERVER_URL')
        self.gs_username = os.getenv('GEOSERVER_USERNAME', 'admin')
        self.gs_password = os.getenv('GEOSERVER_PASSWORD', 'geoserver')
        self.gs_workspace = os.getenv('GEOSERVER_WORKSPACE', 'clowder')
        self.proxy_url = os.getenv('PROXY_URL', 'http://localhost:9000/api/proxy/')
        self.proxy_on = os.getenv('PROXY_ON', 'false')
        self.raster_style = "rasterTemplate.xml"

        self.secret_key = secret_key
        self.logger = logging.getLogger(__name__)

        """Process the geotiff and create geoserver layer"""
        filename = resource['name']
        inputfile = resource["local_paths"][0]
        fileid = resource['id']

        tmpfile = None

        try:
            # call actual program
            result = self.extractGeotiff(inputfile, fileid, filename)

            if not result['WMS Layer URL'] or not result['WMS Service URL'] or not result['WMS Layer URL']:
                self.logger.info('[%s], inputfile: %s has empty result', fileid, inputfile)

            # store results as metadata
            elif not result['isGeotiff'] or len(result['errorMsg']) > 0:
                channel = parameters['channel']
                header = parameters['header']
                for i in range(len(result['errorMsg'])):
                    connector.status_update(StatusMessage.error, {"type": "file", "id": fileid},
                                            result['errorMsg'][i])
                    self.logger.info('[%s] : %s', fileid, result['errorMsg'][i], extra={'fileid': fileid})
            else:
                # Context URL
                context_url = "https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"
                metadata = {
                    "@context": [
                        context_url,
                        {
                            'WMS Layer Name': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#WMS Layer Name',
                            'WMS Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#WMS Service URL',
                            'WMS Layer URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#WMS Layer URL'
                        }
                    ],
                    'attachedTo': {'resourceType': 'file', 'id': parameters["id"]},
                    'agent': {
                        '@type': 'cat:extractor',
                        'extractor_id': 'https://clowder.ncsa.illinois.edu/clowder/api/extractors/' + self.extractorName},
                    'content': {
                        'WMS Layer Name': result['WMS Layer Name'],
                        'WMS Service URL': result['WMS Service URL'],
                        'WMS Layer URL': result['WMS Layer URL']
                    }
                }

                # register geotiff preview
                (_, ext) = os.path.splitext(inputfile)
                (_, tmpfile) = tempfile.mkstemp(suffix=ext)
                # extractors.upload_preview(previewfile=tmpfile, parameters=parameters)
                # logger.debug("upload previewer")
                # extractors.upload_file_metadata_jsonld(mdata=metadata, parameters=parameters)
                # logger.debug("upload file metadata")
                pyclowder.files.upload_metadata(connector, host, secret_key, fileid, metadata)
                self.logger.debug("upload file metadata")

        except Exception as ex:
            self.logger.debug(ex.message)
        finally:
            try:
                os.remove(tmpfile)
                self.logger.debug("delete tmpfile: " + tmpfile)
            except OSError:
                pass

    def remove_geoserver_layer(self, layer_name):
        cat = Catalog(self.geoServer, username=self.gs_username, password=self.gs_password)
        # worksp = cat.get_workspace(gs_workspace)
        store = cat.get_store("store_name")
        layer = cat.get_layer("layer_name")
        cat.delete(layer)
        cat.reload()
        cat.delete(store)
        cat.reload

    def extractGeotiff(self, inputfile, fileid, filename):
        storeName = fileid
        msg = {}
        msg['errorMsg'] = []
        msg['WMS Layer Name'] = ''
        msg['WMS Service URL'] = ''
        msg['WMS Layer URL'] = ''
        msg['isGeotiff'] = False

        uploadfile = inputfile

        geotiffUtil = gu.Utils(uploadfile, self.raster_style)

        if not geotiffUtil.hasError():
            msg['isGeotiff'] = True
            if self.proxy_on.lower() == 'true':
                geoserver_url = self.geoServer
                parsed_uri = urlparse(geoserver_url)
                gs_domain = u'{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                self.geoServer = geoserver_url.replace(gs_domain, self.proxy_url)

            gsclient = gs.Client(self.geoServer, self.gs_username, self.gs_password)

            epsg = "EPSG:" + str(geotiffUtil.getEpsg())
            style = None

            # check if the input geotiff has a style,
            # you can do this by checking if there is any color table
            uploadfile_dataset = gdal.Open(uploadfile)
            uploadfile_band = uploadfile_dataset.GetRasterBand(1)
            color_table = uploadfile_band.GetColorTable()
            if color_table is not None:
                self.logger.debug("Geotiff has the style already")
            else:
                style = geotiffUtil.createStyle()
                self.logger.debug("style created")

            # merge file name and id and make a new store name
            combined_name = filename + "_" + storeName

            success = gsclient.uploadGeotiff(self.gs_workspace, combined_name, uploadfile, filename, style, epsg, self.secret_key, self.proxy_on)
            if success:
                self.logger.debug("upload geotiff successfully")
                metadata = gsclient.mintMetadata(self.gs_workspace, combined_name, geotiffUtil.getExtent())
                self.logger.debug("mintMetadata obtained")
                if len(metadata) == 0:
                    msg['errorMsg'].append("Coulnd't generate metadata")
                else:
                    msg['WMS Layer Name'] = metadata['WMS Layer Name']
                    if self.proxy_on.lower() == 'true':
                        msg['WMS Service URL'] = urljoin(self.proxy_url, 'geoserver/wms')
                        # create layer url by switching geoserver url to geoserver proxy url
                        wms_layer_url = metadata['WMS Layer URL']
                        parsed_uri = urlparse(wms_layer_url)
                        gs_domain = u'{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                        wms_layer_url = wms_layer_url.replace(gs_domain, self.proxy_url)
                        msg['WMS Layer URL'] = wms_layer_url
                    else:
                        msg['WMS Service URL'] = metadata['WMS Service URL']
                        msg['WMS Layer URL'] = metadata['WMS Layer URL']
            else:
                msg['errorMsg'].append("Fail to upload the file to geoserver")
        else:
            if not geotiffUtil.isGeotiff:
                msg['isGeotiff'] = False
                msg['errorMsg'].append("Normal TIFF file")
                return msg

            if geotiffUtil.getEpsg() == 'UNKNOWN':
                msg['errorMsg'].append("The projection ccould not be recognized")

            if geotiffUtil.getExtent() == 'UNKNOWN':
                msg['errorMsg'].append("The extent could not be calculated")

        return msg


if __name__ == "__main__":
    extractor = ExtractorsGeotiffPreview()
    extractor.start()
