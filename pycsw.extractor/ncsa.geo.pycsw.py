#!/usr/bin/env python

import requests
import logging
import json
import subprocess
import os
import tempfile
import urlparse

# from pyclowder import extractors
from pyclowder.extractors import Extractor
from pyclowder.utils import StatusMessage
import pyclowder.files

from osgeo import gdal

import zipshputils as zs
import geotiffutils as gu
import pycswutils as pu

# to post layer to pycsw
import os, sys, inspect

######################################
# https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/pyclowder2/browse/sample-extractors/wordcount/wordcount.py
######################################

class PycswExtractor(Extractor):
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
        self.extractorName = os.getenv('RABBITMQ_QUEUE', "ncsa.pycsw.extractor")
        self.messageType = ["*.file.multi.files-zipped.#",
                       "*.file.application.zip",
                       "*.file.application.x-zip",
                       "*.file.application.x-7z-compressed",
                       "*.file.image.tiff",
                       "*.file.image.tif"
                       ]
        self.geoServer = os.getenv('GEOSERVER_URL')
        self.gs_workspace = os.getenv('GEOSERVER_WORKSPACE', 'clowder')
        self.proxy_url = os.getenv('PROXY_URL')
        self.proxy_on = os.getenv('PROXY_ON', 'false')
        self.raster_style = "rasterTemplate.xml"

        self.pycsw_server = os.getenv('PYCSW_URL', 'http://141.142.60.190:8000/pycsw')

        self.secret_key = secret_key
        self.logger = logging.getLogger(__name__)

        """Process the compressed shapefile and create geoserver layer"""
        tmpfile = None
        try:
            filename = resource['name']
            inputfile = resource["local_paths"][0]
            fileid = resource['id']
            combined_name = filename + "_" + fileid
            fil_baseename, file_extension = os.path.splitext(filename)
            is_shp = False
            is_geotiff = False

            # check file extension if it is zip or tiff
            if file_extension.lower() == '.zip':
                is_shp = True
            if file_extension.lower() == '.tiff' or file_extension.lower() == '.tif':
                is_geotiff = True

            if is_shp:
                # call actual program
                result = self.extractZipShp(inputfile, fileid, filename)

                # store results as metadata
                if not result['isZipShp'] or len(result['errorMsg']) > 0:
                    channel = parameters['channel']
                    header = parameters['header']
                    for i in range(len(result['errorMsg'])):
                        connector.status_update(StatusMessage.error, {"type": "file", "id": fileid},
                                                result['errorMsg'][i])
                        self.logger.info('[%s] : %s', fileid, result['errorMsg'][i], extra={'fileid', fileid})
                else:
                    # Context URL
                    context_url = "https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"

                    metadata = {
                        "@context": [
                            context_url,
                            {
                                'CSW Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#CSW Service URL',
                                'CSW Record URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#CSW Record URL'
                            }
                        ],
                        'attachedTo': {'resourceType': 'file', 'id': parameters["id"]},
                        'agent': {
                            '@type': 'cat:extractor',
                            'extractor_id': 'https://clowder.ncsa.illinois.edu/clowder/api/extractors/' + self.extractorName},
                        'content': {
                            'CSW Service URL': result['CSW Service URL'],
                            'CSW Record URL': result['CSW Record URL']
                        }
                    }

                    # post shapefile layer to pycsw
                    wmsserver = urlparse.urljoin(self.geoServer, 'wms')
                    layer_name = self.gs_workspace + ":" + combined_name
                    layer_url = wmsserver + '?request=GetMap&layers=' + layer_name + '&bbox=' + result['Shp Extent']\
                                + '&width=640&height=480&srs=EPSG:3857&format=image%2Fpng'
                    result = self.post_layer_to_pycsw(layer_name, layer_url, True)

                    # upload metadata
                    # self.extractor.upload_file_metadata_jsonld(mdata=metadata, parameters=parameters)
                    pyclowder.files.upload_metadata(connector, host, secret_key, fileid, metadata)
                    self.logger.debug("upload file metadata")
            elif is_geotiff:   # geotiff
                # call actual program
                result = self.extractGeotiff(inputfile, fileid, filename)

                # store results as metadata
                if not result['isGeotiff'] or len(result['errorMsg']) > 0:
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
                                'CSW Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#CSW Service URL',
                                'CSW Record URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#CSW Record URL'
                            }
                        ],
                        'attachedTo': {'resourceType': 'file', 'id': parameters["id"]},
                        'agent': {
                            '@type': 'cat:extractor',
                            'extractor_id': 'https://clowder.ncsa.illinois.edu/clowder/api/extractors/' + self.extractorName},
                        'content': {
                            'CSW Service URL': result['CSW Service URL'],
                            'CSW Record URL': result['CSW Record URL']
                        }
                    }

                    # post shapefile layer to pycsw
                    wmsserver = urlparse.urljoin(self.geoServer, 'wms')
                    layer_name = self.gs_workspace + ":" + combined_name
                    layer_url = wmsserver + '?request=GetMap&layers=' + layer_name + '&bbox=' + result['Tiff Extent'] \
                                + '&width=640&height=480&srs=EPSG:3857&format=image%2Fpng'
                    result = self.post_layer_to_pycsw(layer_name, layer_url, False)

                    pyclowder.files.upload_metadata(connector, host, secret_key, fileid, metadata)
                    self.logger.debug("upload file metadata")
        except Exception as ex:
            self.logger.debug(ex.message)
        finally:
            try:
                self.logger.debug("Finished posting pycsw server entry")
            except OSError:
                pass


    """
    post layer information to pycsw server
    """
    def post_layer_to_pycsw(self, layer_name, layer_url, is_feature):
        pycswutil = pu.Utils()
        bbox_list = pycswutil.parse_bbox_from_url(layer_url)
        layer_title = layer_name.split(':')[1]

        xml_identifier = layer_name
        xml_reference = layer_name

        xml_subject = layer_title
        xml_keyword = [layer_name.split(':')[0], layer_name.split(':')[1]]
        xml_title = layer_title
        xml_lower_corner = str(bbox_list[0]) + " " + str(bbox_list[1])
        xml_upper_corner = str(bbox_list[2]) + " " + str(bbox_list[3])
        if (is_feature):
            xml_isFeature = 'features'
        else:
            xml_isFeature = 'GeoTIFF'

        xml_str = pycswutil.construct_insert_xml(xml_identifier, xml_reference, xml_isFeature, xml_subject,
                                          xml_keyword,
                                          xml_title,
                                          xml_lower_corner, xml_upper_corner)
        result = pycswutil.post_insert_xml(self.pycsw_server, xml_str, self.secret_key, self.proxy_on, self.proxy_url)

        return result


    def extractZipShp(self, inputfile, fileid, filename):
        storename = fileid
        msg = {}
        msg['errorMsg'] = []
        msg['CSW Service URL'] = ''
        msg['CSW Record URL'] = ''
        msg['Shp Extent'] = ''
        msg['isZipShp'] = False
        combined_name = filename + "_" + storename

        zipshp = zs.Utils(inputfile)
        if not zipshp.hasError():
            msg['isZipShp'] = True
            if self.proxy_on.lower() == 'true':
                cswserver = urlparse.urljoin(self.proxy_url, 'geoserver/csw')
                cswrecord = cswserver + "?service=CSW&version=2.0.2&request=GetRecordById&elementsetname=summary" \
                                        "&id=" + self.gs_workspace + ":" + combined_name + "&typeNames=gmd:MD_Metadata" \
                                                                                      "&resultType=results&elementSetName=full&outputSchema" \
                                                                                      "=http://www.isotc211.org/2005/gmd"
                msg['Shp Extent'] = zipshp.getExtent()
                msg['CSW Service URL'] = cswserver
                msg['CSW Record URL'] = cswrecord
            else:
                cswserver = urlparse.urljoin(self.geoServer, 'csw')
                cswrecord = cswserver + "?service=CSW&version=2.0.2&request=GetRecordById&elementsetname=summary" \
                                        "&id=" + self.gs_workspace + ":" + combined_name + "&typeNames=gmd:MD_Metadata" \
                                        "&resultType=results&elementSetName=full&outputSchema" \
                                         "=http://www.isotc211.org/2005/gmd"
                msg['Shp Extent'] = zipshp.getExtent()
                msg['CSW Service URL'] = cswserver
                msg['CSW Record URL'] = cswrecord

            result = subprocess.check_output(['file', '-b', '--mime-type', inputfile], stderr=subprocess.STDOUT)
            self.logger.info('result.strip is [%s]', result.strip())
            if result.strip() != 'application/zip':
                msg['errorMsg'].append('result.strip is: ' + str(result.strip()))
                return msg

            if zipshp.getEpsg() == 'UNKNOWN' or zipshp.getEpsg() == None:
                epsg = "EPSG:4326"
            else:
                epsg = "EPSG:" + zipshp.getEpsg()
        else:
            error = zipshp.zipShpProp
            if error['shpFile'] == None:
                msg['isZipShp'] = False
                msg['errorMsg'].append("normal compressed file")
                return msg

            if error['hasDir']:
                msg['errorMsg'].append("a compressed shapefile can not have directory")
                return msg

            if error['numShp'] > 1:
                msg['errorMsg'].append("a compressed shapefile can not have multiple shpefiles")
                return msg

            if error['hasSameName'] == False:
                msg['errorMsg'].append("a shapefile files (.shp, .shx, .dbf, .prj) should have same name")
                return msg

            if error['shxFile'] == None:
                msg['errorMsg'].append(".shx file is missing")

            if error['dbfFile'] == None:
                msg['errorMsg'].append(".dbf file is missing")

            if error['prjFile'] == None:
                msg['errorMsg'].append(".prj file is missing")

            if error['epsg'] == 'UNKNOWN':
                msg['errorMsg'].append("The projection ccould not be recognized")

            if error['extent'] == 'UNKNOWN':
                msg['errorMsg'].append("The extent could not be calculated")
        return msg

    def extractGeotiff(self, inputfile, fileid, filename):
        storeName = fileid
        msg = {}
        msg = {}
        msg['errorMsg'] = []
        msg['CSW Service URL'] = ''
        msg['CSW Record URL'] = ''
        msg['Tiff Extent'] = ''
        combined_name = filename + "_" + storeName

        uploadfile = inputfile

        geotiffUtil = gu.Utils(uploadfile, self.raster_style)

        if not geotiffUtil.hasError():
            msg['isGeotiff'] = True
            if self.proxy_on.lower() == 'true':
                cswserver = urlparse.urljoin(self.proxy_url, 'geoserver/csw')
                cswrecord = cswserver + "?service=CSW&version=2.0.2&request=GetRecordById&elementsetname=summary" \
                                        "&id=" + self.gs_workspace + ":" + combined_name + "&typeNames=gmd:MD_Metadata" \
                                        "&resultType=results&elementSetName=full&outputSchema" \
                                        "=http://www.isotc211.org/2005/gmd"
                msg['Tiff Extent'] = geotiffUtil.getExtent()
                msg['CSW Service URL'] = cswserver
                msg['CSW Record URL'] = cswrecord
            else:
                cswserver = urlparse.urljoin(self.geoServer, 'csw')
                cswrecord = cswserver + "?service=CSW&version=2.0.2&request=GetRecordById&elementsetname=summary" \
                                        "&id=" + self.gs_workspace + ":" + combined_name + "&typeNames=gmd:MD_Metadata" \
                                                                                      "&resultType=results&elementSetName=full&outputSchema" \
                                                                                      "=http://www.isotc211.org/2005/gmd"
                msg['Tiff Extent'] = geotiffUtil.getExtent()
                msg['CSW Service URL'] = cswserver
                msg['CSW Record URL'] = cswrecord
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
    extractor = PycswExtractor()
    extractor.start()
