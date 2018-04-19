#!/usr/bin/env python

import requests
import logging
import json
import subprocess
import os
import tempfile
import urlparse

from pyclowder import extractors
from osgeo import gdal

from config import *
import zipshputils as zs
import geotiffutils as gu
import pycswutils as pu

# to post layer to pycsw
import os, sys, inspect


def main():
    global extractorName, messageType, rabbitmqExchange, rabbitmqURL, logger

    # set logging
    logging.basicConfig(format='%(asctime)-15s %(levelname)-7s : %(name)s - %(message)s',
                        level=logging.WARN)
    logging.getLogger("zipshputils").setLevel(logging.INFO)
    logging.getLogger('pyclowder.extractors').setLevel(logging.DEBUG)
    logger = logging.getLogger(extractorName)
    logger.setLevel(logging.DEBUG)

    # connect to rabbitmq
    extractors.connect_message_bus(extractorName=extractorName,
                                   messageType=messageType,
                                   processFileFunction=process_file,
                                   rabbitmqExchange=rabbitmqExchange,
                                   rabbitmqURL=rabbitmqURL)


# ----------------------------------------------------------------------
# Process the file and upload the results
def process_file(parameters):
    """Process the compressed shapefile and create geoserver layer"""
    global geoServer, gs_username, gs_password, gs_workspace, pycsw_server

    tmpfile = None
    try:
        fileid = parameters['fileid']
        inputfile = parameters['inputfile']
        filename = parameters['filename']
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
            result = extractZipShp(inputfile, fileid, filename)

            # store results as metadata
            if not result['isZipShp'] or len(result['errorMsg']) > 0:
                channel = parameters['channel']
                header = parameters['header']
                for i in range(len(result['errorMsg'])):
                    extractors.status_update(result['errorMsg'][i], fileid, channel, header)
                    logger.info('[%s] : %s', fileid, result['errorMsg'][i], extra={'fileid', fileid})
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
                    'attachedTo': {'resourceType': 'file', 'id': parameters["fileid"]},
                    'agent': {
                        '@type': 'cat:extractor',
                        'extractor_id': 'https://clowder.ncsa.illinois.edu/clowder/api/extractors/' + extractorName},
                    'content': {
                        'CSW Service URL': result['CSW Service URL'],
                        'CSW Record URL': result['CSW Record URL']
                    }
                }

                # post shapefile layer to pycsw
                wmsserver = urlparse.urljoin(geoServer, 'wms')
                layer_name = gs_workspace + ":" + combined_name
                layer_url = wmsserver + '?request=GetMap&layers=' + layer_name + '&bbox=' + result['Shp Extent']\
                            + '&width=640&height=480&srs=EPSG:3857&format=image%2Fpng'
                result = post_layer_to_pycsw(layer_name, layer_url, True)

                # upload metadata
                extractors.upload_file_metadata_jsonld(mdata=metadata, parameters=parameters)
                logger.debug("upload file metadata")
        elif is_geotiff:   # geotiff
            # call actual program
            result = extractGeotiff(inputfile, fileid, filename)

            # store results as metadata
            if not result['isGeotiff'] or len(result['errorMsg']) > 0:
                channel = parameters['channel']
                header = parameters['header']
                for i in range(len(result['errorMsg'])):
                    extractors.status_update(result['errorMsg'][i], fileid, channel, header)
                    logger.info('[%s] : %s', fileid, result['errorMsg'][i], extra={'fileid': fileid})
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
                    'attachedTo': {'resourceType': 'file', 'id': parameters["fileid"]},
                    'agent': {
                        '@type': 'cat:extractor',
                        'extractor_id': 'https://clowder.ncsa.illinois.edu/clowder/api/extractors/' + extractorName},
                    'content': {
                        'CSW Service URL': result['CSW Service URL'],
                        'CSW Record URL': result['CSW Record URL']
                    }
                }

                # post shapefile layer to pycsw
                wmsserver = urlparse.urljoin(geoServer, 'wms')
                layer_name = gs_workspace + ":" + combined_name
                layer_url = wmsserver + '?request=GetMap&layers=' + layer_name + '&bbox=' + result['Tiff Extent'] \
                            + '&width=640&height=480&srs=EPSG:3857&format=image%2Fpng'
                result = post_layer_to_pycsw(layer_name, layer_url, False)

                # upload metadata
                extractors.upload_file_metadata_jsonld(mdata=metadata, parameters=parameters)
                logger.debug("upload file metadata")
    except Exception as ex:
        logger.debug(ex.message)
    finally:
        try:
            logger.debug("Finished posting pycsw server entry")
        except OSError:
            pass


"""
post layer information to pycsw server
"""
def post_layer_to_pycsw(layer_name, layer_url, is_feature):
    bbox_list = pu.parse_bbox_from_url(layer_url)
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

    xml_str = pu.construct_insert_xml(xml_identifier, xml_reference, xml_isFeature, xml_subject,
                                      xml_keyword,
                                      xml_title,
                                      xml_lower_corner, xml_upper_corner)
    result = pu.post_insert_xml(xml_str)

    return result


def extractZipShp(inputfile, fileid, filename):
    global geoServer, gs_workspace

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
        cswserver = urlparse.urljoin(geoServer, 'geoserver/csw')
        cswrecord = cswserver + "?service=CSW&version=2.0.2&request=GetRecordById&elementsetname=summary" \
                                "&id=" + gs_workspace + ":" + combined_name + "&typeNames=gmd:MD_Metadata" \
                                "&resultType=results&elementSetName=full&outputSchema" \
                                 "=http://www.isotc211.org/2005/gmd"
        msg['Shp Extent'] = zipshp.getExtent()
        msg['CSW Service URL'] = cswserver
        msg['CSW Record URL'] = cswrecord

        result = subprocess.check_output(['file', '-b', '--mime-type', inputfile], stderr=subprocess.STDOUT)
        logger.info('result.strip is [%s]', result.strip())
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

def extractGeotiff(inputfile, fileid, filename):
    global geoServer, gs_username, gs_password, gs_workspace, raster_style, logger

    storeName = fileid
    msg = {}
    msg = {}
    msg['errorMsg'] = []
    msg['CSW Service URL'] = ''
    msg['CSW Record URL'] = ''
    msg['Tiff Extent'] = ''
    combined_name = filename + "_" + storeName

    uploadfile = inputfile

    geotiffUtil = gu.Utils(uploadfile, raster_style)

    if not geotiffUtil.hasError():
        msg['isGeotiff'] = True
        cswserver = urlparse.urljoin(geoServer, 'geoserver/csw')
        cswrecord = cswserver + "?service=CSW&version=2.0.2&request=GetRecordById&elementsetname=summary" \
                                "&id=" + gs_workspace + ":" + combined_name + "&typeNames=gmd:MD_Metadata" \
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


if __name__ == '__main__':
    main()
