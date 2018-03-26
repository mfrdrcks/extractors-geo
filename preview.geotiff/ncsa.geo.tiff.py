#!/usr/bin/env python

import json
import logging
import requests
import time
import pika
import os
import tempfile

from pyclowder import extractors
from osgeo import gdal

from config import *
import geotiffutils as gu
import gsclient as gs

# to post layer to pycsw
import os, sys, inspect
# following lines should be changed to something like this after pycsw.utils package got merged
# add the line in requirements.txt file
# https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/extractors-geo/browse#egg=extractors-geo
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, os.path.join(parentdir, "pycsw.utils"))
import pycsw_utils as pycswutils


def main():
    global extractorName, messageType, rabbitmqExchange, rabbitmqURL, logger

    # set logging
    logging.basicConfig(format='%(asctime)-15s %(levelname)-7s : %(name)s - %(message)s',
                        level=logging.WARN)
    logging.getLogger('pyclowder.extractors').setLevel(logging.DEBUG)
    logger = logging.getLogger(extractorName)
    logger.setLevel(logging.DEBUG)

    # connect to rabbitmq
    extractors.connect_message_bus(extractorName=extractorName,
                                   messageType=messageType,
                                   processFileFunction=process_file,
                                   rabbitmqExchange=rabbitmqExchange,
                                   rabbitmqURL=rabbitmqURL)


# Process the file and upload the results
def process_file(parameters):
    """Process the geotiff and create geoserver layer"""

    global logger

    fileid = parameters['fileid']
    inputfile = parameters['inputfile']
    filename = parameters['filename']
    tmpfile = None

    # TODO: add the method to check if the geoserver has csw
    has_csw = True

    try:
        # call actual program
        result = extractGeotiff(inputfile, fileid, filename, has_csw)

        if not result['WMS Layer URL'] or not result['WMS Service URL'] or not result['WMS Layer URL']:
            logger.info('[%s], inputfile: %s has empty result', fileid, inputfile)

        # store results as metadata
        elif not result['isGeotiff'] or len(result['errorMsg']) > 0:
            channel = parameters['channel']
            header = parameters['header']
            for i in range(len(result['errorMsg'])):
                extractors.status_update(result['errorMsg'][i], fileid, channel, header)
                logger.info('[%s] : %s', fileid, result['errorMsg'][i], extra={'fileid': fileid})
        else:
            # Context URL
            context_url = "https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"

            if has_csw:
                metadata = {
                    "@context": [
                        context_url,
                        {
                            'WMS Layer Name': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#WMS Layer Name',
                            'WMS Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#WMS Service URL',
                            'WMS Layer URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#WMS Layer URL',
                            'CSW Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#CSW Service URL',
                            'CSW Record URL':'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#CSW Record URL'
                        }
                    ],
                    'attachedTo': {'resourceType': 'file', 'id': parameters["fileid"]},
                    'agent': {
                        '@type': 'cat:extractor',
                        'extractor_id': 'https://clowder.ncsa.illinois.edu/clowder/api/extractors/' + extractorName},
                    'content': {
                        'WMS Layer Name': result['WMS Layer Name'],
                        'WMS Service URL': result['WMS Service URL'],
                        'WMS Layer URL': result['WMS Layer URL'],
                        'CSW Service URL': result['CSW Service URL'],
                        'CSW Record URL': result['CSW Record URL']
                    }
                }
            else:
                metadata = {
                    "@context": [
                        context_url,
                        {
                            'WMS Layer Name': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#WMS Layer Name',
                            'WMS Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#WMS Service URL',
                            'WMS Layer URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#WMS Layer URL'
                        }
                    ],
                    'attachedTo': {'resourceType': 'file', 'id': parameters["fileid"]},
                    'agent': {
                        '@type': 'cat:extractor',
                        'extractor_id': 'https://clowder.ncsa.illinois.edu/clowder/api/extractors/' + extractorName},
                    'content': {
                        'WMS Layer Name': result['WMS Layer Name'],
                        'WMS Service URL': result['WMS Service URL'],
                        'WMS Layer URL': result['WMS Layer URL']
                    }
                }

            # post geotiff layer to pycsw
            result = post_layer_to_pycsw(result['WMS Layer Name'], result['WMS Layer URL'])

            # register geotiff preview
            (_, ext) = os.path.splitext(inputfile)
            (_, tmpfile) = tempfile.mkstemp(suffix=ext)
            extractors.upload_preview(previewfile=tmpfile, parameters=parameters)
            logger.debug("upload previewer")
            extractors.upload_file_metadata_jsonld(mdata=metadata, parameters=parameters)
            logger.debug("upload file metadata")

    except Exception as ex:
        logger.debug(ex.message)
    finally:
        try:
            os.remove(tmpfile)
            logger.debug("delete tmpfile: " + tmpfile)
        except OSError:
            pass

"""
post layer information to pycsw server
"""
def post_layer_to_pycsw(layer_name, layer_url):
    bbox_list = parse_bbox_from_url(layer_url)
    layer_title = layer_name.split(':')[1]

    is_feature = False  # true: shapefile, false: geotiff
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

    xml_str = pycswutils.construct_insert_xml(xml_identifier, xml_reference, xml_isFeature, xml_subject,
                                              xml_keyword,
                                              xml_title,
                                              xml_lower_corner, xml_upper_corner)
    result = pycswutils.post_insert_xml(xml_str)

    return result

"""
parse bounding box information from layer url
"""
def parse_bbox_from_url(url):
    bbox_list = []
    for line in url.split('&'):
        elements = line.split("=")
        if (elements[0]).lower() == 'bbox':
            for bbox in elements[1].split(','):
                bbox_list.append(bbox)

    bbox_list = pycswutils.convert_bounding_box_3857_4326(bbox_list)
    b1 = bbox_list[0]
    b2 = bbox_list[1]
    b3 = bbox_list[2]
    b4 = bbox_list[3]
    bbox_list[0] = b2
    bbox_list[1] = b1
    bbox_list[2] = b4
    bbox_list[3] = b3

    return bbox_list

def extractGeotiff(inputfile, fileid, filename, has_csw):
    global geoServer, gs_username, gs_password, gs_workspace, raster_style, logger

    storeName = fileid
    msg = {}
    msg['errorMsg'] = []
    msg['WMS Layer Name'] = ''
    msg['WMS Service URL'] = ''
    msg['WMS Layer URL'] = ''
    msg['isGeotiff'] = False

    uploadfile = inputfile

    geotiffUtil = gu.Utils(uploadfile, raster_style)

    if not geotiffUtil.hasError():
        msg['isGeotiff'] = True
        gsclient = gs.Client(geoServer, gs_username, gs_password)

        epsg = "EPSG:" + str(geotiffUtil.getEpsg())
        style = None

        # check if the input geotiff has a style,
        # you can do this by checking if there is any color table
        uploadfile_dataset = gdal.Open(uploadfile)
        uploadfile_band = uploadfile_dataset.GetRasterBand(1)
        color_table = uploadfile_band.GetColorTable()
        if color_table is not None:
            logger.debug("Geotiff has the style already")
        else:
            style = geotiffUtil.createStyle()
            logger.debug("style created")

        # merge file name and id and make a new store name
        combined_name = filename + "_" + storeName
        success = gsclient.uploadGeotiff(gs_workspace, combined_name, uploadfile, filename, style, epsg)
        logger.debug("upload geotiff successfully")
        if success:
            metadata = gsclient.mintMetadata(gs_workspace, combined_name, geotiffUtil.getExtent(), has_csw)
            logger.debug("mintMetadata obtained")
            if len(metadata) == 0:
                msg['errorMsg'].append("Coulnd't generate metadata")
            else:
                msg['WMS Layer Name'] = metadata['WMS Layer Name']
                msg['WMS Service URL'] = metadata['WMS Service URL']
                msg['WMS Layer URL'] = metadata['WMS Layer URL']
                if has_csw:
                    msg['CSW Service URL'] = metadata['CSW Service URL']
                    msg['CSW Record URL'] = metadata['CSW Record URL']
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


if __name__ == '__main__':
    main()
