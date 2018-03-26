#!/usr/bin/env python

import requests
import logging
import json
import subprocess
import os
import tempfile

from pyclowder import extractors

from config import *
import gsclient as gs
import zipshputils as zs

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
    tmpfile = None
    try:
        fileid = parameters['fileid']
        inputfile = parameters['inputfile']
        filename = parameters['filename']

        # TODO: add the method to check if the geoserver has csw
        has_csw = True

        # call actual program
        result = extractZipShp(inputfile, fileid, filename, has_csw)

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

            if has_csw:
                metadata = {
                    "@context": [
                        context_url,
                        {
                            'WMS Layer Name': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Layer Name',
                            'WMS Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Service URL',
                            'WMS Layer URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Layer URL',
                            'CSW Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#CSW Service URL',
                            'CSW Record URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.preview#CSW Record URL'
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
                            'WMS Layer Name': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Layer Name',
                            'WMS Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Service URL',
                            'WMS Layer URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Layer URL'
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

            # post shapefile layer to pycsw
            result = post_layer_to_pycsw(result['WMS Layer Name'], result['WMS Layer URL'])

            # register geoshp preview
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

    is_feature = True  # true: shapefile, false: geotiff
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

def extractZipShp(inputfile, fileid, filename, has_csw):
    global geoServer, gs_username, gs_password, gs_workspace

    storename = fileid
    msg = {}
    msg['errorMsg'] = []
    msg['WMS Layer Name'] = ''
    msg['WMS Service URL'] = ''
    msg['WMS Layer URL'] = ''
    msg['isZipShp'] = False

    uploadfile = inputfile
    combined_name = filename + "_" + storename

    zipshp = zs.Utils(uploadfile)
    if not zipshp.hasError():
        msg['isZipShp'] = True
        # result = subprocess.check_output(['file', '-b', '--mime-type', inputfile], stderr=subprocess.STDOUT)
        result = 'application/zip'
        logger.info('result.strip is [%s]', result.strip())
        if result.strip() != 'application/zip':
            msg['errorMsg'].append('result.strip is: ' + str(result.strip()))
            return msg

        uploadfile = zipshp.createZip(zipshp.tempDir, combined_name)
        gsclient = gs.Client(geoServer, gs_username, gs_password)

        if zipshp.getEpsg() == 'UNKNOWN' or zipshp.getEpsg() == None:
            epsg = "EPSG:4326"
        else:
            epsg = "EPSG:" + zipshp.getEpsg()

        success = gsclient.uploadShapefile(gs_workspace, combined_name, uploadfile, epsg)

        if success:
            logger.debug("---->success")
            metadata = gsclient.mintMetadata(gs_workspace, combined_name, zipshp.getExtent(), has_csw)
            # TODO: create thumbnail and upload it to Medici
            # thumbPath = gsclient.createThumbnail(gs_workspace, storeName, zipshp.getExtent(), "200", "180")

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

if __name__ == '__main__':
    main()
