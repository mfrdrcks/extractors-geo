#!/usr/bin/env python

import json
import logging
import requests
import time
import pika

from pyclowder import extractors

from config_tif import *
import geotiffutils as gu
import gsclient as gs


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

    # call actual program
    result = extractGeotiff(inputfile, fileid)

    # store results as metadata
    if not result['isGeotiff'] or len(result['errorMsg']) > 0:
        channel = parameters['channel']
        header = parameters['header']
        for i in range(len(result['errorMsg'])):
            extractors.status_update(result['errorMsg'][i], fileid, channel, header)
            logger.info('[%s] : %s', fileid, result['errorMsg'][i], extra={'fileid': fileid})
    else:
        metadata = {}
        metadata['WMS Layer Name'] = result['WMS Layer Name']
        metadata['WMS Service URL'] = result['WMS Service URL']
        metadata['WMS Layer URL'] = result['WMS Layer URL']
        extractors.upload_file_metadata(metadata, parameters)

def extractGeotiff(inputfile, fileid):
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
        
        style = geotiffUtil.createStyle()
        logger.debug("style created")
        success = gsclient.uploadGeotiff(gs_workspace, storeName, uploadfile, style, epsg)
        logger.debug("upload geotiff successfully")
        if success: 
            metadata = gsclient.mintMetadata(gs_workspace, storeName, geotiffUtil.getExtent())
            logger.debug("mintMetadata obtained")
            if len(metadata) == 0:
                msg['errorMsg'].append("Coulnd't generate metadata")
            else:
                msg['WMS Layer Name'] = metadata['WMS Layer Name']
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
    

if __name__ == '__main__':
    main()
