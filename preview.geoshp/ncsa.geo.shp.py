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

        # call actual program
        result = extractZipShp(inputfile, fileid)

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
                      'WMS Layer Name': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Layer Name',
    	          'WMS Service URL':'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Service URL',
    	          'WMS Layer URL':  'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Layer URL'
                }
    	      ],
    	      'attachedTo': {'resourceType': 'file', 'id': parameters["fileid"]},
                'agent': {
                  '@type': 'cat:extractor',
    	          'extractor_id': 'https://clowder.ncsa.illinois.edu/clowder/api/extractors/' + extractorName},
    	      'content': {
    	        'WMS Layer Name':  result['WMS Layer Name'],
    	        'WMS Service URL': result['WMS Service URL'],
    	        'WMS Layer URL':   result['WMS Layer URL']	 
              }
            }
            
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


def extractZipShp(inputfile, fileid):
    global geoServer, gs_username, gs_password, gs_workspace

    storeName = fileid
    msg = {}
    msg['errorMsg'] = []
    msg['WMS Layer Name'] = ''
    msg['WMS Service URL'] = ''
    msg['WMS Layer URL'] = ''
    msg['isZipShp'] = False    

    uploadfile = inputfile

    zipshp = zs.Utils(uploadfile)
    if not zipshp.hasError():
        msg['isZipShp'] = True    
        result = subprocess.check_output(['file', '-b', '--mime-type', inputfile], stderr=subprocess.STDOUT)
        logger.info('result.strip is [%s]', result.strip())
	if result.strip() != 'application/zip':    
	    msg['errorMsg'].append('result.strip is: ' + str(result.strip()))
	    return msg

	uploadfile = zipshp.createZip(zipshp.tempDir)
        gsclient = gs.Client(geoServer, gs_username, gs_password)

        if zipshp.getEpsg() == 'UNKNOWN' or zipshp.getEpsg() == None:
            epsg = "EPSG:4326"
        else:
            epsg = "EPSG:"+zipshp.getEpsg()
        success = gsclient.uploadShapefile(gs_workspace, storeName, uploadfile, epsg)

        if success:
            logger.debug("---->success")
            metadata = gsclient.mintMetadata(gs_workspace, storeName, zipshp.getExtent())
            # TODO: create thumbnail and upload it to Medici
            #thumbPath = gsclient.createThumbnail(gs_workspace, storeName, zipshp.getExtent(), "200", "180")
            
            if len(metadata) == 0:
                msg['errorMsg'].append("Coulnd't generate metadata")
            else:
                msg['WMS Layer Name'] = metadata['WMS Layer Name']
                msg['WMS Service URL'] = metadata['WMS Service URL']
                msg['WMS Layer URL'] = metadata['WMS Layer URL']
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
