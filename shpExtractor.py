#!/usr/bin/env python
import gsclient as gs
import zipshputils as zs
import pika
import requests
import sys
import logging
import time
import json
import subprocess
import tempfile
import os


## Assumption: this extractor runs on localhost with rabbitMQ with default setting


# ----------------------------------------------------------------------
# BEGIN CONFIGURATION
# ----------------------------------------------------------------------

# name where rabbitmq is running
rabbitmqhost = "localhost"

# name to show in rabbitmq queue list
exchange = "medici"

# name to show in rabbitmq queue list
extractorName = "shpExtractor"

# username and password to connect to rabbitmq
username = None
password = None

# accept any type of file that is text
routingKeys = ["*.file.multi.files-zipped.#", "*.file.application.zip", "*.file.application.x-zip", "*.file.application.x-7z-compressed"]
#routingKeys = ["*.file.multi.files-zipped.#"]
#routingKeys = ["*.file.application.zip"]

# secret key used to connect to medici, this will eventually be
# part of the message received.
#secretKey = "r1ek3rs"

# trust certificates, set this to false for self signed certificates
sslVerify=False

# Geoserver setting
geoServer = "http://localhost:8080/geoserver/"
gs_username = ""
gs_password = ""
gs_workspace = ""

# ----------------------------------------------------------------------
# END CONFIGURATION
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
# setup connection to server and wait for messages
def connect_message_bus():
    """Connect to message bus and wait for messages"""
    global extractorName, username, password, messageType, exchange

    # connect to rabbitmq using input username and password
    if (username is None or password is None):
        connection = pika.BlockingConnection()
    else:
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(host=rabbitmqhost, credentials=credentials)
        connection = pika.BlockingConnection(parameters)
    
    # connect to channel
    channel = connection.channel()
    
    # declare the exchange in case it does not exist
    channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)
    
    # declare the queue in case it does not exist
    channel.queue_declare(queue=extractorName, durable=True)

    # connect queue and exchange
    for k in routingKeys:
        channel.queue_bind(queue=extractorName, exchange=exchange, routing_key=k)

    # create listener
    channel.basic_consume(on_message, queue=extractorName, no_ack=False)

    # start listening
    logger.info("Waiting for messages. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()

    # close connection
    connection.close()
 
# ----------------------------------------------------------------------
# Process any incoming message
def on_message(channel, method, header, body):
    """When message is received do the following steps:
    1. download the file
    2. launch extractor function"""

    global logger, extractorName

    inputfile=None
    fileid=0

    try:
        # parse body back from json
        jbody=json.loads(body)
        host=jbody['host']
        fileid=jbody['id']
        secretKey=jbody['secretKey']
        intermediatefileid=jbody['intermediateId']
        if not (host.endswith('/')):
            host += '/'
        
         # tell everybody we are starting to process the file
        status_update(channel, header, fileid, "Started processing file")

        # download file
        inputfile = download_file(channel, header, host, secretKey, fileid, intermediatefileid)

        # call actual extractor function
        process_file(channel, header, host, secretKey, fileid, intermediatefileid, inputfile)
 
        # notify rabbitMQ we are done processsing message
        channel.basic_ack(method.delivery_tag)

    except subprocess.CalledProcessError as e:
        msg = str.format("Error processing [exit code=%d]\n%s", e.returncode, e.output)
        logger.exception("[%s] %s", fileid, msg)
        status_update(channel, header, fileid, msg)
    except:
        logger.exception("[%s] error processing", fileid)
        status_update(channel, header, fileid, "Error processing")
    finally:
        status_update(channel, header, fileid, "Done")
        if inputfile is not None:
            try:
                os.remove(inputfile)
            except OSError:
                pass
            except UnboundLocalError:
                pass

# ----------------------------------------------------------------------
# Send updates about status of processing file
def status_update(channel, header, fileid, status):
    """Send notification on message bus with update"""

    global extractorName, exchange

    logger.debug("[%s] : %s", fileid, status)

    statusreport = {}
    statusreport['file_id'] = fileid
    statusreport['extractor_id'] = extractorName
    statusreport['status'] = status
    statusreport['start'] = time.strftime('%Y-%m-%dT%H:%M:%S')
    channel.basic_publish(exchange=exchange,
                          routing_key=header.reply_to,
                          properties=pika.BasicProperties(correlation_id = header.correlation_id),
                          body=json.dumps(statusreport))

# ----------------------------------------------------------------------
# Download file from medici
def download_file(channel, header, host, key, fileid, intermediatefileid):
    """Download file to be processed from Medici"""

    global sslVerify

    status_update(channel, header, fileid, "Downloading file.")

    # fetch data
    url=host + 'api/files/' + intermediatefileid + '?key=' + key
    r=requests.get('%sapi/files/%s?key=%s' % (host, intermediatefileid, key),
                   stream=True, verify=sslVerify)
    r.raise_for_status()
    (fd, inputfile)=tempfile.mkstemp()
    with os.fdopen(fd, "w") as f:
        for chunk in r.iter_content(chunk_size=10*1024):
            f.write(chunk)
    return inputfile


# ----------------------------------------------------------------------
# Process the file and upload the results
def process_file(channel, header, host, key, fileid, intermediatefileid, inputfile):
    """Process the compressed shapefile and create geoserver layer"""

    global sslVerify

    status_update(channel, header, fileid, "Counting words in file.")

    # call actual program
    result = extractZipShp(inputfile, fileid)
    #result = subprocess.check_output(['wc', inputfile], stderr=subprocess.STDOUT)
    if not result['isZipShp']:
        # zip file is not shapefile
        return

    # store results as metadata
    metadata={}
    if len(result['errorMsg']) > 0:
        for i in range(len(result['errorMsg'])):
            fieldName = 'error'+str(i)
            metadata[fieldName] = result['errorMsg'][i]
    else:
        metadata['WMS Layer Name'] = result['WMS Layer Name']
        metadata['WMS Service URL'] = result['WMS Service URL']
        metadata['WMS Layer URL'] = result['WMS Layer URL']

    headers={'Content-Type': 'application/json'}
    r = requests.post('%sapi/files/%s/metadata?key=%s' % (host, fileid, key),
                      headers=headers,
                      data=json.dumps(metadata),
                      verify=sslVerify);
    r.raise_for_status()


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
		if result.strip() != 'application/zip':	
			uploadfile = zipshp.createZip(zipshp.tempDir)
		gsclient = gs.Client(geoServer, gs_username, gs_password)

		if zipshp.getEpsg() == 'UNKNOWN' or zipshp.getEpsg() == None:
			epsg = "EPSG:4326"
		else:
			epsg = "EPSG:"+zipshp.getEpsg()
		
		success = gsclient.uploadShapefile(gs_workspace, storeName, uploadfile, epsg)

		if success: 
			#print "---->success"
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
			#print "---->fail"
			msg['errorMsg'].append("Fail to upload the file to geoserver") 
	else:
		error = zipshp.zipShpProp	
		if error['shpFile'] == None:
			msg['isZipShp'] = False	
			#print "---->error: normal compressed file"
			return msg

		if error['hasDir']:
			msg['errorMsg'].append("a compressed shapefile can not have directory")
			#print "---->error: a compressed shapefile can not have directory"
			return msg
			
		if error['numShp'] > 1:
			msg['errorMsg'].append("a compressed shapefile can not have multiple shpefiles")
			#print "---->error: a compressed shapefile can not have multiple shpefiles"
			return msg

		if error['hasSameName'] == False:
			msg['errorMsg'].append("a shapefile files (.shp, .shx, .dbf, .prj) should have same name")
			#print "---->error: a shapefile files (.shp, .shx, .dbf, .prj) should have same name"
			return msg

		if error['shxFile'] == None:
			msg['errorMsg'].append(".shx file is missing")
			#print "---->error: .shx file is missing"

		if error['dbfFile'] == None:
			msg['errorMsg'].append(".dbf file is missing")
			#print "---->error: .dbf file is missing"

		if error['prjFile'] == None:
			msg['errorMsg'].append(".prj file is missing")
			#print "---->error: .prj file is missing"

		if error['epsg'] == 'UNKNOWN':
			msg['errorMsg'].append("The projection ccould not be recognized")
			#print "---->error: The projection can not be recognized"

		if error['extent'] == 'UNKNOWN':
			msg['errorMsg'].append("The extent could not be calculated")
			#print "---->error: The extent could not be calculated"

	return msg
	

if __name__ == '__main__':
    # configure the logging system
    logging.basicConfig(format="%(asctime)-15s %(name)-10s %(levelname)-7s : %(message)s",
                        level=logging.WARN)

    if len(sys.argv) < 5:
        logger.info("geoserver url, admin username, admin password, workspace")
        sys.exit()

    geoServer = sys.argv[1]
    gs_username = sys.argv[2]
    gs_password = sys.argv[3]
    gs_workspace = sys.argv[4]

    logger = logging.getLogger(extractorName)
    logger.setLevel(logging.DEBUG)

    # connect and process data    
    sys.exit(connect_message_bus())
