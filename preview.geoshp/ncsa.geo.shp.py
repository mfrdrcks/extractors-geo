#!/usr/bin/env python
import copy
import requests
import logging
import os
import tempfile
import subprocess

from urlparse import urlparse
from urlparse import urljoin

from pyclowder.extractors import Extractor
from pyclowder.utils import StatusMessage
from pyclowder.utils import CheckMessage
import pyclowder.files

import gsclient as gs
import zipshputils as zs
from geoserver.catalog import Catalog

class ExtractorsGeoshpPreview(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # parse command line and load default logging configuration
        self.setup()

        self.extractorName = os.getenv('RABBITMQ_QUEUE', "ncsa.geoshp.preview")
        self.geoServer = os.getenv('GEOSERVER_URL')
        self.gs_username = os.getenv('GEOSERVER_USERNAME', 'admin')
        self.gs_password = os.getenv('GEOSERVER_PASSWORD', 'geosever')
        self.proxy_url = os.getenv('PROXY_URL', 'http://localhost:9000/api/proxy/')
        self.proxy_on = os.getenv('PROXY_ON', 'false')

        self.datasetid = None
        self.logger = logging.getLogger('geoshp preview')
        self.logger.setLevel(logging.DEBUG)
        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def check_message(self, connector, host, secret_key, resource, parameters):
        logger = logging.getLogger('check_message')
        logger.setLevel(logging.DEBUG)
        if 'activity' in parameters:
            fileid = parameters.get('id')
            action = parameters.get('activity')
            logger.debug("activity %s for fileid %s " % (action, str(fileid)))
            if 'removed' == action:
                fileid = parameters['id']
                if 'source' in parameters:
                    mimetype = parameters.get('source').get('mimeType')
                    logger.debug("mimetype: %s for fileid %s " % (mimetype, str(fileid)))
                filename = parameters.get('source').get('extra').get('filename')
                if filename is None:
                    logger.warn('can not get filename for fileid %s' % str(fileid))

                storename = filename + '_' + str(fileid)
                layername = self.gs_workspace + ':' + storename

                logger.debug('remove layername %s' % layername)
                logger.debug("CheckMessage.ignore: activity %s for fileid %s " % (action, str(fileid)))
                self.remove_geoserver_layer(storename, layername)
                logger.debug("activity %s for fileid %s is done" % (action, str(fileid)))
                return CheckMessage.ignore
        return CheckMessage.download

    # ----------------------------------------------------------------------
    # Process the file and upload the results
    def process_message(self, connector, host, secret_key, resource, parameters):

        """Process the compressed shapefile and create geoserver layer"""
        tmpfile = None
        try:
            filename = resource['name']
            inputfile = resource["local_paths"][0]
            fileid = resource['id']

            # get variable for geoserver workspace. This is a datasets' id
            try:
                parentid = resource['parent']['id']
            except:
                parentid = "no_datasets"
            self.gs_workspace = parentid

            # call actual program
            result = self.extractZipShp(inputfile, fileid, filename, secret_key)

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
                            'WMS Layer Name': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Layer Name',
                            'WMS Service URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Service URL',
                            'WMS Layer URL': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geoshp.preview#WMS Layer URL'
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

                # register geoshp preview
                (_, ext) = os.path.splitext(inputfile)
                (_, tmpfile) = tempfile.mkstemp(suffix=ext)
                pyclowder.files.upload_metadata(connector, host, secret_key, fileid, metadata)
                self.logger.debug("upload previewer")

        except Exception as ex:
            self.logger.debug(ex.message)
        finally:
            try:
                os.remove(tmpfile)
                self.logger.debug("delete tmpfile: " + tmpfile)
            except OSError:
                pass

    def remove_geoserver_layer(self, storename, layername):
        last_charactor = self.geoServer[-1]
        if last_charactor == '/':
            geoserver_rest = self.geoServer + 'rest'
        else:
            geoserver_rest = self.geoServer + '/rest'
        cat = Catalog(geoserver_rest, username=self.gs_username, password=self.gs_password)
        # worksp = cat.get_workspace(gs_workspace)
        store = cat.get_store(storename)
        layer = cat.get_layer(layername)
        cat.delete(layer)
        cat.reload()
        cat.delete(store)
        cat.reload

    def extractZipShp(self, inputfile, fileid, filename, secret_key):
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
            result = subprocess.check_output(['file', '-b', '--mime-type', inputfile], stderr=subprocess.STDOUT)
            self.logger.info('result.strip is [%s]', result.strip())

            if result.strip() != 'application/zip':
                msg['errorMsg'].append('result.strip is: ' + str(result.strip()))
                return msg

            uploadfile = zipshp.createZip(zipshp.tempDir, combined_name)

            # TODO if the proxy is working, gsclient host should be changed to proxy server
            gsclient = gs.Client(self.geoServer, self.gs_username, self.gs_password)

            if self.proxy_on.lower() == 'true':
                parsed_uri = urlparse(self.geoServer)
                gs_domain = u'{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                geoserver_rest = self.geoServer.replace(gs_domain, self.proxy_url)
            else:
                geoserver_rest = self.geoServer

            if zipshp.getEpsg() == 'UNKNOWN' or zipshp.getEpsg() == None:
                epsg = "EPSG:4326"
            else:
                epsg = "EPSG:" + zipshp.getEpsg()

            success = gsclient.uploadShapefile(geoserver_rest, self.gs_workspace, combined_name, uploadfile, epsg, secret_key, self.proxy_on)

            if success:
                self.logger.debug("---->success")
                metadata = gsclient.mintMetadata(self.gs_workspace, combined_name, zipshp.getExtent())
                # TODO: create thumbnail and upload it to Medici
                # thumbPath = gsclient.createThumbnail(gs_workspace, storeName, zipshp.getExtent(), "200", "180")

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
                # else:
                #     msg['WMS Layer Name'] = metadata['WMS Layer Name']
                #     msg['WMS Service URL'] = metadata['WMS Service URL']
                #     msg['WMS Layer URL'] = metadata['WMS Layer URL']

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

if __name__ == "__main__":
    extractor = ExtractorsGeoshpPreview()
    extractor.start()
