from geoserver.catalog import Catalog
import requests
import os.path
import tempfile
import urlparse
import logging

class Client:
    
    def __init__ (self, geoserver, username, password):
        self.restserver = urlparse.urljoin(geoserver, 'rest')
        self.wmsserver = urlparse.urljoin(geoserver, 'wms')
        self.cswserver = urlparse.urljoin(geoserver, 'csw')
        self.username = username
        self.password = password
        self.catalog = Catalog(self.restserver, self.username, self.password) 
        self.tempDir = tempfile.mkdtemp()
        self.resource = None
        self.layer = None
        self.layerName = None
        logging.basicConfig(format="%(asctime)-15s %(name)-10s %(levelname)-7s : %(message)s", level=logging.WARN)
        self.logger = logging.getLogger("gsclient")

    ## this method assume that there is 1 store per layer
    def getResourceByStoreName(self, storename, workspace):
        if self.resource != None:
            self.logger.debug("resource instance found; no need to fetch")
            return self.resource
        self.logger.debug("catalog.get_store called")
        store = self.catalog.get_store(storename, workspace)
        self.logger.debug("catalog.get_resources called based on store")
        resources = self.catalog.get_resources(store=store)
        self.logger.debug("fetched resources from server")
        if resources == None: 
            return None
        else:
            self.resource = resources[0]
            return self.resource

    def getLayers(self):
        layers = self.catalog.get_layers()
        return layers

    def getLayerByStoreName(self, storename):
        self.logger.debug("getLayerbystore name started")
        layers = self.catalog.get_layers()
    
        for layer in layers:
            if layer.resource.store.name == storename:
                self.logger.debug("found the layer by store name")
                return layer
        return None

    def getLayerByResource(self, resource):
        if self.layer != None:
            self.logger.debug("layer instance found; no need to fetch")
            return self.layer
            
        self.logger.debug("get Layer by Resource started...")
        layers = self.catalog.get_layers(resource)
        self.logger.debug("fetched layers from the server")
        if layers == None: 
            return None
        else:
            self.layer = layers[0]
            return self.layer

    def mintMetadata(self, workspace, storename, extent, has_csw=False):
        self.logger.debug("Creating wms metadata ... ") 
        metadata = {}
        layername = None
        if self.layerName == None:
            if self.layer == None:
                self.logger.debug("getResourceByStoreName..")
                resource = self.getResourceByStoreName(storename, workspace)
                self.logger.debug("getLayerByResource ...")
                layer = self.getLayerByResource(resource)
                                #layername = layer.name 
                self.logger.debug("done getting layer name")
                if layer == None: 
                    self.logger.debug('No layer found [DONE]')
                    return metadata
                else:
                    layername = layer.name
            else:
                layername = self.layer.name
                self.layerName = self.layer.name
        else:
            layername = self.layerName
        # generate metadata 
        wmsLayerName = workspace + ':' + layername
        metadata['WMS Layer Name'] = wmsLayerName
        metadata['WMS Service URL'] = self.wmsserver
        metadata['WMS Layer URL'] = self.wmsserver+'?request=GetMap&layers='+wmsLayerName+'&bbox='+extent+'&width=640&height=480&srs=EPSG:3857&format=image%2Fpng'
        if has_csw:
            metadata['CSW Service URL'] = self.cswserver
            metadata['CSW Record URL'] = self.cswserver + "?service=CSW&version=2.0.2&request=GetRecordById&elementsetname=summary&id=" + workspace + ":" + layername + "&typeNames=gmd:MD_Metadata&resultType=results&elementSetName=full&outputSchema=http://www.isotc211.org/2005/gmd"

        self.logger.debug('[DONE]')
        return metadata

    def uploadShapefile(self, workspace, storename, filename, projection):
        self.logger.debug("Uploading shapefile" + filename +"...")
        url = self.restserver+"/workspaces/"+workspace+"/datastores/" + storename + "/file.shp"
        response = None
        with open(filename, 'rb') as f:
            response = requests.put(url, headers={'content-type':'application/zip'}, auth=(self.username, self.password),data=f)
        self.logger.debug(str(response.status_code) + " " + response.text)

        if response.status_code != 201:
            self.logger.debug("[DONE]")
            return False

        # setup projection
        
        resource = self.getResourceByStoreName(storename, workspace)

        if resource.projection == None:
            self.logger.debug('Setting projection' + projection)
            resource.projection = projection
            self.catalog.save(resource)
        self.logger.debug("[DONE]")
        name, ext = os.path.splitext(os.path.basename(filename))
        self.layerName = storename
        return True

    def uploadGeotiff(self, workspace, storename, filename, title, styleStr, projection):
        self.logger.debug("Uploading geotiff" + filename + "...")
        name, ext = os.path.splitext(os.path.basename(filename))
        # TODO need to check the coverage name to avoid duplication
        url = self.restserver+"/workspaces/"+workspace+"/coveragestores/" + storename + "/file.geotiff" + "?coverageName=" + storename
        response = None
        self.logger.debug(url)
        with open(filename, 'rb') as f:
            response = requests.put(url, headers={'content-type':'image/tiff'}, auth=(self.username, self.password),data=f)
        self.logger.debug(str(response.status_code) + " " + response.text)

        if response.status_code != 201:
            self.logger.error(response.text)
            self.logger.debug("[DONE]")
            return False
        self.layerName = storename

        resource = self.getResourceByStoreName(storename, workspace)

        # setting projection
        if resource.projection == None:
            self.logger.debug('Setting projection' + projection)
            resource.projection = projection
            self.catalog.save(resource)

        if styleStr is not None:
            if self.uploadRasterStyle(storename, styleStr):
                self.logger.debug('Setting style')
                self.setStyle(self.layerName, storename)
        
            self.logger.debug("style set: [DONE]")
        return True

    def uploadRasterStyle(self, storename, styleStr):
        if styleStr == 'None': 
            return False
        sldFileName = os.path.join(self.tempDir, storename + ".sld")
        sldFile = open(sldFileName, 'w')
        sldFile.write(styleStr)
        sldFile.close()

        url = self.restserver+"/styles"
        self.logger.debug(url)
        response = requests.post(url, headers={'content-type':'text/xml'}, auth=(self.username, self.password), data="<style><name>" + storename + "</name><filename>" + storename + ".sld</filename></style>")
        if response.status_code != 201:
            self.logger.debug('error' + response.text)
            return False

        with open(sldFileName, 'rb') as f:
            response = requests.put(url +"/" + storename, headers={'content-type': 'application/vnd.ogc.sld+xml'}, auth=(self.username, self.password), data=f)
        self.logger.debug(response.status_code)
        self.logger.debug(response.text)
        self.logger.debug("uploaded the raster style")
        if response.status_code == 200:
            return True
        else: 
            return False

    def setStyle(self, layername, stylename):
        layer = None
        if self.layer != None:
            layer = self.layer
        else:
            self.logger.debug("getting a layer by name")
            layer = self.catalog.get_layer(layername)
        layer.default_style = stylename
        self.catalog.save(layer)

    def createThumbnail(self, workspace, storename, extent, width, height):
        self.logger.debug('Creating Thumbnail ...')
        layername = None
        if self.layerName == None:
            if self.layer == None:
                self.logger.debug("getResourceByStoreName..")
                resource = self.getResourceByStoreName(storename, workspace)
                self.logger.debug("getLayerByResource ...")
                layer = self.getLayerByResource(resource)
                self.logger.debug("done getting layer name")
                if layer == None: 
                    self.logger.debug('No layer found [DONE]')
                    return metadata
                else:
                    layername = layer.name
            else:
                self.logger.debug("layer instance found: no need to fetch")
                layername = self.layer.name
                self.layerName = self.layer.name
        else:
            self.logger.debug("layerName instance found: no need to fetch")
            layername = self.layerName
            #wmsLayerName = workspace+":"+layer.name
            wmsLayerName = workspace+":"+layername
        url = self.wmsserver+"?request=GetMap&layers="+wmsLayerName+"&bbox="+extent+"&width="+width+"&height="+height+"&srs=EPSG:3857&format=image%2Fpng"

        r = requests.get(url, stream=True)
        path=os.path.join(self.tempDir,'tmp.png')

        if r.status_code == 200:
            tmp = r.headers['content-disposition']
            tmplist = tmp.split(';')
            for t in tmplist:
                if t.strip().find('filename=') != -1:
                    path = os.path.join(self.tempDir, t.strip().split('=')[1])
            with open(path, 'wb') as f:
                for chunk in r.iter_content():
                    f.write(chunk)
            self.logger.debug('[DONE]')
            return path
        else:
            self.logger.debug('can not create thumbnail [DONE]')
            return ''


    def __del__(self):
        # delete the temp dir
        if self.tempDir != None:
            try:
                import shutil
                self.logger.debug( "Deleting temp dir "+ self.tempDir)
                shutil.rmtree(self.tempDir)
                self.logger.debug("Deleted Temp file")
            except OSError as exc:
                if exc.errno != errno.ENOENT:
                    raise


if __name__ == "__main__":
    
    # global loggergs
    geoserver = ""
    username = ""
    password = ""
    myclient = Client(geoserver, username, password)
    #logging.basicConfig(format="%(asctime)-15s %(name)-10s %(levelname)-7s : %(message)s",
    #                level=logging.WARN)
    #loggergs = logging.getLogger("gsClient")
    #loggergs.setLevel(logging.DEBUG) 
    #myclient.setStyle('geotiff', 'testing')
    # myclient.createRasterStyle('testing', f.read())
    #myclient.uploadShapefile("medici", "test-shp", "/home/jonglee/share/browndog/qina-data2/huc12.zip", "EPSG:26916")
    #myclient.uploadGeotiff("medici", "test", "/home/jonglee/share/browndog/39-44.tif", 'None', "EPSG:26916")
    #getLayers(geoserver, username, password)
    #l = myclient.getLayerByStore("gltg-pools")
    #if l != None:
    #    print l.name
    #else: 
    #    print "couldn't find"
