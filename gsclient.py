from geoserver.catalog import Catalog
import requests
import os.path
import tempfile
import urlparse

class Client:
	
	def __init__ (self, geoserver, username, password):
		self.restserver = urlparse.urljoin(geoserver, 'rest')
		self.wmsserver = urlparse.urljoin(geoserver, 'wms')
		self.username = username
		self.password = password
		self.catalog = Catalog(self.restserver, self.username, self.password) 
		self.tempDir = tempfile.mkdtemp()

	def getLayers(self):
		layers = self.catalog.get_layers()
		return layers

	def getLayerByStore(self, storeName):
		layers = self.catalog.get_layers()
	
		for layer in layers:
			if layer.resource.store.name == storeName:
				return layer
		return None

	def mintMetadata(self, workspace, storeName, extent):
		metadata = {}
		layer = self.getLayerByStore(storeName)
		if layer == None: 
			return metadata

		# generate metadata 
		wmsLayerName = workspace + ':' + layer.name
		metadata['WMS Layer Name'] = wmsLayerName
		metadata['WMS Service URL'] = self.wmsserver
		metadata['WMS Layer URL'] = self.wmsserver+'?request=GetMap&layers='+wmsLayerName+'&bbox='+extent+'&width=640&height=480&srs=EPSG:3857&format=image%2Fpng'

		return metadata

	def uploadShapefile(self, workspace, storeName, filename, projection):
		print "Uploading shapefile",filename,"...",
		url = self.restserver+"/workspaces/"+workspace+"/datastores/"+storeName+"/file.shp"
		response = None
		with open(filename, 'rb') as f:
			response = requests.put(url, headers={'content-type':'application/zip'}, auth=(self.username, self.password),data=f)
		print response.status_code, response.text

		if response.status_code != 201: 
			print "[DONE]"
			return False

		# setup projection
		
		resources = self.catalog.get_resources()

		for resource in resources:
			if resource.store.name == storeName:
				if resource.projection == None:
					print 'Setting projection', projection
					resource.projection = projection
				self.catalog.save(resource)
				break
		print "[DONE]"
		return True

	def uploadGeotiff(self, workspace, storeName, filename, styleStr, projection):
		print "Uploading geotiff",filename,"...",
		name, ext = os.path.splitext(os.path.basename(filename))
		# TODO need to check the coverage name to avoid duplication
		url = self.restserver+"/workspaces/"+workspace+"/coveragestores/"+storeName+"/file.geotiff"+"?coverageName="+name
		response = None
		print url
		with open(filename, 'rb') as f:
			response = requests.put(url, headers={'content-type':'image/tiff'}, auth=(self.username, self.password),data=f)
		print response.status_code, response.text

		if response.status_code != 201: 
			print "[DONE]"
			return False

		# setup projection
		resources = self.catalog.get_resources()

		for resource in resources:
			if resource.store.name == storeName:
				# setting projection
				if resource.projection == None:
					print 'Setting projection', projection
					resource.projection = projection
					self.catalog.save(resource)
				break
		
		if self.uploadRasterStyle(storeName, styleStr):
			print 'Setting style'
			layer = self.getLayerByStore(storeName)
			self.setStyle(layer.name, storeName)
		
		print "[DONE]"
		return True

	def uploadRasterStyle(self, storeName, styleStr):
		if styleStr == 'None': 
			return False
		sldFileName = os.path.join(self.tempDir, storeName+".sld")
		sldFile = open(sldFileName, 'w')
		sldFile.write(styleStr)
		sldFile.close()

		url = self.restserver+"/styles"
		print url
		response = requests.post(url, headers={'content-type':'text/xml'}, auth=(self.username, self.password),data="<style><name>"+storeName+"</name><filename>"+storeName+".sld</filename></style>")
		if response.status_code != 201:
			print 'error', response.text
			return False

		with open(sldFileName, 'rb') as f:
			response = requests.put(url+"/"+storeName, headers={'content-type':'application/vnd.ogc.sld+xml'}, auth=(self.username, self.password),data=f)
		print response.status_code, 
		print response.text

		if response.status_code == 200:
			return True
		else: 
			return False

	def setStyle(self, layername, stylename):
		layer = self.catalog.get_layer(layername)
		layer.default_style = stylename
		self.catalog.save(layer)

	def createThumbnail(self, workspace, storeName, extent, width, height):
		layer = self.getLayerByStore(storeName)
		layerName = workspace+":"+layer.name
		url = self.wmsserver+"?request=GetMap&layers="+layerName+"&bbox="+extent+"&width="+width+"&height="+height+"&srs=EPSG:3857&format=image%2Fpng"

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
			return path
		else:
			return ''


	def __del__(self):
		# delete the temp dir
		if self.tempDir != None:
			try:
				import shutil
				print "Deleting temp dir ", self.tempDir
				shutil.rmtree(self.tempDir)
			except OSError as exc:
				if exc.errno != errno.ENOENT:
					raise


if __name__ == "__main__":
	geoserver = ""
	username = ""
	password = ""
	myclient = Client(geoserver, username, password)
	myclient.setStyle('geotiff', 'testing')
	# myclient.createRasterStyle('testing', f.read())
	#uploadShapefile(geoserver, username, password, "wssi", "gltg-pools-test", "gltg-pools-nad83", "gltg-pools-nad83.zip", "EPSG:4269")
	#myclient.uploadGeotiff("wssi", "test", "geotiff.tif", "EPSG:26916")
	#getLayers(geoserver, username, password)
	#l = myclient.getLayerByStore("gltg-pools")
	#if l != None:
	#	print l.name
	#else: 
	#	print "couldn't find"
