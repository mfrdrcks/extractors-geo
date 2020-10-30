#!/usr/bin/python
import json
import urllib
from osgeo import osr
from osgeo import ogr
import tempfile
import subprocess
import errno
import os
import os.path
import time
import logging


class Utils:
    zipUtil = "/usr/bin/7z"

    def __init__(self, shpzipfile, zipUtil="/usr/bin/7z"):
        self.zipUtil = zipUtil
        self.shpzipfile = shpzipfile

        self.zipShpProp = {}
        self.zipShpProp['hasError'] = False
        self.zipShpProp['hasDir'] = False
        self.zipShpProp['shpFile'] = None
        self.zipShpProp['shxFile'] = None
        self.zipShpProp['dbfFile'] = None
        self.zipShpProp['prjFile'] = None
        self.zipShpProp['shpName'] = None
        self.zipShpProp['hasSameName'] = True
        self.zipShpProp['numShp'] = 0
        self.zipShpProp['targetZip'] = None
        self.zipShpProp['epsg'] = 'UNKNOWN'
        self.zipShpProp['extent'] = 'UNKNOWN'
        logging.basicConfig(format="%(asctime)-15s %(name)-10s %(levelname)-7s : %(message)s", level=logging.WARN)
        self.logger = logging.getLogger("zipshputils")
        self.logger.setLevel(logging.DEBUG)
        # setup logging for the gsclient
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)
        ts = time.gmtime()
        self.time_stamp = str(ts.tm_mon) + str(ts.tm_hour) + str(ts.tm_min) + str(ts.tm_sec)

        # create temp directory
        self.logger.debug("Creating temp dir ...")
        self.tempDir = tempfile.mkdtemp()
        self.logger.debug(self.tempDir + " [DONE]")

        # unzip compressed file
        self.logger.debug("Uncompress the file ...")
        output = subprocess.check_output([zipUtil, 'x', '-o%s' % self.tempDir, shpzipfile], shell=False,
                                         stderr=subprocess.STDOUT)
        self.logger.debug(output)
        self.logger.debug(shpzipfile + " [DONE]")

        # check the self.zipShpProp
        self.files = [os.path.join(self.tempDir, f) for f in os.listdir(self.tempDir)]

        self.no_proj = 'no_proj'

        if not self.checkZipShp():
            # find projection
            epsg_code = self.findProjection()
            if epsg_code == self.no_proj:
                self.zipShpProp['epsg'] = 'UNKNOWN'
                self.zipShpProp['hasError'] = True
            else:
                if epsg_code != 'None':
                    self.zipShpProp['epsg'] = epsg_code
                else:
                    self.zipShpProp['epsg'] = 'UNKNOWN'

                # find extent
                if self.zipShpProp['epsg'] != 'UNKNOWN':
                    extent = self.findExtent()
                    if extent != 'None':
                        self.zipShpProp['extent'] = extent
                    else:
                        self.zipShpProp['extent'] = 'UNKNOWN'
                if self.zipShpProp['epsg'] == 'UNKNOWN' or self.zipShpProp['extent'] == 'UNKNOWN':
                    self.zipShpProp['hasError'] = True

    def __del__(self):
        # delete the temp dir
        if self.tempDir != None:
            try:
                import shutil
                self.logger.debug("Deleting temp dir " + self.tempDir)
                shutil.rmtree(self.tempDir)
            except OSError as exc:
                if exc.errno != errno.ENOENT:
                    raise

    def hasError(self):
        return self.zipShpProp['hasError']

    def getEpsg(self):
        return self.zipShpProp['epsg']

    def getExtent(self):
        return self.zipShpProp['extent']

    def getShpName(self):
        return self.zipShpProp['shpName']

    def checkZipShp(self):
        for f in self.files:
            name, ext = os.path.splitext(os.path.basename(f))
            self.logger.debug("shp name : " + name + "  ext: " + ext)
            os.rename(self.tempDir + "/" + os.path.basename(f),
                      self.tempDir + "/" + os.path.basename(self.shpzipfile) + self.time_stamp + ext)
        for f in os.listdir(self.tempDir):
            self.logger.debug("file name" + os.path.basename(f))
        for f in self.files:
            name, ext = os.path.splitext(os.path.basename(f))
            self.logger.debug("shp rename : " + name + "  re-ext: " + ext)
        self.files = [os.path.join(self.tempDir, f) for f in os.listdir(self.tempDir)]
        # os.rename(self.tempDir+"/"+os.path.basename(f), name+self.time_stamp+'.ext')
        for f in self.files:
            name, ext = os.path.splitext(os.path.basename(f))
            self.logger.debug("shp name : " + name + "  ext: " + ext)
            if (os.path.isdir(f)):
                self.zipShpProp['hasDir'] = True
                self.zipShpProp['hasError'] = True
                continue

            if (ext.lower() == '.shp'):
                self.zipShpProp['numShp'] = self.zipShpProp['numShp'] + 1
                self.zipShpProp['shpFile'] = f
                if self.zipShpProp['shpName'] == None:
                    self.zipShpProp['shpName'] = name
                else:
                    if (self.zipShpProp['shpName'] != name):
                        self.zipShpProp['hasSameName'] = False
                        self.zipShpProp['hasError'] = True
                        self.zipShpProp['shpName'] = name

            if (ext.lower() == '.shx'):
                self.zipShpProp['shxFile'] = f
                if self.zipShpProp['shpName'] == None:
                    self.zipShpProp['shpName'] = name
                else:
                    if (self.zipShpProp['shpName'] != name):
                        self.zipShpProp['hasSameName'] = False
                        self.zipShpProp['hasError'] = True

            if (ext.lower() == '.dbf'):
                self.zipShpProp['dbfFile'] = f
                if self.zipShpProp['shpName'] == None:
                    self.zipShpProp['shpName'] = name
                else:
                    if (self.zipShpProp['shpName'] != name):
                        self.zipShpProp['hasSameName'] = False
                        self.zipShpProp['hasError'] = True

            if (ext.lower() == '.prj'):
                self.zipShpProp['prjFile'] = f
                if self.zipShpProp['shpName'] == None:
                    self.zipShpProp['shpName'] = name
                else:
                    if (self.zipShpProp['shpName'] != name):
                        self.zipShpProp['hasSameName'] = False
                        self.zipShpProp['hasError'] = True
            # self.logger.debug("ShapeFileName: "+ self.zipShpProp['shpName'])
        if self.zipShpProp['shpFile'] == None or self.zipShpProp['shxFile'] == None or self.zipShpProp[
            'dbfFile'] == None or self.zipShpProp['prjFile'] == None:
            self.zipShpProp['hasError'] = True

        if self.zipShpProp['numShp'] > 1:
            self.zipShpProp['hasError'] = True

        return self.zipShpProp['hasError']

    def findProjection(self):
        if self.zipShpProp['hasError']:
            self.logger.debug('findProjection: Zipped Shapefile has error')
            return None

        prj_file = open(self.zipShpProp['prjFile'], 'r')
        prj_txt = prj_file.read()
        prj_file.close()
        prj_code = 'None'

        try:
            srs = osr.SpatialReference()
            # check if the projection is not working projection
            epsg_no = self.checkSpecialProjection(prj_txt)
            if epsg_no > 0:
                logging.debug("the projection does not work correctly")
                return self.no_proj
            srs.ImportFromESRI([prj_txt])
            srs.AutoIdentifyEPSG()
            prj_code = srs.GetAuthorityCode(None)

        except:
            prj_code = 'None'

        if str(prj_code).strip() != 'None':
            return prj_code

        query = urllib.urlencode({'exact': True, 'error': True, 'mode': 'wkt', 'terms': prj_txt})

        try:
            webres = urllib.urlopen('http://prj2epsg.org/search.json', query.encode())
            jres = json.loads(webres.read().decode())
            if len(jres['codes']) > 0:
                prj_code = jres['codes'][0]['code']
            else:
                prj_code = 'None'
        except:
            prj_code = 'None'

        return prj_code

    def checkSpecialProjection(self, prj_txt):
        special_prj_list = [['Albers_Equal_Area_Conic', 102008]]

        for i in range(len(special_prj_list)):
            if special_prj_list[i][0] in prj_txt:
                return special_prj_list[i][1]

        return 0

    def findExtent(self):
        if self.zipShpProp['hasError']:
            self.logger.debug('findExtent: Zipped Shapefile has error')
            return 'None'
        if self.zipShpProp['epsg'] == 'UNKNOWN':
            self.logger.debug('findExtent: Unknown projection; could not calculate extent')
            return 'None'

        shpfile = ogr.Open(self.zipShpProp['shpFile'])
        osrs = osr.SpatialReference()
        osrs.ImportFromEPSG(int(self.zipShpProp['epsg']))
        dsrs = osr.SpatialReference()
        dsrs.ImportFromEPSG(3857)  # google projeciton in epsg code
        ct = osr.CoordinateTransformation(osrs, dsrs)
        layer = shpfile.GetLayer(0)
        a = layer.GetExtent()
        proj = layer.GetSpatialRef()
        if proj.GetAttrValue("AUTHORITY", 1) == '4326':
            a = self.validateBbox(a)
        ab = ct.TransformPoint(a[2], a[0], 0)
        cd = ct.TransformPoint(a[3], a[1], 0)
        r = [ab[0], ab[1], cd[0], cd[1]]

        return ','.join(map(str, r))

    def validateBbox(self, intuple):
        lst = list(intuple)
        tuple_changed = False
        if intuple[0] <= 180 and intuple[0] > 179:
            lst[0] = 179
            tuple_changed = True
        if intuple[0] >= -180 and intuple[0] < -179:
            lst[0] = -179
            tuple_changed = True
        if intuple[1] <= 180 and intuple[1] > 179:
            lst[1] = 179
            tuple_changed = True
        if intuple[1] >= -180 and intuple[1] < -179:
            lst[1] = -179
            tuple_changed = True
        if intuple[2] <= 90 and intuple[2] > 89:
            lst[2] = 89
            tuple_changed = True
        if intuple[2] >= -90 and intuple[2] < -89:
            lst[2] = -89
            tuple_changed = True
        if intuple[3] <= 90 and intuple[3] > 89:
            lst[3] = 89
            tuple_changed = True
        if intuple[3] >= -90 and intuple[3] < -89:
            lst[3] = -89
            tuple_changed = True

        if tuple_changed:
            return tuple(lst)
        else:
            return intuple

    def createZip(self, destinationDir, newname):
        if self.zipShpProp['hasError']:
            self.logger.debug('createZip: Zipped Shapefile has error')
            return None
        self.logger.debug("shpName: " + self.zipShpProp['shpName'])
        # rezip the files in zip format if it is not a zip file
        self.zipShpProp['targetZip'] = os.path.join(destinationDir, newname + '.zip')
        self.logger.debug("targetZip : " + self.zipShpProp['targetZip'])

        # rename all the files in the folder
        self.zipShpProp['shpName'] = newname
        filenames = os.listdir(self.tempDir)
        for filename in filenames:
            title, ext = os.path.splitext(os.path.basename(filename))
            os.rename(self.tempDir + "/" + filename, self.tempDir + "/" + newname + ext)
        subprocess.check_call([self.zipUtil, 'a', '-tzip', self.zipShpProp['targetZip'], self.tempDir + '/*'],
                              shell=False)
        return self.zipShpProp['targetZip']


if __name__ == "__main__":
    source = "gltg.7z"
    zipshp = Utils(source)
    zipshp2 = Utils("gltg-folder.7z")

    if not zipshp.hasError():
        print (zipshp.zipShpProp)
        zipshp.createZip('.')
    print (zipshp2.hasError())
    print (zipshp2.zipShpProp)
