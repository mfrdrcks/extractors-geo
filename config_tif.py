#!/usr/bin/env python
#-----------------------------------------------------------------------
#BEGIN CONFIGURATION
# ----------------------------------------------------------------------
#rabbitmqURL Parameters
rabbitmqURL = ""

#exchane name
exchange = "medici"

# name to show in rabbitmq queue list
extractorName = "ncsa.geotiffExtractor"


# accept any type of file that is text
routingKeys = ["*.file.image.tiff","*.file.image.tif"]


# trust certificates, set this to false for self signed certificates
sslVerify=False

# Geoserver setting
geoServer = ""
gs_username = ""
gs_password = ""
gs_workspace = ""
raster_style = ""
# ----------------------------------------------------------------------
# END CONFIGURATION
# ----------------------------------------------------------------------


