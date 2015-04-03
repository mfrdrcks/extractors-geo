#!/usr/bin/env python
#-----------------------------------------------------------------------
#BEGIN CONFIGURATION
# ----------------------------------------------------------------------
#rabbitmqURL Parameters
rabbitmqURL = ""

#exchane name
exchange = "medici"

# name to show in rabbitmq queue list
extractorName = "ncsa.geo.shpExtractor"


# accept any type of file that is text
routingKeys = ["*.file.multi.files-zipped.#", "*.file.application.zip", "*.file.application.x-zip", "*.file.application.x-7z-compressed"]
#routingKeys = ["*.file.multi.files-zipped.#"]
#routingKeys = ["*.file.application.zip"]


# trust certificates, set this to false for self signed certificates
sslVerify=False

# Geoserver setting
geoServer = ""
gs_username = ""
gs_password = ""
gs_workspace = ""

# ----------------------------------------------------------------------
# END CONFIGURATION
# ----------------------------------------------------------------------


