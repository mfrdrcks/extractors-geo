# =============================================================================
#
# In order for this extractor to run according to your preferences,
# the following parameters need to be set.
#
# Some parameters can be left with the default values provided here - in that
# case it is important to verify that the default value is appropriate to
# your system. It is especially important to verify that paths to files and
# software applications are valid in your system.
#
# =============================================================================

import os

# name to show in rabbitmq queue list
extractorName = os.getenv('RABBITMQ_QUEUE', "ncsa.geoshp.preview")

# URL to be used for connecting to rabbitmq
rabbitmqURL = os.getenv('RABBITMQ_URI', "amqp://guest:guest@localhost/%2f")

# name of rabbitmq exchange
rabbitmqExchange = os.getenv('RABBITMQ_EXCHANGE', "clowder")

# type of files to process
messageType = ["*.file.multi.files-zipped.#",
               "*.file.application.zip",
               "*.file.application.x-zip",
               "*.file.application.x-7z-compressed"]

# trust certificates, set this to false for self signed certificates
sslVerify = os.getenv('RABBITMQ_SSLVERIFY', False)

# Geoserver setting
geoServer = os.getenv('GEOSERVER_URL', '')
gs_username = os.getenv('GEOSERVER_USERNAME', 'admin')
gs_password = os.getenv('GEOSERVER_PASSWORD', 'geoserver')
gs_workspace = os.getenv('GEOSERVER_WORKSPACE', 'clowder')
