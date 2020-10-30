#!/usr/bin/env python

import logging
import os
import re
import gdal

# Geotiff-specific modules: GDAL and pygeoprocessing.
import pygeoprocessing.geoprocessing as geoprocess

from pyclowder.extractors import Extractor
import pyclowder.files

# Author: Mostafa Elag, Rui Liu, Yong Wook Kim
# Date: Feb 2016.


class MetadataGeotiff(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # parse command line and load default logging configuration
        self.setup()
        logging.basicConfig(level=logging.INFO)
        # setup logging for the exctractor
        logging.getLogger("pika").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

        logging.getLogger('pyclowder').setLevel(logging.INFO)
        logging.getLogger('__main__').setLevel(logging.INFO)


    """Process the file and upload the metadata."""

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.extractorName = os.getenv('RABBITMQ_QUEUE', "ncsa.geotiff.metadata")
        self.messageType = ["*.file.image.tiff", "*.file.image.tif"]

        self.logger = logging.getLogger(__name__)

        input_file = resource["local_paths"][0]
        fileid = resource['id']
        result = self.parse_geotiff(input_file)

        # Context URL
        context_url = "https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"

        metadata = {
        "@context": [
            context_url,
            {'raster': 'http://clowder.ncsa.illinois.edu/metadata/ncsa.geotiff.metadata#raster'}
        ],
        'attachedTo': {'resourceType': 'file', 'id': parameters["id"]},
            'agent': {
                '@type': 'cat:extractor',
            'extractor_id': 'https://clowder.ncsa.illinois.edu/clowder/api/extractors/' + self.extractorName},
        'content': {'raster': result}
        }

        pyclowder.files.upload_metadata(connector, host, secret_key, fileid, metadata)

    """Extract and return metadata from the Geotiff file."""
    def parse_geotiff(self, input_file):
        # This method was originally written by Dr. Mostafa Elag.
        raster_uri = input_file
        # Get the bounding box of a raster
        # bbox_list = geoprocess.get_bounding_box(raster_uri)
        raster_info = geoprocess.get_raster_info(raster_uri)
        boundingbox = raster_info.get('bounding_box')
        bbox_list = [boundingbox[0], boundingbox[3], boundingbox[2], boundingbox[1]]
        # Get the cell size of a raster
        cell_size = raster_info.get('pixel_size')
        # cell_size = geoprocess.get_cell_size_from_uri(raster_uri)

        # Get the projection of a raster as well-known text
        proj_wkt = raster_info.get('projection_wkt')
        # proj_wkt = geoprocess.get_dataset_projection_wkt_uri(raster_uri)
        proj = re.findall('"([^"]*)"', proj_wkt)[0]

        # Get the datatype of a raster
        dtype = raster_info.get('datatype')
        # dtype = geoprocess.get_datatype_from_uri(raster_uri)

        # Get dimension and size properties of a raster
        properties_dict = dict()
        properties_dict['width'] = raster_info.get('pixel_size')[0]
        properties_dict['height'] = raster_info.get('pixel_size')[1]
        properties_dict['x_size'] = raster_info.get('raster_size')[0]
        properties_dict['y_size'] = raster_info.get('raster_size')[1]
        # properties_dict = geoprocess.raster_properties()
        # Get a raster's attribute table
        attr_dict = {1: 'type1', 2: 'type2'}
        gtif = gdal.Open(raster_uri)
        srcband = gtif.GetRasterBand(1)
        # Get raster statistics
        stats = srcband.GetStatistics(False, True)
        rast_stats = dict()
        rast_stats['max'] = stats[1]
        rast_stats['min'] = stats[0]
        rast_stats['average'] = stats[2]
        rast_stats['st-dev'] = stats[3]
        #geoprocess.create_rat_uri(raster_uri, attr_dict, column_name)
        # rat_dict = geoprocess.get_rat_as_dictionary_uri(raster_uri)

        # Get the number of rows and columns of a raster
        row_col = (properties_dict['y_size'], properties_dict['x_size'])
        # row_col = geoprocess.get_row_col_from_uri(raster_uri)

        # Get statistics of a raster
        # stat = geoprocess.get_statistics_from_uri(raster_uri)
        # stats = {'min': stat[0], 'max': stat[1], 'average': stat[2], 'st-dev': stat[3]}

        # Need to add the context and field from the GML
        ''' do we need to call the SAS spatial annotation from here or directly add  the context and fields'''
        Raster_info = {'GeoJSON': {'type': 'Polygon', 'coordinates': [
            [[bbox_list[0], bbox_list[3]], [bbox_list[0], bbox_list[1]], [bbox_list[2], bbox_list[1]],
             [bbox_list[2], bbox_list[3]], [bbox_list[0], bbox_list[3]]]]}, 'box': bbox_list, 'proj': proj,
                       'properties': properties_dict, 'nrow_col': row_col, 'rast_stats': rast_stats}

        return Raster_info

        ###### Extra function that may be used later

        # Get the nodata value of a raster
        #nodata_val = geoprocess.get_nodata_from_uri(raster_uri)

        # Get raster band as numpy memmap array
        #temp_file = geoprocess.temporary_filename()
        #mm_array = geoprocess.load_memory_mapped_array(raster_uri, temp_file)


        # Get list of unique values in raster
        #unique_vals_list = geoprocess.unique_raster_values_uri(raster_uri)

        # Get dictionary of unique values in raster and their count
        #unique_vals_count_dict = geoprocess.unique_raster_values_count(raster_uri)
        #print unique_vals_count_dict

        #defaultdict(<type 'int'>, {0.0: 3, 0.5: 3, 1.0: 3})

        # Get the intersection between rasters
        #dataset_1 = gdal.Open(raster_1_uri)
        #dataset_2 = gdal.Open(raster_2_uri)
        #r_list = [dataset_1, dataset_2]
        #bbox_list = geoprocess.calculate_intersection_rectangle(r_list)

    
if __name__ == "__main__":
    extractor = MetadataGeotiff()
    extractor.start()
