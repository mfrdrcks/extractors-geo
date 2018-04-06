# =============================================================================
# this will create the xml string of the dataset for inserting it into pycsw
# the parameters are defined in pycsw_insert_template.xml file
# if you change the names of these parameters to replaced,
# the smae parameters in pycsw_insert_template.xml should be changed
# create by ywkim at Mar 14, 2018
# =============================================================================
#!/usr/bin/env python
import requests
import os, inspect

from config import *
from pyproj import Proj, transform


"""
construnct xml for dataset insert to pycsw
"""
def construct_insert_xml(xml_identifier, xml_reference, xml_isFeature, xml_subject, xml_keyword, xml_title,
                         xml_lower_corner, xml_upper_corner):
    # read xml template
    currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    xml_tempfile = os.path.join(currentdir, "pycsw_insert_template.xml")
    xml_read = open(xml_tempfile, 'r')
    xml_temp_str = xml_read.read()
    xml_read.close()

    # replace parameters
    # the string are located in pycsw_insert_template.xml
    # if you change the string values, the strings in xml file should also be changed.
    rep_identifier = '%=identifier=%'
    rep_reference = '%=reference=%'
    rep_feature = '%=isFeature=%'
    rep_subject_title = '%=subjectTitle=%'
    rep_keyword = '%=keyword=%'
    rep_title = '%=title=%'
    rep_lowercorner = '%=lowerCorner=%'
    rep_uppercorner = '%=upperCorner=%'

    xml_temp_str = xml_temp_str.replace(rep_identifier, xml_identifier)
    xml_temp_str = xml_temp_str.replace(rep_reference, xml_reference)
    xml_temp_str = xml_temp_str.replace(rep_feature, xml_isFeature)
    xml_temp_str = xml_temp_str.replace(rep_subject_title, xml_subject)
    xml_temp_str = xml_temp_str.replace(rep_title, xml_title)
    xml_temp_str = xml_temp_str.replace(rep_lowercorner, xml_lower_corner)
    xml_temp_str = xml_temp_str.replace(rep_uppercorner, xml_upper_corner)

    # replace keyword
    temp_keyword_str = ''
    for i in range(len(xml_keyword)):
        temp_keyword_str = temp_keyword_str + '<dc:keyword>' + xml_keyword[i] + '</dc:keyword>'
    xml_temp_str = xml_temp_str.replace(rep_keyword, temp_keyword_str)

    return xml_temp_str


"""
parse bounding box information from layer url
"""
def parse_bbox_from_url(url):
    bbox_list = []
    for line in url.split('&'):
        elements = line.split("=")
        if (elements[0]).lower() == 'bbox':
            for bbox in elements[1].split(','):
                bbox_list.append(bbox)

    bbox_list = pu.convert_bounding_box_3857_4326(bbox_list)

    # x and y should be switched in the xml to insert it to pycsw
    b1 = bbox_list[0]
    b2 = bbox_list[1]
    b3 = bbox_list[2]
    b4 = bbox_list[3]
    bbox_list[0] = b2
    bbox_list[1] = b1
    bbox_list[2] = b4
    bbox_list[3] = b3

    return bbox_list


"""
post xml to pycsw
"""
def post_insert_xml(xml_str):
    global pycsw_server
    result = requests.post(pycsw_server, data = xml_str)

    return result

"""
convert EPSG 3857 bounding box to 4326 bounding box
"""
def convert_bounding_box_3857_4326(bbox):
    inProj = Proj(init='epsg:3857')
    outProj = Proj(init='epsg:4326')
    bbox[0], bbox[1] = transform(inProj,outProj, bbox[0], bbox[1])
    bbox[2], bbox[3] = transform(inProj, outProj, bbox[2], bbox[3])

    return bbox

if __name__ == '__main__':
    # test function for inserting xml to pycsw server
    rest_url = 'http://10.193.101.22:8000/pycsw'
    is_feature = False  # true: shapefile, false: geotiff
    if (is_feature):
        xml_identifier = 'clowder:Bedrock_topography_1994.zip_5a37ecc74f0cff3dedae2f1a'
        xml_reference = 'clowder:Bedrock_topography_1994.zip_5a37ecc74f0cff3dedae2f1a'
        xml_isFeature = 'features'
        xml_subject = 'Bedrock_topography_1994.zip_5a37ecc74f0cff3dedae2f1a'
        xml_keyword = ['test1', 'test2']
        xml_title = 'Bedrock_topography_1994.zip_5a37ecc74f0cff3dedae2f1a'
        xml_lower_corner = '36.968509499999996 -91.51091716666667'
        xml_upper_corner = '42.51070925 -87.49870283333334'
    else:
        xml_identifier = 'clowder:1-2010_30m_cdls_il_wgs84.tif_5a721ed45e0ec566b56f3e0b'
        xml_reference = 'clowder:1-2010_30m_cdls_il_wgs84.tif_5a721ed45e0ec566b56f3e0b'
        xml_isFeature = 'GeoTIFF'
        xml_subject = '1-2010_30m_cdls_il_wgs84.tif_5a721ed45e0ec566b56f3e0b'
        xml_keyword = ['test1', 'test2']
        xml_title = '1-2010_30m_cdls_il_wgs84.tif_5a721ed45e0ec566b56f3e0b'
        xml_lower_corner = '36.8774350196876 -91.69904227423626'
        xml_upper_corner = '42.72041279362245 -87.01986323987131'

    xml_str = construct_insert_xml(xml_identifier, xml_reference, xml_isFeature, xml_subject, xml_keyword,
                                   xml_title,
                                   xml_lower_corner, xml_upper_corner)

    post_insert_xml(rest_url, xml_str)