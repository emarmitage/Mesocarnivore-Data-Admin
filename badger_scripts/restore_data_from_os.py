"""
Author: Emma Armitage, emma.armitage@gov.bc.ca
Date: Aug 28, 2024
Purpose: Uploads data from object storage to an ArcGIS Online Feature layer. 
"""

from arcgis.gis import GIS
from arcgis.features import Feature
import boto3
import botocore
import os
from datetime import datetime
import re
import json

def run_app():
    gis = connect_to_ago()
    boto_resource = connect_to_object_storage()
    geojson_data = get_object_storage_content(boto_resource)
    ago_flayer = get_feature_layer(gis, ago_layer_id='fdb949b3807b4837ab77daeb7a737238') # Editing feature layer
    # restore_data(ago_flayer=ago_flayer, geojson_data=geojson_data, badger_bucket='bmrm', boto_resource=boto_resource) # uncomment this line to run script

def connect_to_ago():
    """
    Connect to ArcGIS Online

    Returns: ArcGIS Online connection
    """

    ago_user = os.environ['AGO_USER']
    ago_pass = os.environ['AGO_PASS']
    url = 'https://governmentofbc.maps.arcgis.com/home'

    gis = GIS(username=ago_user, password=ago_pass, url=url, expiration=9999)

    return gis

def connect_to_object_storage():
    """
    Connect to Amazon S3 Object Storage Bucket

    Returns: object storage connection
    """
    obj_store_user = os.environ['OBJ_STORE_USER'] 
    obj_store_api_key = os.environ['OBJ_STORE_API_KEY']
    obj_store_host = os.environ['OBJ_STORE_HOST']

    boto_resource = boto3.resource(service_name='s3',
                                    aws_access_key_id=obj_store_user,
                                    aws_secret_access_key=obj_store_api_key,
                                    endpoint_url=f'https://{obj_store_host}')
    
    return boto_resource

def get_object_storage_content(boto_resource):
    """
    Downloads and reads the most recent geojson file from object storage.
    The geojson file is the backup from the ArcGIS Online feature layer

    Returns: geojson dictionary
    """
    badger_bucket = 'bmrm'

    obj_bucket = boto_resource.Bucket(badger_bucket)
    lst_objects = []
    for obj in obj_bucket.objects.all():
        lst_objects.append(os.path.basename(obj.key))

    # get a list of the geojson files
    geojson_extension = '.geojson'
    lst_geojson = [geojson for geojson in lst_objects if geojson.lower().endswith(geojson_extension)]

    # the date pattern in the file name
    date_pattern = re.compile(r'(\d{2}-\d{2}-\d{4})')

    # extract the date from the geojson file name
    def extract_date(file_name):
        match = date_pattern.search(file_name)
        if match:
            date_str = match.group(1)
            return datetime.strptime(date_str, '%d-%m-%Y')
        return None

    # find the most recent geojson file
    geojson = max(lst_geojson, key=lambda file: extract_date(file))

    # define path to save temporary geojson file
    tmp_file_path = f'/tmp/{geojson}'

    try:
        # download file
        boto_resource.Bucket(badger_bucket).download_file(Key=f'backup_data/{geojson}', Filename=tmp_file_path)

        # read file
        with open(tmp_file_path, 'r') as f:
            geojson_data = json.load(f)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

    return geojson_data

def get_feature_layer(gis, ago_layer_id):
    """
    Returns the ArcGIS Online feature layer whose data needs to be restored
    """
    ago_item = gis.content.get(ago_layer_id)
    ago_flayer = ago_item.layers[0]

    return ago_flayer

def restore_data(ago_flayer, geojson_data, badger_bucket, boto_resource):
    """
    Copies geojson features to AGO feature layer. 
    When the feature has photos, it downloads those photos from object storage and adds them as attachments to the feature

    ***Make sure you uncomment the lines below. They delete all existing features in the AGO feature layer
    """
    # delete existing features
    print("Deleting existing features")
    ago_flayer.manager.truncate()

    # append geojson data
    features = [Feature(geometry=feature['geometry'], attributes=feature['properties']) for feature in geojson_data['features']]

    print('Adding features to AGO feature layer')
    for feature in features:

        photo_names = feature.attributes['photo_name']

        try:
            # add backup feature to feature layer
            response = ago_flayer.edit_features(adds=[feature])

            if response.get('addResults'):
                for result in response['addResults']:
                    if not result['success']:
                        print(f"Feature add failed: {result['error']}")
            
                    else:
                        # check if the feature layer has attachments
                        if photo_names is not None:
                            # get feature oid
                            oid = result['objectId']

                            # upload photos from object storage to AGO feature layer
                            upload_attachments(photo_names, ago_flayer, boto_resource, badger_bucket, oid)


        except Exception as e:
            print(f"Error during feature update: {e}")

def upload_attachments(photo_names, ago_flayer, boto_resource, badger_bucket, oid):
    photo_names_list = photo_names.split(",")

    for photo_name in photo_names_list:

        # define path to save temp photo file
        tmp_photo_path = f'/tmp/{photo_name}'

        try:
            # download the file
            boto_resource.Bucket(badger_bucket).download_file(Key=f'badger_sightings_photos/{photo_name}', Filename=tmp_photo_path)

            # upload the file to ago feature layer
            try:
                print(f"Adding {photo_name}")
                ago_flayer.attachments.add(oid=oid, file_path=tmp_photo_path)

            except Exception as e:
                print(f"Photo upload failed with Exception: {e}")

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

        finally:
            if os.path.exists(tmp_photo_path):
                os.remove(tmp_photo_path)


if __name__ == '__main__':
    run_app()
