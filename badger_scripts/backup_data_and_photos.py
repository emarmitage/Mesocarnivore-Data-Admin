"""
Backup AGO Data and Attachments Script
Backs up AGO feature layer data as geojson and attachments to object storage
Includes a function to rename attachments 
Runs in GitHub Actions

Author: Emma Armitage (some code adapted from Graydon Shevchenko)
Updated: March 03 2025

"""
# imports 
import os
from minio import Minio
from minio.deleteobjects import DeleteObject
from  minio.error import S3Error
from arcgis.gis import GIS 
from copy import deepcopy
import json
from datetime import datetime, timezone, timedelta, date
from io import BytesIO
import re

import badger_config

# actually need to convert dates to ISO 8601 format

def run_app():

    ago_user, ago_pass, obj_store_user, obj_store_api_key, obj_store_host = get_input_parameters()
    report = BadgerBackupData(ago_user=ago_user, ago_pass=ago_pass, obj_store_user=obj_store_user, obj_store_api_key=obj_store_api_key, obj_store_host=obj_store_host)
    ago_item, ago_flayer, flayer_properties, flayer_data, edited_ago_item, edited_flayer_data = report.get_feature_layer_data(ago_layer_id=badger_config.BADGERS_ITEM_ID,
                                                                                                                                edited_ago_layer_id=badger_config.EDITED_ITEM_ID,
                                                                                                                                layer_name="Badger Sightings")
    report.download_attachments(ago_flayer=ago_flayer,
                                flayer_properties=flayer_properties,
                                flayer_data=flayer_data)
    
    dataset_list = [flayer_data, edited_flayer_data]
    counter = 1
    for dataset in dataset_list:
        geojson = report.convert_flayer_to_geojson(dataset)
        report.save_geojson_to_os(geojson, counter)
        counter += 1

    del report 

# get user login credentials
def get_input_parameters():
    """
    Function:
        Set up parameters

    Returns:
        tuple: user entered parameters required for tool execution
    """
    
    ago_user = os.environ['AGO_USER']
    ago_pass = os.environ['AGO_PASS']
    obj_store_user = os.environ['OBJ_STORE_USER']
    obj_store_api_key = os.environ['OBJ_STORE_API_KEY']
    obj_store_host = os.environ['OBJ_STORE_HOST']

    return ago_user, ago_pass, obj_store_user, obj_store_api_key, obj_store_host


# connect to AGOL and object storage
class BadgerBackupData:
    def __init__(self, ago_user, ago_pass, obj_store_user, obj_store_api_key, obj_store_host) -> None:
        self.ago_user = ago_user
        self.ago_pass = ago_pass
        self.obj_store_user = obj_store_user
        self.obj_store_api_key = obj_store_api_key
        self.object_store_host = obj_store_host

        self.portal_url = badger_config.MAPHUB
        self.ago_badgers_item_id = badger_config.BADGERS_ITEM_ID

        self.badger_bucket = badger_config.BUCKET
        self.bucket_prefix = "badger_sightings_photos"

        print("Connecting to MapHub")
        self.gis = GIS(url=self.portal_url, username=self.ago_user, password=self.ago_pass, expiration=9999)
        print("Connection successful")

        print("Connecting to object storage")
        self.s3_connection = Minio(obj_store_host, obj_store_user, obj_store_api_key)
        
    def __del__(self) -> None:
        print("Disconnecting from MapHub")
        del self.gis
        print("Closing object storage connection")
        # del self.boto_resource 

    # get feature layer data
    def get_feature_layer_data(self, ago_layer_id, edited_ago_layer_id, layer_name):
        ago_item = self.gis.content.get(ago_layer_id)
        if layer_name == 'Badger Sightings':
            ago_flayer = ago_item.layers[0]
        flayer_properties = ago_flayer.query()
        flayer_data = flayer_properties.features

        # get the edited ago feature layer data
        edited_ago_item = self.gis.content.get(edited_ago_layer_id)
        edited_flayer = edited_ago_item.layers[0]
        edited_flayer_properties = edited_flayer.query()
        edited_flayer_data = edited_flayer_properties.features

        return ago_item, ago_flayer, flayer_properties, flayer_data, edited_ago_item, edited_flayer_data
        
    def list_contents(self) -> list:
        """
        Get a list of object storage contents

        Returns: list of object storage contents
        """

        objects = self.s3_connection.list_objects(bucket_name=self.badger_bucket, prefix="badger_sightings_photos", recursive=True)

        lst_objects = [os.path.basename(obj.object_name) for obj in objects]

        return lst_objects
        
    def download_attachments(self, ago_flayer, flayer_properties, flayer_data) -> None:
        """
        Function:
            Runs download attachment functions
        Returns:
            None
            
        """
        # get a list of pictures from object storage
        lst_pictures = self.list_contents()

        # copy new photos to object storage
        self.copy_to_object_storage(ago_flayer=ago_flayer, 
                                    flayer_properties=flayer_properties, 
                                    flayer_data=flayer_data, 
                                    picture="photo_name", lst_os_pictures=lst_pictures)

    def copy_to_object_storage(self, ago_flayer, flayer_properties, flayer_data, picture, lst_os_pictures) -> None:
        """
        Function:
            Downloads attachments from AGO feature layer and copies them to object storage.
        Returns:
            None
        """
        print(f"Downloading photos")
        
        if len(flayer_data) == 0:
            return
            
        # save all OIDs from the feature set in a list 
        lst_oids = flayer_properties.sdf["objectid"].tolist() # may need pandas for this but unsure

        # for each object id...
        for oid in lst_oids:
            # get a list of dictionaries containings information about attachments
            lst_attachments = ago_flayer.attachments.get_list(oid=oid)

            # check if there are attachments 
            if lst_attachments:

                # find the original feature 
                original_feature = [f for f in flayer_data if f.attributes["objectid"] == oid][0]

                # try to retrieve a list of picture attributes from the records in the feature layer 
                try:
                    lst_pictures = original_feature.attributes[picture].split(',')
                except:
                    # if there are no attachments associated with the record, create an empty list
                    lst_pictures = []

                # create a list of picture that are not already saved to object storage
                lst_new_pictures = [pic for pic in lst_pictures if pic not in lst_os_pictures]
                if not lst_new_pictures:
                    continue 

                # iterate through each attachment item
                for attach in lst_attachments:

                    # if the attachment's name is in the list of new pictures, copy the item to the object storage bucket
                    if attach['name'] in lst_new_pictures:
                        print(f"Copying {attach['name']} to object storage")
                        attach_id = attach['id']
                        attach_file = ago_flayer.attachments.download(oid=oid, attachment_id=attach_id)[0]

                        ostore_path = f"{self.bucket_prefix}/{attach['name']}"

                        # Upload the file to MinIO bucket
                        try:
                            self.s3_connection.fput_object(self.badger_bucket, ostore_path, attach_file)
                            print(f"File {attach['name']} uploaded successfully to {self.badger_bucket}/{ostore_path}")
                        except S3Error as e:
                            print(f"Error uploading file {attach['name']} to MinIO: {e}")

    
    def convert_flayer_to_geojson(self, flayer_data):
        """
        Converts the feature layer data to geojson structure
        """

        print("Converting AGO data to GeoJSON format")

        # converts timestamps in AGO feature layer to correct format
        def convert_timestamp(key, value, unit='milliseconds'):
            if key in ['survey_start', 'survey_end', 'CreationDate', 'EditDate']:
                try:
                    # Convert milliseconds to seconds if needed
                    if unit == 'milliseconds':
                        value = value / 1000
                    
                    formatted_date = datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
                    return formatted_date
                except (OSError, OverflowError, ValueError):
                    return value
                    
            return value

        # create geojson structure
        geojson_dict = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            feature.geometry['x'],
                            feature.geometry['y']
                        ]
                    },
                    "properties": {
                        key: convert_timestamp(key, value, unit='milliseconds') for key, value in feature.attributes.items()
                    }
                }
                for feature in flayer_data
            ]
        }

        # convert dict to geojson
        geojson = json.dumps(geojson_dict, indent=2)

        return geojson

    def save_geojson_to_os(self, geojson, counter):
        """
        Saves the geojson to object storage
        """
        print("Saving GeoJSON to object storage")

        # now = datetime.now().strftime("%Y-%m-%d")
        today = date.today()
        thirty_days_ago = today - timedelta(days=30)

        if counter == 1:

            ostore_path = f'backup_data/survey123_raw_backup_data_{today}.geojson'

        else:
            ostore_path = f'backup_data/survey123_edited_backup_data_{today}.geojson'

        bucket_name = self.badger_bucket

        # delete existing data in the bucket
        objects = self.s3_connection.list_objects(bucket_name=self.badger_bucket, prefix="backup_data", recursive=True)

        lst_objects = [obj.object_name for obj in objects]
        
        # list for files older that 30 days 
        lst_old_objs = []
        
        for obj in lst_objects:
            match = re.search(r"\d{4}-\d{2}-\d{2}", obj)

            if match == None:
                print(f"..no match found for object {obj}")
                continue

            else:
                extracted_date = datetime.strptime(match.group(), "%Y-%m-%d").date()

                if extracted_date < thirty_days_ago:
                    lst_old_objs.append(obj)


        # upload geojson file
        try:
            geojson_data = BytesIO(geojson.encode('utf-8'))

            self.s3_connection.put_object(
                bucket_name=bucket_name,
                object_name=ostore_path,
                data=geojson_data,
                length=-1,
                part_size=5 * 1024 * 1024, # 5MB
                content_type='application/geo+json'
            )
            print(f"GeoJSON data has been uploaded to s3://{bucket_name}/{ostore_path}")
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == '__main__':
    run_app()
