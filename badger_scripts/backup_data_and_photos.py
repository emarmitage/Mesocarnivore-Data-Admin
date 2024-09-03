"""
Backup AGO Data and Attachments Script
Backs up AGO feature layer data as geojson and attachments to object storage
Includes a function to rename attachments 
Written to run in GitHub Actions

Author: Emma Armitage (some code adapted from Graydon Shevchenko)
Aug 29, 2024

"""
# imports 
import os
import boto3
from arcgis.gis import GIS 
from copy import deepcopy
import json
from datetime import datetime, timezone
from io import BytesIO

import badger_config

def run_app():

    ago_user, ago_pass, obj_store_user, obj_store_api_key, obj_store_host = get_input_parameters()
    report = BadgerBackupData(ago_user=ago_user, ago_pass=ago_pass, obj_store_user=obj_store_user, obj_store_api_key=obj_store_api_key, obj_store_host=obj_store_host)
    ago_item, ago_flayer, flayer_properties, flayer_data, edited_ago_item, edited_flayer_data = report.get_feature_layer_data(ago_layer_id=badger_config.BADGERS_ITEM_ID,
                                                                                                                                edited_ago_layer_id=badger_config.EDITED_ITEM_ID,
                                                                                                                                layer_name="Badger Sightings")
    report.rename_attachments(ago_flayer=ago_flayer,
                              flayer_properties=flayer_properties,
                              flayer_data=flayer_data)
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
        self.boto_resource = boto3.resource(service_name='s3',
                                            aws_access_key_id=self.obj_store_user,
                                            aws_secret_access_key=self.obj_store_api_key,
                                            endpoint_url=f'https://{self.object_store_host}')
        
    def __del__(self) -> None:
        print("Disconnecting from MapHub")
        del self.gis
        print("Closing object storage connection")
        del self.boto_resource 

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
    
    def clean_filename(self, filename: str) -> str:
        """
        Removes any invalid characters from date fields in AGO feature layer data 
        The date field is used to construct the new attachment name

        Returns: date w/o invalid characters
        """    

        # define invalid path characteres
        invalid_chars = '<>:"/\\|?*'

        # clean the file name
        clean_filename = ''.join('-' if c in invalid_chars else c for c in filename).rstrip('. ')

        return clean_filename
    
    def download_attachment(self, ago_flayer, oid, attachment_id):
        """
        Downloads attachments on an AGO feature layer into GitHub Actions temporary directory

        Returns: downloaded file
        """
        # download the file 
        file = ago_flayer.attachments.download(oid=oid, attachment_id=attachment_id)[0]

        if not file:
            raise ValueError("Failed to download the attachment")
        
        return file
    
    def rename_file(self, file_path: str, new_name: str) -> str:
        """
        Renames attachments

        Returns: file path to renamed attachment
        """
        new_path = os.path.join(os.path.dirname(file_path), new_name)

        os.rename(file_path, new_path)

        return new_path


    # rename attachments
    def rename_attachments(self, ago_flayer, flayer_properties, flayer_data):
        """
        Renames photos on AGO feature layer

        Returns: None
        """

        list_oids = flayer_properties.sdf['objectid'].tolist()

        features_for_update = []

        for oid in list_oids:

            # for each oid, get a list of it's attachments
            attachments_list = ago_flayer.attachments.get_list(oid=oid)

            # if the feature has attachments 
            if attachments_list:

                # get attributes from the feature associated with the attachment
                original_feature = [f for f in flayer_data if f.attributes['objectid'] == oid][0]
                    
                # get sighting date response 
                sighting_date = original_feature.attributes['sighting_date_response']

                # remove any invalid path characters from the sighting date
                clean_sighting_date = self.clean_filename(filename=sighting_date)

                photo_name_list = []

                # initialize attachment counter
                attachment_counter = 1

                for attachment in attachments_list:

                    # get the current attachment name
                    current_attach_name = attachment['name']

                    # get the attachment id
                    attach_id = attachment['id']

                    # variable indicating if the feature should be updated
                    update = False

                    # check if the photo has already been renamed 
                    if current_attach_name.startswith(f"{oid}_{clean_sighting_date}"):
                        continue

                    # otherwise rename the file 
                    else:

                        # get the file type (ex: png, jpg, jpeg)
                        file_type = current_attach_name.split('.')[-1]

                        # create the new attachment name 
                        attachment_name = f"{oid}_{clean_sighting_date}_{attachment_counter}.{file_type}"

                        # increment the attachment counter by 1 
                        attachment_counter += 1

                        # download the file 
                        file = self.download_attachment(ago_flayer=ago_flayer, 
                                                         oid=oid, 
                                                         attachment_id=attach_id)

                        # new attach file path
                        new_attach_file = self.rename_file(file_path=file, 
                                                           new_name=attachment_name)


                        # download attachments
                        try:
                            ago_flayer.attachments.update(oid=oid,
                                                            attachment_id=attach_id,
                                                            file_path=new_attach_file)
                            
                        except:
                            ago_flayer.attachments.add(oid=oid, file_path=new_attach_file)
                            ago_flayer.attachments.delete(oid=oid, attachment_id=attach_id)

                        # append the file names to photo_name_list
                        photo_name_list.append(attachment_name)

                        # set the update variable to True
                        update = True

                    if update:
                        # create a copy of the original feature 
                        feature_to_update = deepcopy(original_feature)

                        # update the photo_name field in the AGO feature layer with a list of the new photo names 
                        feature_to_update.attributes['photo_name'] = ','.join(photo_name_list)

                        # update the list of photo names 
                        features_for_update.append(feature_to_update)

        # apply edits to the photo_name field in the AGO feature layer
        if features_for_update:
            ago_flayer.edit_features(updates=features_for_update)
        
    def list_contents(self) -> list:
        """
        Get a list of object storage contents

        Returns: list of object storage contents
        """
        obj_bucket = self.boto_resource.Bucket(self.badger_bucket)
        lst_objects = []
        for obj in obj_bucket.objects.all():
            lst_objects.append(os.path.basename(obj.key))

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

                        self.boto_resource.meta.client.upload_file(attach_file, self.badger_bucket, ostore_path)
    
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
                        key: convert_timestamp(key, value, unit='milliseconds') 
                        for key, value in feature.attributes.items()
                        if key != "SHAPE" 
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

        now = datetime.now().strftime("%d-%m-%Y")

        if counter == 1:

            ostore_path = f'backup_data/survey123_raw_backup_data_{now}.geojson'

        else:
            ostore_path = f'backup_data/survey123_edited_backup_data_{now}.geojson'

        bucket_name = self.badger_bucket

        try:
            geojson_data = BytesIO(geojson.encode('utf-8'))
            s3_object = self.boto_resource.Object(bucket_name, ostore_path)
            s3_object.put(
                Body=geojson_data,
                ContentType='application/geo+json'
            )
            print(f"GeoJSON data has been uploaded to s3://{bucket_name}/{ostore_path}")
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == '__main__':
    run_app()
