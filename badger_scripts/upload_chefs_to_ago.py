#-------------------------------------------------------------------------------
# Name:                     Upload CHEFS Data to ArcGIS Online
#
# Purpose:                  This script streamlines the Badger Sightings data processing pipeline by:
#                               (1) Fetches incoming Badger Sightings data from CHEFS.
#                               (2) Matches CHEFS records to photos submitted via Survey123 via the unique_id field. Updates these records' geometry and attributes from CHEFS response.
#                               (3) Identifies new CHEFS records not yet loaded to AGOL. Uploads these records to AGOL. 
#                               (4) Renames AGO attachments. 
#                               (5) Formats and creates an excel report for both all sightings and sightings within Simpcw land.
#                               (6) Uploads excel report to object storage.
#              
# Input(s):                 (1) Object Storage credentials.
#                           (2) AGOL credentials. 
#                           (3) CHEFS form ID and version ID. 
#                           (4) AGOL feature layer item ID.         
#
# Author:                   Emma Armitage - GeoBC
#              
# Modified:                 2025-04-03
#
# CHEFS documentation:      https://submit.digital.gov.bc.ca/app/api/v1/docs#tag/Submission/operation/exportWithFields
#-------------------------------------------------------------------------------

import base64
import urllib3
import json
import pandas as pd
from arcgis import GIS
import logging
import os
import sys
from minio import Minio
from  minio.error import S3Error
from copy import deepcopy

def main():

    # set the logging level & configure message format
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    API_KEY = os.environ['CHEFS_API_KEY'] # api key
    BASE_URL = os.environ['CHEFS_BASE_URL']
    FORM_ID = os.environ['CHEFS_FORM_ID'] # form ID
    # VERSION_ID = os.environ['CHEFS_VERSION_ID'] # form version 12
    VERSION_13 = os.environ['CHEFS_VERSION_ID_13']
    VERSION_12 = os.environ['CHEFS_VERSION_ID_12']
    REQUEST_FIELDS = "confirmationId,createdAt,first_name,last_name,email,sighting_date,sighting_type,sighting_type_other,number_badgers,badger_status,in_conflict,road_location,obs_type,family_at_burrow,location_type,ground_squirrels,additional_info,upload_image,image_permission,unique_id,sighting_location,latitude,longitude,point_accuracy,referral_source,social_media_source,referral_source_other"
    HTTP_POOL_MANAGER = urllib3.PoolManager()

    # input parameters
    AGO_USER = os.environ['AGO_USER']
    AGO_PASS = os.environ['AGO_PASS']
    MAPHUB_URL = os.environ['MAPHUB_URL']
    AGO_ITEM_ID = os.environ['BADGER_SIGHTINGS_AGO_ITEM']
    SIMPCW_ITEM_ID = os.environ['SIMPCW_BADGER_SIGHTINGS_AGO_ITEM']
    USERNAME = os.environ['OBJ_STORE_USER']
    ENDPOINT = os.environ['OBJ_STORE_HOST']
    SECRET = os.environ['OBJ_STORE_API_KEY']
    S3_BUCKET = os.environ['BADGER_S3_BUCKET']

        # current year for naming & querying
    year = pd.to_datetime('today').year

    QUERY = f""" CreationDate >= '{year}-01-01' AND unique_id IS NOT NULL """

    # drop columns from feature layer sdf
    flayer_drop_columns = ['objectid', 'globalid', 'sighting_date', 'survey_start', 'survey_end', 'CreationDate', 'Creator', 'EditDate', 'Editor', 'badger_photo_header', 'Form_Header_1', 'Form_Header_2', 'Form_Description', 'badger_status_note', 'road_mortality_note', 'duplicate_sighting', 'social_media_source_other', 'SHAPE']
    # CHEFS keep columns
    chefs_keep_columns = ['first_name', 'last_name', 'email']

    logging.info('\nConnecting to MapHub')
    gis = connect_to_ago(AGO_USER, AGO_PASS, MAPHUB_URL)

    logging.info('\nConnecting to object storage')
    s3_connection = object_storage_connection(USERNAME, ENDPOINT, SECRET)

    logging.info(f'\nReading CHEFS data for form version {VERSION_12 and VERSION_13} to pandas dataframe')
    chefs_df = chefs_data_api_request(HTTP_POOL_MANAGER, FORM_ID, VERSION_12, VERSION_13, API_KEY, BASE_URL, REQUEST_FIELDS)

    logging.info(f'\nGetting Survey123 data')
    survey123_item, survey123_layer, survey123_properties, survey123_df = get_ago_data(gis, AGO_ITEM_ID, QUERY)

    logging.info(f'\nMerging dataframes')
    master_df = merge_dataframes(chefs_df, survey123_df)

    logging.info('\nUpdating Dataframe values')
    new_photos_update_df, new_chefs_records_update_df = filter_dataframes(master_df)

    if not new_photos_update_df.empty or not new_chefs_records_update_df.empty:
        logging.info(f'\nFormatting data for AGOL')
        ago_records_for_update = format_data_for_ago(df = new_photos_update_df)
        new_records_for_ago = format_data_for_ago(df=new_chefs_records_update_df)

        logging.info(f'\nUpdating data in AGOL')
        edit_ago_data(ago_records_for_update, new_records_for_ago, survey123_layer, survey123_properties)

        logging.info(f'\nGetting updated data from AGOL')
        updated_ago_layer, updated_ago_properties, updated_ago_features, updated_ago_sdf, simpcw_sdf = get_updated_ago_data(gis, AGO_ITEM_ID, SIMPCW_ITEM_ID, QUERY)

        # logging.info(f'\nCleaning AGOL data')
        # logging.info(f'..finding records to remove from AGOL - either blank or duplicate records')
        # remove_ago_duplicates_and_blanks(updated_ago_layer, updated_ago_sdf)

        # logging.info(f'\nGetting updated data from AGOL after removing duplicates and blanks')
        # updated_ago_layer, updated_ago_properties, updated_ago_features, updated_ago_sdf, simpcw_sdf = get_updated_ago_data(gis, AGO_ITEM_ID, SIMPCW_ITEM_ID, QUERY)

        logging.info(f'\nRenaming AGO feature layer attachments')
        rename_attachments(updated_ago_layer, updated_ago_properties, updated_ago_features)

        logging.info(f'\nCreating excel report for the entire {year} badger sightings dataset')
        ago_file_name = "badger_sightings_report"
        ago_ostore_path = "badger_excel_report"
        excel_path = create_excel_report(updated_ago_sdf, chefs_df, flayer_drop_columns, chefs_keep_columns, year, ago_file_name)

        logging.info(f'\nSaving full excel report to object storage')
        save_to_object_storage(S3_BUCKET, ago_ostore_path, excel_path, s3_connection)

        if not simpcw_sdf.empty:
            logging.info(f'\nCreating excel report for Simpcw Nation')
            simpcw_file_name = "simpcw_badger_sightings_report"
            simpcw_ostore_path = r"simpcw_badger_data/simpcw_badger_excel_report"
            simpcw_excel_path = create_excel_report(simpcw_sdf, chefs_df, flayer_drop_columns, chefs_keep_columns, year, simpcw_file_name)

            logging.info(f'\nSaving Simpcw excel report to object storage')
            save_to_object_storage(S3_BUCKET, simpcw_ostore_path, simpcw_excel_path, s3_connection)

        else:
            logging.info(f'\nNo Simpcw data found for {year} - no report will be created')

    else:
        logging.info('\nNo new records to update - exiting script')
        sys.exit(0)


def connect_to_ago(username, password, url):
    """
    Returns an AGOL connection
    """

    gis = GIS(url=url, username=username, password=password)

    if gis.users.me:
        logging.info('..successfully connected to AGOL as {}'.format(gis.users.me.username))
    else:
        logging.error('..connection to AGOL failed')

    return gis

def object_storage_connection(username, endpoint, secret):
    """
    Returns a connect to Amazon S3 object storage
    """
    s3_connection = Minio(endpoint, access_key=username, secret_key=secret)

    if s3_connection:
        logging.info('..successfully connected to S3 object storage')
        return s3_connection
    else:
        logging.error('..failed to connect to object storage')

def chefs_data_api_request(http_pool_manager, form_id, version_12, version_13, api_key, base_url, request_fields):

    form_metadata_url_v12 = f"{base_url}/{form_id}/versions/{version_12}/submissions/discover"
    form_metadata_url_v13 = f"{base_url}/{form_id}/versions/{version_13}/submissions/discover"

    get_form_submission_url = f"{base_url}/{form_id}/submissions"

    # Encode username and password for basic authentication
    hash_string = base64.b64encode(f"{form_id}:{api_key}".encode()).decode()

    # Set headers for authorization
    headers = {"Authorization": f"Basic {hash_string}"}

    fields = {"fields": request_fields}

    # make the request
    response_metadata_v12 = http_pool_manager.request("GET", form_metadata_url_v12, fields=fields, headers=headers)
    response_metadata_v13 = http_pool_manager.request("GET", form_metadata_url_v13, fields=fields, headers=headers)
    response_submissions = http_pool_manager.request("GET", get_form_submission_url, headers=headers)

    # response status
    if response_submissions.status == 200:
        logging.info('..successfully retrieved CHEFS data')
    else:
        logging.error(f'..error retrieving CHEFS data: {response_submissions.status}')

    # convert response to json
    response_data_decode_v12 = json.loads(response_metadata_v12.data.decode("utf-8"))
    response_data_decode_v13 = json.loads(response_metadata_v13.data.decode("utf-8"))
    response_submission_data = json.loads(response_submissions.data.decode("utf-8"))

    # convert response to pandas dataframe
    response_data_df_v12 = pd.DataFrame(response_data_decode_v12)
    response_data_df_v13 = pd.DataFrame(response_data_decode_v13)
    response_submission_df = pd.DataFrame(response_submission_data)

    # concat the two versions into one dataframe
    response_data_df = pd.concat([response_data_df_v12, response_data_df_v13], axis=0, ignore_index=True)

    # merge dataframes to get the ConfirmationID and the CreatedAt fields
    merged_df = pd.merge(response_submission_df, response_data_df, left_on='submissionId', right_on='id')

    logging.info('..converting to pandas dataframe')
    # drop unnecessary fields
    drop_fields = ["formId", "formSubmissionStatusCode", "submissionId", "deleted", "createdBy", "formVersionId", "lateEntry"]
    chefs_df = merged_df.drop(columns=drop_fields)

    chefs_df['createdAt'] = pd.to_datetime(chefs_df['createdAt']).dt.tz_convert('UTC')

    logging.info(f'..CHEFS dataframe head: \n{chefs_df.head}')

    return chefs_df

def get_ago_data(gis, ago_item_id, query):
    """
    Gets data from AGO
    """

    survey123_item = gis.content.get(ago_item_id)
    survey123_layer = survey123_item.layers[0]
    survey123_properties = survey123_layer.query(where=query)
    survey123_sdf = survey123_properties.sdf

    if not survey123_sdf.empty:
        logging.info(f'..the query returned {len(survey123_sdf)} features')
    else:
        logging.error('..could not retrieve data from AGO')

    return survey123_item, survey123_layer, survey123_properties, survey123_sdf

def merge_dataframes(chefs_df, survey123_df):
    """
    Joins the survey123 dataframe to the CHEFS dataframe on the unique_id field
    """
    master_df = pd.merge(left=chefs_df,
                         right=survey123_df,
                         how='left',
                         on='unique_id')
    
    if not master_df.empty:
        logging.info('..successfully merged the dataframes')
    else:
        logging.error('..Failed to merge dataframes')

    return master_df

def update_dataframe(filtered_df, photos):
    """
    Updates the survey123 columns in the master dataframe with the values from CHEFS
    """
    filtered_df = filtered_df.copy()  # Prevent SettingWithCopyWarning

    # convert the chefs data format to be compatible for AGOL (from dictionary to comma seperated values)
    filtered_df['obs_type_x'] = filtered_df['obs_type_x'].apply(
        lambda x: ", ".join([key.title() for key, value in x.items() if value])
        )

    logging.info(f'....updating survey123 fields from chefs fields in the master dataframe')
    for column, data in filtered_df.items():
        if "x" in column:
            corresponding_y_column = column.replace('x', 'y')
            filtered_df.loc[:, corresponding_y_column] = filtered_df[column]

        # update the survey_start field with the createdAt date if it's empty
        if column == "survey_start":
            filtered_df.loc[filtered_df['survey_start'].isna() | (filtered_df['survey_start'] == ""), 'survey_start'] = filtered_df['createdAt']

        if photos and column == "SHAPE":
            filtered_df.loc[:, 'SHAPE'] = filtered_df.apply(lambda row: {**row['SHAPE'], 'x': row['longitude_y'], 'y': row['latitude_y']}, axis=1)

    return filtered_df


def filter_dataframes(master_df):
    """
    Filters for new submissions from CHEFS
    """

    logging.info('..finding new records')
    # new records with photos from survey123
    new_photos_df = master_df[pd.notna(master_df['photo_name']) & pd.isna(master_df['sighting_type_y'])]
    # new records from CHEFS
    new_chefs_records_df = master_df[pd.isna(master_df['photo_name']) & pd.isna(master_df['sighting_type_y'])] 

    # update survey123 fields with CHEFS data
    if not new_photos_df.empty and not new_chefs_records_df.empty:
        logging.info(f'....{len(new_photos_df)} new records with photos found')
        new_photos_update_df = update_dataframe(filtered_df=new_photos_df, photos=True)

        logging.info(f'....{len(new_chefs_records_df)} new CHEFS records without photos found')
        new_chefs_records_update_df = update_dataframe(filtered_df=new_chefs_records_df, photos=False)   

    elif not new_photos_df.empty:
        logging.info(f'....{len(new_photos_df)} new records with photos found')
        new_photos_update_df = update_dataframe(filtered_df=new_photos_df, photos=True)

    elif not new_chefs_records_df.empty:
        logging.info(f'....{len(new_chefs_records_df)} new CHEFS records without photos found')
        new_chefs_records_update_df = update_dataframe(filtered_df=new_chefs_records_df, photos=False)

    else:
        logging.info('....no new records with photos found')

    # create empty dataframes if no new records were found
    if new_photos_df.empty:
        new_photos_update_df = pd.DataFrame()
    if new_chefs_records_df.empty:
        new_chefs_records_update_df = pd.DataFrame()

    return new_photos_update_df, new_chefs_records_update_df

def format_data_for_ago(df):
    """
    Formats CHEFS data to be compatible with AGOL feature layer
    """
    # change some coded values to be consistent with AGOL
    map_dict = {
        'sighting_type_y': {
            'badger': 'Badger',
            'badger_family': 'Badger Family',
            'other': 'other'
        },
        'badger_status_y': {
            'alive': 'Alive',
            'dead': 'Dead',
            'not_sure': 'Not Sure'
        },
        'in_conflict_y': {
            'yes': 'Yes',
            'no': 'No'
        },
        'road_location_y': {
            'badger_road_crossing': 'Badger road crossing',
            'badger_road_mortality': 'Badger road mortality',
            'other': 'Other'
        },
        'location_type_y': {
            'public_land_or_park': 'Public Land or Park',
            'private_property': 'Private Property',
            'highway_or_road': 'Highway or Road',
            'other': 'Other'
        },
        'ground_squirrels_y': {
            'none': 'None',
            'few_less_than_10': 'Few (<10)',
            'many_10_to_20': 'Many (10-20)',
            'abundant_over_20': 'Abundant (>20)',
            'unsure': 'Unsure'
        },
        'point_accuracy_y': {
            '100m_exact': '<100m (Exactly the spot)',
            '1km_almost': '<1km (Almost the spot)',
            '10km_general_area': '<10km (In the general area)'
        },
        'image_permission_y': {
            'yes': 'Yes',
            'no': 'No'
        }
    }

    # convert CHEFS date to datetime format
    if not df.empty:
        logging.info('....formatting CHEFS data for AGOL')
        # convert to datetime format
        df['sighting_date_y'] = pd.to_datetime(df['sighting_date_y'], errors='coerce', utc=True)

        for column, data in df.items():
            if column in map_dict.keys():
                # find data value in map_dict keys for the given column. Update the column with the mapped value
                df[column] = df[column].map(map_dict[column])

            # Convert blank image_permission values to None
            if column == "image_permission_y":
                df[column] = df[column].apply(lambda x: None if pd.isna(x) or x == "" else x)

    else:
        logging.info('....no new records to update')
        # create empty dataframe
        df = pd.DataFrame()
    
    return df

# edit data in AGO
def edit_ago_data(ago_records_for_update, new_records_for_ago, survey123_layer, survey123_properties):
    """
    Edits data in AGOL with data from CHEFS

    survey123_properties: feature layer queried to only get features with photos
    """

    # list of features to edit
    features_to_edit = []

    # list of features to add
    features_to_add = []

    # get the features from survey123_properties
    original_features = survey123_properties.features

    # Extract existing unique_ids from AGOL
    existing_unique_ids = {feature.attributes['unique_id'] for feature in original_features}

    if not ago_records_for_update.empty:
        # iterate through the features
        for feature in original_features:

            # get the unique_id from the feature
            unique_id = feature.attributes['unique_id']

            # get the corresponding record from ago_records_for_update
            df_record = ago_records_for_update[ago_records_for_update['unique_id'] == unique_id]

            # update the feature's geometry and attributes
            if not df_record.empty:
                for _, row in df_record.iterrows():

                    updated_attributes = feature.attributes.copy()

                    updated_attributes.update({
                        column.replace('_y', ''): row[column] for column in df_record.columns if column.endswith("_y")
                    })

                    updated_feature = {
                        "attributes" : updated_attributes,
                        "geometry": {
                            "x": row['longitude_y'],
                            "y": row['latitude_y']
                        }   
                    }

                # update sighting_date_response
                updated_feature['attributes']['sighting_date_response'] = row['sighting_date_y'].strftime('%Y-%m-%d')
                updated_feature['attributes']['sighting_date'] = row['sighting_date_y'].strftime('%Y-%m-%d')

                # update CHEFS confirmation ID
                updated_feature['attributes']['chefs_confirmation_id'] = row.get('confirmationId')

                # add the updated feature to the list
                features_to_edit.append(updated_feature)

        # if there are features to edit, update the feature layer
        if features_to_edit:
            logging.info(f'..updating {len(features_to_edit)} features in AGOL')
            try: 
                edit_response = survey123_layer.edit_features(updates=features_to_edit)
                logging.info(f'....AGOL edit response: {edit_response}')
            except Exception as e:
                logging.error(f'....error updating AGOL: {e}')

    # append only new CHEFS records to the feature layer
    if not new_records_for_ago.empty:

        # filter out records with unique_ids that already exist in AGOL
        new_records_for_ago = new_records_for_ago[~new_records_for_ago['unique_id'].isin(existing_unique_ids)]

        # select only the columns that end with "_y"
        y_columns = [col for col in new_records_for_ago.columns if col.endswith('_y') or col in ['sighting_date_response', 'unique_id', 'chefs_confirmation_id', 'survey_start_date_time', 'confirmationId', 'survey_start']]
        attributes_df = new_records_for_ago[y_columns]

        # rename the columns to remove the "_y" suffix
        attributes_df.columns = [col.replace('_y', '') for col in attributes_df.columns]

        # create a dictionary for the new feature
        for _, row in attributes_df.iterrows():
            new_feature = {
                "attributes": row.to_dict(),
                "geometry": {
                    "x": row['longitude'],
                    "y": row['latitude']
                }
            }

            # Handle 'sighting_date' if it exists
            sighting_date = row.get('sighting_date')
            if pd.notna(sighting_date):
                new_feature['attributes']['sighting_date_response'] = sighting_date.strftime('%Y-%m-%d')
                new_feature['attributes']['sighting_date'] = sighting_date.strftime('%Y-%m-%d')

            # Handle 'unique_id' if it exists
            unique_id = row.get('unique_id')
            if pd.notna(unique_id):
                new_feature['attributes']['unique_id'] = unique_id

            # Handle 'chefs_confirmation_id' if it exists
            chefs_confirmation_id = row.get('confirmationId')
            if pd.notna(chefs_confirmation_id):
                new_feature['attributes']['chefs_confirmation_id'] = chefs_confirmation_id

            # remove columns that are not needed for AGOL
            new_feature['attributes'].pop('confirmationId', "Key not found")
            new_feature['attributes'].pop('survey_start', 'Key not found')

            # add the new feature to the list
            features_to_add.append(new_feature)

            print(new_feature)

        # if there are features to add, append them to the AGOL feature layer
        if features_to_add:
            logging.info(f'..adding {len(features_to_add)} new feature(s) to AGOL')
            try:
                add_response = survey123_layer.edit_features(adds=features_to_add)
                try:
                    if all(res.get('success') for res in add_response.get('addResults', [])):
                        logging.info(f"..{len(features_to_add)} features added successfully.")
                    else:
                        logging.error("..some features failed to add.")
                        logging.error(f"..full result: {add_response}")
                except Exception as e:
                    logging.exception(f"..unexpected error: {e}")                
            except Exception as e:
                logging.error(f'....error adding features to AGOL: {e}')

def get_updated_ago_data(gis, ago_item_id, simpcw_item_id, query):
    """
    Gets updated data from AGOL
    """

    # get the feature layer from AGOL
    updated_ago_item = gis.content.get(ago_item_id)
    updated_ago_layer = updated_ago_item.layers[0]
    updated_ago_properties = updated_ago_layer.query(where=query)
    updated_ago_sdf = updated_ago_properties.sdf
    updated_ago_features = updated_ago_properties.features

    # get the Simpcw feature layer from AGOL
    simpcw_item = gis.content.get(simpcw_item_id)
    simpcw_layer = simpcw_item.layers[0]
    simpcw_properties = simpcw_layer.query(where=query)
    simpcw_sdf = simpcw_properties.sdf

    if not updated_ago_sdf.empty:
        logging.info(f'..the query returned {len(updated_ago_sdf)} features')
    else:
        logging.error('..could not retrieve data from AGO')

    if not simpcw_sdf.empty:
        logging.info(f'..the query returned {len(simpcw_sdf)} features for Simcpw')
    else:
        logging.error('..could not retrieve Simpcw data from AGO')

    return updated_ago_layer, updated_ago_properties, updated_ago_features, updated_ago_sdf, simpcw_sdf

def remove_ago_duplicates_and_blanks(updated_ago_layer, updated_ago_sdf):
    """
    Removes any duplicate records from the AGOL feature layer
    """
    # list to hold duplicates
    remove_oids = []

    # filter dataframe for duplicate records and blank records
    duplicate_records = updated_ago_sdf[updated_ago_sdf.duplicated(subset=['unique_id'])]
    blank_records = updated_ago_sdf.loc[(updated_ago_sdf['unique_id'].notna()) & (updated_ago_sdf['chefs_confirmation_id'].isna())]

    # merge the two dataframes
    records_to_remove = pd.concat([duplicate_records, blank_records], ignore_index=True)

    if not records_to_remove.empty:
        logging.info(f'..found {len(records_to_remove)} records to remove from AGOL')

        # iterate through duplicates and extract their unique_ids
        for _, row in records_to_remove.iterrows():
            # get the unique_id from the row
            remove_oid = row['objectid']

            # append the unique_id to the list
            remove_oids.append(remove_oid)

        logging.info(f'..removing {len(remove_oids)} duplicate records from AGOL')
        delete_result = updated_ago_layer.edit_features(deletes=str(remove_oids))

        if all(res.get('success') for res in delete_result.get('deleteResults', [])):
            logging.info(f'....{len(remove_oids)} duplicate records removed successfully')
        else:
            logging.error('....some duplicate records failed to delete')
            logging.error(f'....full result: {delete_result}')

    else:
        logging.info('..no records to remove found')

def clean_filename(filename: str) -> str:
    """
    Removes any invalid characters from date fields in AGO feature layer data 
    The date field is used to construct the new attachment name

    Returns: date w/o invalid characters
    """    

    # define invalid path characteres
    invalid_chars = '<>:"/\\|?*'

    # clean the file name
    if filename is not None:
        clean_filename = ''.join('-' if c in invalid_chars else c for c in filename).rstrip('. ')

    return clean_filename

def download_attachment(ago_flayer, oid, attachment_id):
    """
    Downloads attachments on an AGO feature layer into GitHub Actions temporary directory

    Returns: downloaded file
    """
    # download the file 
    file = ago_flayer.attachments.download(oid=oid, attachment_id=attachment_id)[0]

    if not file:
        raise ValueError("Failed to download the attachment")
    
    return file

def rename_file(file_path: str, new_name: str) -> str:
    """
    Renames attachments

    Returns: file path to renamed attachment
    """
    new_path = os.path.join(os.path.dirname(file_path), new_name)

    os.rename(file_path, new_path)

    return new_path


# rename attachments
def rename_attachments(ago_flayer, flayer_properties, flayer_data):
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
            clean_sighting_date = clean_filename(filename=sighting_date)

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
                    file = download_attachment(ago_flayer=ago_flayer, 
                                                oid=oid, 
                                                attachment_id=attach_id)

                    # new attach file path
                    new_attach_file = rename_file(file_path=file, 
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
        logging.info(f'..updating {len(features_for_update)} feature attachment names')
        ago_flayer.edit_features(updates=features_for_update)

def create_excel_report(ago_sdf, chefs_df, flayer_drop_columns, chefs_keep_columns, file_name, year):
    """
    Clean the dataframe and create excel report
    """
    # drop columns from CHEFS df
    chefs_drop_columns = []

    # iterate through flayer columns, if they contain "response" add them to the drop list
    for column in ago_sdf.columns:
        if column.endswith("response") and column != "photo_response" and column != "sighting_date_response":
            flayer_drop_columns.append(column)
        if "review" in column:
            flayer_drop_columns.append(column)

    # iterate through CHEFS columns, add to drop list if they are not in the keep list
    for column in chefs_df.columns:
        if column not in chefs_keep_columns and column != "unique_id":
            chefs_drop_columns.append(column)

    # drop columns from AGOL feature layer
    logging.info('..dropping unnecessary columns from the dataframes')
    ago_sdf_clean = ago_sdf.drop(columns=flayer_drop_columns, errors='ignore')

    # drop columns from CHEFS df (only keep firstName, lastName, email)
    chefs_df_clean = chefs_df.drop(columns=chefs_drop_columns, errors='ignore')

    # convert unique_id to string
    chefs_df_clean['unique_id'] = chefs_df_clean['unique_id'].astype(str)
    ago_sdf_clean['unique_id'] = ago_sdf_clean['unique_id'].astype(str)

    # join dataframes - need to use pd concat or deal with datatypes
    logging.info('..joining AGOL and CHEFS dataframes')
    excel_df = pd.concat([chefs_df_clean, ago_sdf_clean], axis=1)

    # drop extra unique ID column
    excel_df = excel_df.loc[:, ~excel_df.columns.duplicated()].copy()

    # rename columns
    new_column_names = {
        "unique_id": "Unique ID",
        "chefs_confirmation_id": "CHEFS Confirmation ID",
        "first_name": "First Name",
        "last_name": "Last Name",
        "email": "Email",
        "sighting_date_response": "Sighting Date",
        "sighting_type": "Sighting Type",
        "sighting_type_other": "Sighting Type Other",
        "number_badgers": "How Many Badgers Did You See?",
        "badger_status": "Was the badger alive or dead?",
        "in_conflict": "Are you reporting a badger in conflict where public safety is at risk?",
        "road_location": "Are you reporting the location of:",
        "obs_type": "Types of Observations",
        "family_at_burrow": "Badger Location Type:",
        "location_type": "If you are reporting a badger family at a burrow, how many years have you seen them at this location?",
        "ground_squirrels": "Are there ground squirrels in this area?",
        "additional_info": "Describe the Badger Sighting:",
        "photo_name": "Photo Name(s)",
        "image_permission": "Would you like to give BC Badgers permission to use your photo(s) for program materials and this website?",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "point_accuracy": "How accurate is the location on the map above?",
        "referral_source": "How did you hear about the provincial Report a Badger Sightings program?",
        "social_media_source": "Specify Social Media:",
        "referral_source_other": "Specify Other:",
    }

    # reorder columns
    column_order = []
    for column in new_column_names.keys():
        if column in excel_df.columns:
            column_order.append(column)

    logging.info('..reordering columns')
    excel_df = excel_df[column_order]
    logging.info('..renaming columns')
    excel_df.rename(columns=new_column_names, inplace=True)

    # write to Excel
    logging.info('..writing dataframe to excel file')
    excel_path = os.path.join(os.getcwd(), f'{file_name}_{year}.xlsx')

    try:
        excel_df.to_excel(excel_path, index=False)
        logging.info(f'....succesfully wrote dataframe to excel file: {excel_path}')
    except Exception as e:
        logging.error(f'....error writing dataframe to excel file: {e}')

    return excel_path

def save_to_object_storage(s3_bucket, ostore_path, excel_path, s3_connection):
    """
    Saves the excel report to Amazon S3 object storage
    """
    try:
        # check that the file exists before uploading
        if not os.path.exists(excel_path):
            logging.error(f"..file {excel_path} not found. Cannot upload.")
            return
        
        file_name = os.path.basename(excel_path)
        full_path = f"{ostore_path}/{file_name}"
        print(full_path)
        
        s3_connection.fput_object(s3_bucket, full_path, excel_path)
        logging.info(f'..file {os.path.basename(excel_path)} uploaded successfully to {s3_bucket}/{full_path}')
    
    except S3Error as e:
        logging.error(f"..error uploading file {os.path.basename(excel_path)} to object storage: {e}")

    except Exception as e:
        logging.error(f"..unexpected error uploading file {os.path.basename(excel_path)}: {e}")

if __name__ == "__main__":
    main()
