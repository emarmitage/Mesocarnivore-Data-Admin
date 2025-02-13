# Purpose: Rename photos collected from Field Maps. Update status in point feature layer with status from the most recent related record
# Author: Emma Armitage
# Last edit date: 2025-02-13

from arcgis.gis import GIS
import logging
import os
from copy import deepcopy
import pandas as pd

def run_app():

    # set logging level
    logging.basicConfig(level=logging.INFO)

    USERNAME = os.environ['AGO_USER']
    PASSWORD = os.environ['AGO_PASS']
    HOST = os.environ['HOST_URL']
    LAYER_ID = os.environ['HAIR_SNAG_ID']

    gis = connect_to_ago(HOST=HOST, USERNAME=USERNAME, PASSWORD=PASSWORD)
    ago_flayer, flayer_properties, flayer_data, tbl_cubby_check, cubby_check_query, cubby_check_data = get_feature_layer_data(ago_layer_id=LAYER_ID, gis=gis)
    update_cubby_status(ago_flayer=ago_flayer, flayer_data=flayer_data, tbl_cubby_check=tbl_cubby_check)
    cubby_check_complete(ago_flayer=ago_flayer, flayer_data=flayer_data, tbl_cubby_check=tbl_cubby_check)
    rename_cubby_loc_attachments(ago_flayer=ago_flayer, flayer_properties=flayer_properties, flayer_data=flayer_data)
    rename_cubby_check_attachments(tbl_cubby_check=tbl_cubby_check, check_properties=cubby_check_query, check_data=cubby_check_data)

def connect_to_ago(HOST, USERNAME, PASSWORD):
    """
    Connects to AGOL
    """
    gis = GIS(HOST, USERNAME, PASSWORD)

    if gis.users.me:
        logging.info(f'..successfully connect to AGOL as {gis.users.me.username}')
    else:
        logging.error('..connection to AGOL failed')

    return gis 

def get_feature_layer_data(ago_layer_id, gis):
    ago_item = gis.content.get(ago_layer_id)

    ago_flayer = ago_item.layers[0]
    flayer_properties = ago_flayer.query()
    flayer_data = flayer_properties.features

    tbl_cubby_check = ago_item.tables[0]
    cubby_check_query = tbl_cubby_check.query()
    cubby_check_data = cubby_check_query.features


    return ago_flayer, flayer_properties, flayer_data, tbl_cubby_check, cubby_check_query, cubby_check_data

def update_cubby_status(ago_flayer, flayer_data, tbl_cubby_check):
    logging.info('Updating cubby location with most recent cubby check status')

    # list containing corrected features
    features_for_update = []

    for cubby in flayer_data:

        # get the cubby location unique id
        site_id = cubby.attributes['SITE_ID']

        # get the cubby location status
        cubby_loc_status = cubby.attributes['SITE_STATUS']

        # query cubby checks associated with SITE_ID
        cubby_check_subset = tbl_cubby_check.query(where=f"SITE_ID = \'{site_id}\'")

        # if there are no cubby checks, skip that feature
        if len(cubby_check_subset) == 0:
            continue

        sorted_checks = cubby_check_subset.sdf.sort_values(by='START_DATE', ascending=False)

        latest_check = sorted_checks.iloc[0]

        check_status = latest_check['SITE_STATUS']

        if cubby_loc_status != check_status:
            original_feature = [f for f in flayer_data if f.attributes['SITE_ID'] == site_id][0]
            feature_to_update = deepcopy(original_feature)
            feature_to_update.attributes['SITE_STATUS'] = check_status
            features_for_update.append(feature_to_update)

    if features_for_update:
        logging.info(f"Updating {len(features_for_update)} cubby locations' status")
        ago_flayer.edit_features(updates=features_for_update)

def cubby_check_complete(ago_flayer, flayer_data, tbl_cubby_check):
    logging.info('Updating cubby location as complete if cubby check complete')

    # list containing corrected features
    features_for_update = []

    for cubby in flayer_data:

        # get the cubby location unique id
        site_id = cubby.attributes['SITE_ID']

        # get the cubby location completion status
        cubby_loc_complete = cubby.attributes['CHECK_COMPLETE']

        # query cubby checks associated with SITE_ID
        cubby_subset = tbl_cubby_check.query(where=f"SITE_ID = \'{site_id}\'")

        # if there are no cubby checks, skip that feature
        if len(cubby_subset) == 0:
            continue

        sorted_checks = cubby_subset.sdf.sort_values(by='START_DATE', ascending=False)

        latest_check = sorted_checks.iloc[0]

        check_complete = latest_check['CHECK_COMPLETE']

        # if the completion statuses, differ, update the cubby location with the most recent check completion status
        if cubby_loc_complete != check_complete:
            original_feature = [f for f in flayer_data if f.attributes['SITE_ID'] == site_id][0]
            feature_to_update = deepcopy(original_feature)
            feature_to_update.attributes['CHECK_COMPLETE'] = check_complete
            features_for_update.append(feature_to_update)

    # update the feature layer if there are edits
    if features_for_update:
        logging.info(f"Updating {len(features_for_update)} cubby locations' completion status")
        ago_flayer.edit_features(updates=features_for_update)

def rename_cubby_loc_attachments(ago_flayer, flayer_properties, flayer_data):
    rename_attachments(
        oid_list=flayer_properties.sdf['OBJECTID'].tolist(),
        layer=ago_flayer,
        flayer_data=flayer_data,
        get_id=lambda feature: feature.attributes['SITE_ID'],
    )
    
def rename_cubby_check_attachments(tbl_cubby_check, check_properties, check_data):
    rename_attachments(
        oid_list=pd.DataFrame.spatial.from_layer(tbl_cubby_check)['OBJECTID'].tolist(),
        layer=tbl_cubby_check,
        flayer_data=check_data,
        get_id=lambda feature: feature.attributes['SITE_CHECK_ID'],
    )

def download_attachment(ago_flayer, oid, attachment_id):

    # download the file
    file = ago_flayer.attachments.download(oid=oid, attachment_id=attachment_id)[0]

    if not file:
        logging.error('..Failed to download attachment')

    return file

def rename_file(file_path: str, new_name: str):
    new_path = os.path.join(os.path.dirname(file_path), new_name)

    os.rename(file_path, new_path)

    return new_path

def rename_attachments(oid_list, layer, flayer_data, get_id):
    logging.info("Renaming attachments")
    features_for_update = [] 

    for oid in oid_list:

        # for each oid, get a list of its attachments
        attachments_list = layer.attachments.get_list(oid=oid)

        # if the feature has attachments
        if attachments_list:

            # get attributes of the feature associated with the attachment
            original_feature = [f for f in flayer_data if f.attributes['OBJECTID'] == oid][0]

            # get the SITE_ID or SITE_CHECK_ID
            feature_id = get_id(original_feature)

            # initialize attachment counter
            attachment_counter = 1

            for attachment in attachments_list:

                # get the current attachment name
                current_attach_name = attachment['name']

                # get the attachment id
                attach_id = attachment['id']

                # variable indicating if the feature should be updated
                update = False

                # skip photos that have already been renamed
                if current_attach_name.startswith(f"{feature_id}"):
                    continue
                
                # otherwise rename the file
                else:
                    # get the file type (ex: png, jpg, jpeg)
                    file_type = current_attach_name.split('.')[-1]

                    # create the new attachment name
                    attachment_name = f"{feature_id}_photo_{attachment_counter}.{file_type}"

                    # increment the attachment counter by 1
                    attachment_counter += 1

                    # download the file
                    file = download_attachment(ago_flayer=layer,
                                               oid=oid,
                                               attachment_id=attach_id)

                    # new attach file path
                    new_attach_file = rename_file(file_path=file,
                                                  new_name=attachment_name)

                    # download attachments
                    try:
                        layer.attachments.update(oid=oid,
                                                      attachment_id = attach_id,
                                                      file_path = new_attach_file)

                    except:
                        layer.attachments.add(oid=oid, file_path=new_attach_file)
                        layer.attachments.delete(oid=oid, attachment_id=attach_id)      

                    # set the update variable to True
                    update = True                                     

                if update:
                    # create a copy of the original feature 
                    feature_to_update = deepcopy(original_feature)

                    # update the list of photo names 
                    features_for_update.append(feature_to_update)

    # apply edits to the photo_name field in the AGO feature layer
    if features_for_update:
        layer.edit_features(updates=features_for_update)
        
if __name__ == '__main__':
    run_app()
        
if __name__ == '__main__':
    run_app()
