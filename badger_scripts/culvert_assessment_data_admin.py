from arcgis import GIS
import logging
from copy import deepcopy
import pandas as pd
import os

def run_app():

    USERNAME = os.getenv('AGO_USER')
    PASSWORD = os.getenv('AGO_PASS')
    MAPHUB_URL = os.getenv('MAPHUB_URL')

    CULVERT_ITEM_ID = os.getenv('BADGER_CULVERT_ITEM_ID')       


    logging.basicConfig(level=logging.INFO, format='%(message)s')

    logging.info("Connecting to AGO")
    gis = connect_to_ago(url=MAPHUB_URL, username=USERNAME, password=PASSWORD)

    logging.info("Retrieving feature layer data")
    culvert_loc_flayer, culvert_loc_properties, culvert_loc_features, culvert_assessment_tbl, culv_assess_properties, culv_assess_features = get_ago_layers(gis=gis, ago_item_id=CULVERT_ITEM_ID)

    logging.info("Updating feature information")
    update_ago_data(culvert_loc_flayer=culvert_loc_flayer, culvert_loc_properties=culvert_loc_properties, field_to_update='MACHINE_EXCAV_REQ', value_field='MACHINE_EXCAV_REQ')
    update_ago_data(culvert_loc_flayer=culvert_loc_flayer, culvert_loc_properties=culvert_loc_properties, field_to_update='UNDERPASS_PRIORITY', value_field='UNDERPASS_PRIORITY')
    update_ago_data(culvert_loc_flayer=culvert_loc_flayer, culvert_loc_properties=culvert_loc_properties, field_to_update='LANDSCAPE_CONNECT', value_field='LANDSCAPE_CONNECT')

    logging.info("Renaming culvert location photos")
    rename_culvert_loc_attachments(ago_flayer=culvert_loc_flayer, flayer_properties=culvert_loc_properties, flayer_data=culvert_loc_features)

    logging.info("Renaming culvert assessment photos")
    rename_culvert_assess_attachments(tbl_cubby_check=culvert_assessment_tbl, check_properties=culv_assess_properties, check_data=culv_assess_features)

    logging.info("Updating Photo Name field")

def connect_to_ago(url, username, password):
    """ Returns and ArcGIS Online Connection """

    gis = GIS(url=url, username=username, password=password)
    logging.info(f"..successfully connected to ago as {gis.users.me.username}")

    return gis

def get_ago_layers(gis, ago_item_id):
    """ Returns AGO layers and data """

    ago_item = gis.content.get(ago_item_id)

    # culvert location feature layer
    culvert_loc_flayer = ago_item.layers[0]
    culvert_loc_properties = culvert_loc_flayer.query()
    culvert_loc_features = culvert_loc_properties.features

    # culvert assessment feature layer
    culvert_assessment_tbl = ago_item.tables[0]
    culv_assess_properties = culvert_assessment_tbl.query()
    culv_assess_features = culv_assess_properties.features

    logging.info("..successfully retrieved feature layer and table data")

    return culvert_loc_flayer, culvert_loc_properties, culvert_loc_features, culvert_assessment_tbl, culv_assess_properties, culv_assess_features


def update_ago_data(culvert_loc_flayer, culvert_loc_properties, field_to_update, value_field):
    """ Updates the point feature layer with values from the related table """

    logging.info(f"..checking for changes to {value_field} field")

    # convert culvert location to sdf
    culvert_loc_sdf = culvert_loc_properties.sdf

    features_for_update = []

    # query related records and convert them to a pandas dataframe
    for index, loc in culvert_loc_sdf.iterrows():

        oid = loc['OBJECTID']
        loc_value = loc[field_to_update]

        related_records_dict = culvert_loc_flayer.query_related_records(object_ids=oid, relationship_id='0')
        related_record_groups = related_records_dict['relatedRecordGroups']

        related_data = []

        for group in related_record_groups:
            for record in group.get('relatedRecords', []):
                related_data.append(record.get('attributes', {}))

        related_data_df = pd.DataFrame(related_data)

        # find retrieve the field value from the related record if one exists
        if related_data_df.empty:
            continue

        else:
            # get the most recent culvert assessment
            sorted_assessments = related_data_df.sort_values(by='DATE_ASSESSED', ascending=False)
            latest_assessment = sorted_assessments.iloc[0]
            assessment_value = latest_assessment[value_field]

            # compare culvert assessment field value to culvert location field value. Update if different
            if pd.isna(loc_value) or loc_value != assessment_value:
                original_feature = [f for f in culvert_loc_properties if f.attributes['OBJECTID'] == oid][0]
                feature_to_update = deepcopy(original_feature)
                feature_to_update.attributes[field_to_update] = assessment_value
                features_for_update.append(feature_to_update)

    # update culvert location features in AGO if there are differences
    if features_for_update: 
        logging.info(f"..updating {len(features_for_update)} culvert locations' {field_to_update} attribute value")
        try:
            # try to update the features in AGOL
            culvert_loc_flayer.edit_features(updates=features_for_update)

        except Exception as e:
            logging.error(f"Failed to update the culvert locations' status: {e}")
            logging.info("No changes made to cubby locations.")

def rename_photos(oid_list, layer, flayer_properties, get_id):
    """ Renames photos taken in Field Maps """
    features_for_update = []

    for oid in oid_list:
        # get attributes of the feature associated with the attachment
        original_feature = [f for f in flayer_properties if f.attributes['OBJECTID'] == oid][0]

        # get the SITE_ID or SITE_CHECK_ID
        feature_id = get_id(original_feature)

        # initialize attachment counter
        attachment_counter = 1

        # query for attachments
        attachments_list = layer.attachments.get_list(oid=oid)

        if attachments_list:
            # check current name
            for attachment in attachments_list:
                current_attach_name = attachment['name']
                current_attach_id = attachment['id']

                update = False

                if current_attach_name.startswith(feature_id):
                    continue

                else:
                    # new photo name list
                    photo_names = []

                    # get the file type (ex: png, jpg, jpeg)
                    file_type = current_attach_name.split('.')[-1]

                    # create the new attachment name
                    attachment_name = f"{feature_id}_photo_{attachment_counter}.{file_type}"

                    # increment the attachment counter by 1
                    attachment_counter += 1

                    # download the file
                    file = download_attachment(ago_flayer=layer,
                                               oid=oid,
                                               attachment_id=current_attach_id)

                    # new attach file path
                    new_attach_file = rename_file(file_path=file,
                                                  new_name=attachment_name)

                    # download attachments
                    try:
                        layer.attachments.update(oid=oid,
                                                 attachment_id=current_attach_id,
                                                 file_path=new_attach_file)

                    except:
                        layer.attachments.add(oid=oid, file_path=new_attach_file)
                        layer.attachments.delete(oid=oid, attachment_id=current_attach_id)

                    # set the update variable to True
                    update = True

                    # add photo name to list
                    photo_names.append(attachment_name)

                if update:
                    # create a copy of the original feature
                    feature_to_update = deepcopy(original_feature)

                    # update the PHOTO_NAME field
                    feature_to_update.attributes['PHOTO_NAME'] = ",".join(photo_names)

                    # update the list of photo names
                    features_for_update.append(feature_to_update)

    # apply edits to the photo_name field in the AGO feature layer
    if features_for_update:
        layer.edit_features(updates=features_for_update)

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

def rename_culvert_loc_attachments(ago_flayer, flayer_properties, flayer_data):
    rename_photos(
        oid_list=flayer_properties.sdf['OBJECTID'].tolist(),
        layer=ago_flayer,
        flayer_properties=flayer_data,
        get_id=lambda feature: feature.attributes['SITE_ID'],
    )
    
def rename_culvert_assess_attachments(tbl_cubby_check, check_properties, check_data):
    rename_photos(
        oid_list=pd.DataFrame.spatial.from_layer(tbl_cubby_check)['OBJECTID'].tolist(),
        layer=tbl_cubby_check,
        flayer_properties=check_data,
        get_id=lambda feature: feature.attributes['SITE_ASSESS_ID'],
    )
    
if __name__ == "__main__":
    run_app()

