from arcgis import GIS
import logging
from copy import deepcopy
import os
from datetime import datetime, date

def run_app():

    try: 
        USERNAME = os.environ.get('AGO_USERNAME')
        PASSWORD = os.environ.get('AGO_PASSWORD')
        URL = os.environ.get('MAPHUB_URL')
        CAMERA_CHECK_ITEM_ID = os.environ.get('BADGER_CAM_CHECK_ID')

    except:
        USERNAME = os.environ['AGO_USER']
        PASSWORD = os.environ['AGO_PASS']
        URL = os.environ['MAPHUB_URL']
        CAMERA_CHECK_ITEM_ID = os.environ['BADGER_CAM_CHECK_ID']
        
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    logging.info("\nConnecting to AGO")
    gis = connect_to_ago(username=USERNAME, password=PASSWORD, url=URL)

    logging.info("\nGetting AGO layers")
    camera_point_flayer, camera_point_data, camera_check_table, camera_check_data = get_ago_layers(gis=gis, ago_item_id=CAMERA_CHECK_ITEM_ID)

    logging.info("\nChecking culvert assessment completion status")
    check_status_list, number_of_days = check_assessment_status(camera_check_data)

    if number_of_days > 5 and "No" not in check_status_list:
        # change all the check statuses back to No
        logging.info("\nUpdating point status back to 'No'")
        change_check_status(camera_point_flayer, camera_point_data)

        logging.info("Script Complete!")
        
    else: 
        logging.info("\nUpdating camera location completion status based on the most recent check")
        update_camera_check_completion(camera_point_flayer=camera_point_flayer, camera_point_data=camera_point_data, camera_check_table=camera_check_table)

        logging.info("Script Complete!")

# connect to ago
def connect_to_ago(username, password, url):
    """
    Returns AGO GIS connection
    """
    try:
        gis = GIS(url=url, username=username, password=password)
        logging.info(f"..connected to AGO as {gis.users.me.username}")

    except:
        raise ValueError("..Unable to connect to AGO")
    
    return gis

def get_ago_layers(gis, ago_item_id):
    """
    Returns AGO layers, data 
    """

    ago_item = gis.content.get(ago_item_id)

    # camera point feature layer
    camera_point_flayer = ago_item.layers[0]
    flayer_properties = camera_point_flayer.query()
    camera_point_data = flayer_properties.features

    # related camera check table
    camera_check_table = ago_item.tables[0]
    table_properties = camera_check_table.query()
    camera_check_data = table_properties.features

    logging.info(f'..successfully retrieved feature layer and table data')

    return camera_point_flayer, camera_point_data, camera_check_table, camera_check_data


def check_assessment_status(camera_check_data):
    """
    Checks the completion status of the most recent culvert assessments
    """

    now = datetime.now()
    check_status_list = []
    check_date_list = []

    logging.info("..finding all checks' status")
    for row in camera_check_data:
        check_status = row.attributes['CHECK_COMPLETE']
        check_date_unix = row.attributes['DATETIME_ASSESSED']/1000
        check_date = datetime.fromtimestamp(check_date_unix)

        check_status_list.append(check_status)
        check_date_list.append(check_date)

    logging.info("..calculating number of days since last check")
    # sort list 
    check_date_list.sort(reverse=True)

    # latest check
    latest_check = check_date_list[0]

    # number of days between the most recent check and today
    time_delta = now - latest_check
    number_of_days = time_delta.days    

    return check_status_list, number_of_days

def change_check_status(camera_point_flayer, camera_point_data):
    """ 
    Changes all the most recent check statuses to "No" in preparation for a new field outing
    """

    features_for_update = []

    for camera in camera_point_data:

        # get the camera point's unique id
        camera_id = camera.attributes['PROJ_UNIQUE_ID']

        original_feature = [f for f in camera_point_data if f.attributes['PROJ_UNIQUE_ID'] == camera_id][0]
        feature_to_update = deepcopy(original_feature)
        feature_to_update.attributes['CHECK_COMPLETE'] = "No"
        features_for_update.append(feature_to_update)

    if features_for_update:
        logging.info(f"..Updating {len(features_for_update)} camera checks' completion status")
        camera_point_flayer.edit_features(updates=features_for_update)          


def update_camera_check_completion(camera_point_flayer, camera_point_data, camera_check_table):
    """
    Updates the camera point location's completion status based on the most recent camera check
    """

    # list containing corrected features
    features_for_update = []

    for camera in camera_point_data:

        # get the camera point's unique id
        camera_id = camera.attributes['PROJ_UNIQUE_ID']

        # get the camera point completion status
        camera_pt_complete = camera.attributes['CHECK_COMPLETE']

        # query for related camera check records
        camera_checks = camera_check_table.query(where=f"PROJ_UNIQUE_ID = \'{camera_id}\'")

        # if there are no camera checks, skip that feature
        if len(camera_checks) == 0:
            continue

        # sort camera checks by date in descending order
        camera_checks_sorted = camera_checks.sdf.sort_values(by='DATETIME_ASSESSED', ascending=False)

        # find the latest camera check
        latest_check = camera_checks_sorted.iloc[0]

        # get the value from the CHECK_COMPLETE field
        check_complete = latest_check['CHECK_COMPLETE']

        if camera_pt_complete != check_complete:
            original_feature = [f for f in camera_point_data if f.attributes['PROJ_UNIQUE_ID'] == camera_id][0]
            feature_to_update = deepcopy(original_feature)
            feature_to_update.attributes['CHECK_COMPLETE'] = check_complete
            features_for_update.append(feature_to_update)

    if features_for_update:
        logging.info(f"..Updating {len(features_for_update)} camera checks' completion status")
        camera_point_flayer.edit_features(updates=features_for_update)


if __name__ == "__main__":
    run_app()