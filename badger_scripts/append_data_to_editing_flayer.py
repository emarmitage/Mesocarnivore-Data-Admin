from arcgis.gis import GIS
import os

def run_app():
    gis = connect_to_ago()
    raw_ago_flayer, raw_flayer_properties, raw_flayer_data, editing_ago_flayer, editing_flayer_properties = get_feature_layer_data(gis=gis,
                                                                                                                                    raw_ago_layer_id='fa6cde2315764bc0b19af0d78cee5047',
                                                                                                                                    editing_ago_layer_id='fdb949b3807b4837ab77daeb7a737238')
    new_oids = find_new_oids(raw_flayer_properties, editing_flayer_properties)
    if new_oids:
        add_new_features(new_oids=new_oids,
                         raw_flayer=raw_ago_flayer,
                         raw_flayer_data=raw_flayer_data,
                         editing_flayer=editing_ago_flayer)
    

def connect_to_ago():
    ago_user = os.environ['AGO_USER']
    ago_pass = os.environ['AGO_PASS']
    url = 'https://governmentofbc.maps.arcgis.com/home'

    gis = GIS(username=ago_user, password=ago_pass, url=url)

    return gis

def get_feature_layer_data(gis, raw_ago_layer_id, editing_ago_layer_id):
    """
    Get ArcGIS Online feature layer, properties, and data from the raw and editing feature layers

    Returns:
    - raw_ago_flayer
    - raw_flayer_properties
    - raw_flayer_data
    - editing_ago_flayer
    - editing_ago_properties
    """
    # raw data layer
    raw_ago_item = gis.content.get(raw_ago_layer_id)
    raw_ago_flayer = raw_ago_item.layers[0]
    raw_flayer_properties = raw_ago_flayer.query()
    raw_flayer_data = raw_flayer_properties.features

    # editing data layer
    editing_ago_item = gis.content.get(editing_ago_layer_id)
    editing_ago_flayer = editing_ago_item.layers[0]
    editing_flayer_properties = editing_ago_flayer.query()

    return raw_ago_flayer, raw_flayer_properties, raw_flayer_data, editing_ago_flayer, editing_flayer_properties

def find_new_oids(raw_flayer_properties, editing_flayer_properties):
    """
    Find new features in the raw feature layer to be appended to the editing feature layer

    Returns: New objectids (list)
    """

    # get list of each flayer's objectids
    raw_oids = raw_flayer_properties.sdf['objectid'].tolist()
    editing_oids = editing_flayer_properties.sdf['raw_flayer_oid'].tolist()

    # convert lists to sets for easy comparison
    raw_oids_set = set(raw_oids)
    editing_oids_set = set(editing_oids)

    # find new objectids
    new_oids = raw_oids_set - editing_oids_set

    if list(new_oids):
        return list(new_oids)
    else:
        print("No new features found, exiting script")
        exit()

def add_new_features(new_oids, raw_flayer, raw_flayer_data, editing_flayer):
    """
    Adds new features and attachments, if any, to the editing feature layer
    """
    new_features = []

    for oid in new_oids:
        matching_feature = [f for f in raw_flayer_data if f.attributes['objectid'] == oid]
        if matching_feature:
            new_feature = matching_feature[0]
            new_feature.attributes['raw_flayer_oid'] = oid
            new_features.append(new_feature)

            try:
                response = editing_flayer.edit_features(adds=[new_feature])
                # print(f"Edit Features Response: {response}")
                if response.get('addResults'):
                    for result in response['addResults']:
                        if not result['success']:
                            print(f"Feature add failed: {result['error']}")
                        else:
                            editing_oid = result['objectId']
                            print(f"Feature added successfully with editing OID: {editing_oid}")

                            # check if the feature has attachments
                            if 'photo_name' in new_feature.attributes and new_feature.attributes['photo_name']:
                                upload_attachments(oid=oid,
                                                   editing_oid=editing_oid,
                                                   raw_flayer=raw_flayer,
                                                   editing_flayer=editing_flayer)

            except Exception as e:
                print(f"Error during feature update: {e}")

def upload_attachments(oid, editing_oid, raw_flayer, editing_flayer):
    """
    Downloads attachments from the raw feature layer and uploads them to the editing feature layer
    """
    
    try:       
        lst_attachments = raw_flayer.attachments.get_list(oid=oid)

        for attachment in lst_attachments:

            attach_id = attachment['id']

            try:

                # Download the attachment
                attach_file = raw_flayer.attachments.download(oid=oid, attachment_id=attach_id)[0]
                    
                if attach_file:

                    # Download the attachment
                    print(f"Adding {attachment['name']}")
                    editing_flayer.attachments.add(oid=editing_oid, file_path=attach_file)
                else:
                    print(f"No file returned for attachment ID {attach_id} for OID {oid}")
            except Exception as e:
                print(f"Error downloading or adding attachment ID {attach_id} for OID {oid}: {e}")
                # if attachments fail, remove feature from editing flayer
                try:
                    editing_flayer.edit_features(deletes=[editing_oid])
                    print(f"Feature with OID {editing_oid} removed from editing layer due to attachment error.")
                except Exception as delete_error:
                    print(f"Error removing feature with OID {editing_oid} from editing layer: {delete_error}")

    except Exception as e:
    # Handle errors during the retrieval of attachment list
        print(f"Error retrieving attachment list for OID {oid}: {e}")



if __name__ == '__main__':
    run_app()
