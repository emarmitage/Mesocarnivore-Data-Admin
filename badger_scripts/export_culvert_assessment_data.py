###### add header info here #######

from arcgis import GIS
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import sys
import simplekml
import os
import zipfile
import shutil
from minio import Minio
import smtplib, urllib
from datetime import datetime
from email.message import EmailMessage

def main():

    SURVEY_ITEM_ID = os.environ['REQUEST_BADGER_DATA_ITEM_ID']
    CULVERT_ITEM_ID = os.environ['BADGER_CULVERT_ITEM_ID']

    NOW = datetime.today().strftime('%Y-%m-%d')

    ago_user, ago_pass, url, endpoint, access_id, secret = get_input_parameters()
    gis, survey_item, survey_flayer, survey_feature, new_guids = check_new_records(url, ago_user, ago_pass, item_id=SURVEY_ITEM_ID)

    for guid in new_guids:
        print("Creating S3 Connection")
        s3_connection = create_s3_connection(endpoint, access_id, secret)

        change_feature_status(survey_flayer, 
                              update_field="submission_status",
                              new_status="IN PROGRESS",
                              primary_key=guid)
        
        print("Getting survey data")
        email, start_date, end_date, initials = get_survey_data(survey_flayer, primary_key=guid)
        
        data_name = f"{NOW}_{initials}"
        print("Creating new directory")
        proj_dir, data_dir, photo_dir = create_new_directory(data_name)
        
        print("Getting culvert assessment data")
        culvert_flayer, culvert_table, culvert_assessment_data, culvert_loc_df = get_culvert_assessment_data(gis,
                                                                                                            CULVERT_ITEM_ID,
                                                                                                            start_date=start_date,
                                                                                                            end_date=end_date,
                                                                                                            initials=initials,
                                                                                                            email=email,
                                                                                                            data_name=data_name)
        if culvert_assessment_data:
            
            download_to_other_spatial_format(culvert_loc_df, culvert_assessment_data, data_name, data_dir)

            save_photos_to_folder(culvert_flayer, culvert_table, culvert_assessment_data, culvert_loc_df, data_name, photo_dir)

            zip_file = zip_project_files(proj_dir, data_name)

            s3_object, url = upload_s3_object(s3_connection=s3_connection, 
                                                bucket='bmrm', 
                                                s3_file_path=(f"passability_assessments/{data_name}.zip"), 
                                                upload_file_path=zip_file, 
                                                content_type="application/zip", 
                                                public=True, 
                                                part_size = 15728640)

            send_email(email=email, s3_link=url, data_name=data_name)

            change_feature_status(survey_flayer, 
                            update_field="submission_status",
                            new_status="COMPLETE",
                            primary_key=guid)

        else:
            send_request_error_email(email=email, 
                                    data_name=data_name, 
                                    error_message=f"Did not find culvert assessment data associated with initials: {initials} for between {start_date} and {end_date}",
                                    start_date=start_date,
                                    end_date=end_date,
                                    initial=initials)

            change_feature_status(survey_flayer, 
                            update_field="submission_status",
                            new_status="FAILED",
                            primary_key=guid)    

            sys.exit("No culvert assessment data found. Exiting")


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
    url = os.environ['MAPHUB_URL']

    endpoint = os.environ['OBJ_STORE_USER']
    access_id = os.environ['OBJ_STORE_HOST']
    secret = os.environ['OBJ_STORE_API_KEY']

    return ago_user, ago_pass, url, endpoint, access_id, secret

def create_s3_connection(endpoint, access_id, secret):
    """
    Creates a connection to S3 object storage

        Parameters:
            env_variable_endpoint (str): REST endpoint for S3 storage
            env_variable_id (str): Access key ID
            env_variable_key (str): Secret access key

        Returns:
                S3Connection (obj): Minio connection to S3 Object Storage bucket
    """
    s3_connection = Minio(endpoint, access_id, secret)
    print("Successful S3 Object Storage connection")

    return s3_connection

def check_new_records(url, ago_user, ago_pass, item_id):
    """
    Checks for new survey submissions (i.e. data requests)

    Returns: AGO GIS connection
    """

    print("Checking for new data requests")

    try:
        gis = GIS(url, ago_user, ago_pass)
    
    except:
        raise ValueError("  Unable to connect to AGO")
    
    # get ago item
    survey_item = gis.content.get(item_id)

    # from ago item, get feature layer
    survey_flayer = survey_item.layers[0]

    # check for new submissions
    new_guids = []
    features = survey_flayer.query(where="submission_status = 'IN PROGRESS' OR submission_status = 'NOT STARTED' OR submission_status IS NULL OR submission_status = ''")
    for feature in features: # this may need to be for feature in features.feature
        new_guids.append(feature.attributes["globalid"])

    if len(new_guids) > 0:
        print(f"    {len(new_guids)} new records detected")
        return gis, survey_item, survey_flayer, feature, new_guids
    else:
        print("     No new submissions - exiting script")
        sys.exit()

############ PROCESS NEW RECORDS ################

# change feature status
def change_feature_status(survey_flayer, update_field, new_status, primary_key, primary_key_field="globalid"):
    survey_feature = survey_flayer.query(where=f"{primary_key_field} = '{primary_key}'")

    feature_to_update = survey_feature.features[0]
    previous_status = feature_to_update.attributes[update_field]
    feature_to_update.attributes[update_field] = new_status

    try:
        survey_flayer.edit_features(updates=[feature_to_update])
        print(f"    Status of Object {primary_key} updated from '{previous_status}' to {new_status}")

    except Exception as e:
        print(f"    Error updating feature: {e}")
        return
    
def get_survey_data(survey_flayer, primary_key, primary_key_field="globalid"):
    """
    Gets data from survey for use in the script

    Returns: Project ID, Email, Start Date, End Date
    """

    survey_features = survey_flayer.query(where=f"{primary_key_field} = '{primary_key}'")
    survey_feature = survey_features.features[0]

    # project_id = survey_feature.attributes["project_id"]
    email = survey_feature.attributes["email"]
    start_date = survey_feature.attributes["start_date"]
    end_date = survey_feature.attributes["end_date"]
    initials = survey_feature.attributes["initials"]

    # pst_timezone = pytz.timezone('US/Pacific')

    # Convert Unix time to datetime object
    start_date = datetime.fromtimestamp(start_date / 1000).strftime('%Y-%m-%d')
    end_date = datetime.fromtimestamp(end_date / 1000).strftime('%Y-%m-%d')

    # Convert timestamps to datetime objects with PST timezone
    # start_date = datetime.fromtimestamp(start_date / 1000, tz=pst_timezone)
    # end_date = datetime.fromtimestamp(end_date / 1000, tz=pst_timezone)

    # # Print the formatted date and timezone info
    # print(start_date.strftime('%Y-%m-%d'), start_date.tzinfo)
    # print(end_date.strftime('%Y-%m-%d'), end_date.tzinfo)

    return email, start_date, end_date, initials
    
def create_new_directory(folder_name):
    """
    Creates new folders to save output files
    """
    # directory paths
    proj_dir = f"{folder_name}"
    data_dir = os.path.join(proj_dir, "data")
    photo_dir = os.path.join(proj_dir, "photos")

    # make the root project directories
    os.makedirs(f"{folder_name}", exist_ok=True)
    os.makedirs(os.path.join(proj_dir, "data"), exist_ok=True)
    os.makedirs(os.path.join(proj_dir, "photos"), exist_ok=True)

    return proj_dir, data_dir, photo_dir

def get_culvert_assessment_data(gis, culvert_item_id, start_date, end_date, initials, email, data_name):
    """
    Queries culvert assessment data 
    """

    culvert_item = gis.content.get(culvert_item_id)
    # get the point feature layer
    culvert_flayer = culvert_item.layers[0]
    # get the related table content
    culvert_table = culvert_item.tables[0]

    # query culvert assessments for initials and date range
    # query = f"UPPER(ASSESSOR_INITIALS) LIKE UPPER('%{initials}%') and (DATE_ASSESSED >= TIMESTAMP '{start_date} 00:00:00' and DATE_ASSESSED <= TIMESTAMP '{end_date} 23:59:59')"
    query = f"UPPER(ASSESSOR_INITIALS) LIKE UPPER('%{initials}%')"
    culvert_assessment_data = culvert_table.query(where=query)

    # convert to spatial data frame
    culvert_assessment_sdf = culvert_assessment_data.sdf
    print(culvert_assessment_sdf)

    if len(culvert_assessment_sdf) != 0:
        print(f"..initial query returned {len(culvert_assessment_sdf)} features")
        # Strip time from DATE_ASSESSED
        culvert_assessment_sdf['DATE_ASSESSED'] = culvert_assessment_sdf['DATE_ASSESSED'].dt.normalize()

        # convert to timezone aware columns and query for date range
        if culvert_assessment_sdf['DATE_ASSESSED'].dt.tz is None:
            culvert_assessment_sdf['DATE_ASSESSED'] = culvert_assessment_sdf['DATE_ASSESSED'].dt.tz_localize('UTC')    
        culvert_assessment_sdf['DATE_ASSESSED'] = culvert_assessment_sdf['DATE_ASSESSED'].dt.tz_convert('US/Pacific')

        # Ensure start and end dates are timezone-aware (in 'US/Pacific')
        start_date_pst = pd.to_datetime(start_date).tz_localize('US/Pacific', ambiguous='NaT')
        end_date_pst = pd.to_datetime(end_date).tz_localize('US/Pacific', ambiguous='NaT')

        # Filter records within the date range
        culvert_assessment_df = culvert_assessment_sdf[
            (culvert_assessment_sdf['DATE_ASSESSED'] >= start_date_pst) & 
            (culvert_assessment_sdf['DATE_ASSESSED'] <= end_date_pst)
        ]

    else:
        culvert_assessment_data = []
        culvert_loc_df = []

    # check if there are features
    if len(culvert_assessment_df) != 0:
        print(f"..date query returned {len(culvert_assessment_df)} features")

        # get the objectids to query related records
        culvert_assessment_oids = culvert_assessment_df['OBJECTID'].tolist()

        # convert list of oids to correct format to query related records
        oids_string = ",".join(map(str, culvert_assessment_oids))

        # query culvert assessment data
        oid_query = f"OBJECTID = {oids_string}"
        culvert_assessment_data_filtered = culvert_table.query(where=oid_query)

        # query related records
        print("..querying related culvert locations")
        related_points = culvert_table.query_related_records(object_ids=f'{oids_string}', relationship_id=0, out_fields=["*"], return_geometry=True)

        # get the relatedRecordGroups from the related_data dictionary and extract the attributes
        related_record_groups = (related_points['relatedRecordGroups'])

        # Extract the attributes
        related_data = []
        related_geometries = []

        for group in related_record_groups:
            for record in group.get('relatedRecords', []):
                attributes = record.get('attributes', {})
                geometry = record.get('geometry', {})

                related_data.append(attributes)
                # Convert geometry to Shapely Point
                point = Point(geometry['x'], geometry['y'])
                related_geometries.append(point)

        # convert list of attributes to a dataframe
        culvert_loc_df = pd.DataFrame(related_data)

        # add geometry column
        culvert_loc_df['geometry'] = related_geometries

    else:
        culvert_assessment_data = []
        culvert_loc_df = []

    return culvert_flayer, culvert_table, culvert_assessment_data_filtered, culvert_loc_df

def download_to_other_spatial_format(culvert_loc_df, culvert_assessment_data, data_name, data_dir):
    """
    Downloads data to alternate spatial formats

    Returns: CSV, Shapefile, KML
    """

    # assign base file name
    filename = f"culvert_assessment_data_{data_name}"

    # convert related records to dataframe 
    related_df = culvert_assessment_data.sdf

    # convert culvert location to geodataframe
    culvert_gdf = gpd.GeoDataFrame(culvert_loc_df, geometry='geometry', crs="3005")

    print("..cleaning and formatting fields")
    # format times and convert to correct time zone
    culvert_gdf['DATE_TIME_CREATED'] = pd.to_datetime(culvert_gdf['DATE_TIME_CREATED'],unit='ms')

    print("...converting timestamps")
    if culvert_gdf['DATE_TIME_CREATED'].dt.tz is None:
        culvert_gdf['DATE_TIME_CREATED'] = culvert_gdf['DATE_TIME_CREATED'].dt.tz_localize('UTC')    
    culvert_gdf['DATE_TIME_CREATED'] = culvert_gdf['DATE_TIME_CREATED'].dt.tz_convert('US/Pacific')
    culvert_gdf['DATE_TIME_CREATED'] = culvert_gdf['DATE_TIME_CREATED'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # columns to drop
    assessment_drop_cols = ['SECONDARY_FARM', 'SECONDARY_DRAINAGE', 'SECONDARY_WILDLIFE', 
                            'SECONDARY_PEDESTRIAN', 'SECONDARY_UNKNOWN', 'TRACKS_THROUGH_CULVERT', 'TRACKS_PERPENDICULAR',
                            'TRACKS_15M', 'TRACKS_PEOPLE', 'TRACKS_ANIMALS', 'TRACKS_LIVESTOCK', 'TRACKS_VANDALISM', 'TRACKS_NO_SIGN',
                            'GlobalID', 'CreationDate', 'Creator', 'EditDate', 'Editor']
    point_drop_cols = ['GlobalID', 'CreationDate', 'Creator', 'EditDate', 'Editor', 'LANDSCAPE_CONNECT', 'MACHINE_EXCAV_REQ','UNDERPASS_PRIORITY']
    
    print("...dropping columns")
    # drop unnecessary columns from both dataframes
    related_df.drop(columns=assessment_drop_cols, inplace=True)
    culvert_gdf.drop(columns=point_drop_cols, inplace=True)
    
    # rename columns for readability
    point_mapper = {
        "SITE_ID": "Culvert Location ID",
        "RECORD_CHRIS_ID": "Record CHRIS ID?",
        "CHRIS_ID": "CHRIS Culvert ID",
        "RECORD_BMIS_ID": "Record BMIS ID?",
        "BMIS_ID": "BMIS ID",
        "RECORD_RAIL_ID": "Record Rail ID?",
        "RAIL_ID": "RAIL ID",
        "ASSESSOR_INITIALS": "Assessor Initials(s)",
        "DATE_TIME_CREATED": "Date and Time",
        "TRAFFIC_DIRECTION": "Highway Traffic Direction",
        "CARDINAL_DIRECTION": "Cardinal Direction",
        "LATITUDE": "Latitude",
        "LONGITUDE": "Longitude",
        "PHOTO_NAME": "Photo Name(s)",
        "FLAG_CULVERT_DISCREPANCY": "Flag CHRIS Disscrepancy",
        "CHRIS_CULVERT_MISSING": "Culvert Missing In CHRIS",
        "FIELD_CULVERT_MISSING": "Culvert Missing In The Field",
        "SUSPECTED_CHRIS_ID": "Suspected CHRIS ID",
        "COMMENTS": "Comments",
        "STATUS": "Status",
        "DELETE_POINT": "Delete Point",
    }

    table_mapper = {
        "SITE_ID": "Culvert Location ID",
        "SITE_ASSESS_ID": "Culvert Assessment ID",
        "ASSESSOR_INITIALS": "Assessor Name(s)",
        "DATE_ASSESSED": "Date",
        "RECORD_CHRIS_ID": "Record CHRIS ID?",
        "CHRIS ID": "CHRIS ID",
        "SPECIES_GUILD": "Species Guild",
        "STRUCTURE_TYPE": "Structure Type",
        "STRUCTURE_SIZE_CM": "Structure Size (cm)",
        "STRUCTURE_SIZE_MM": "Structure Size (mm)",
        "MAIN_FUNCTION": "Main Function",
        "SECONDARY_FUNCTION": "Secondary Function(s)",
        "STRUCTURE_FUNCTIONAL": "Functional",
        "LANDSCAPE_CONNECT": "Local Landscape Connectivity",
        "RIPARIAN_CORRIDOR": "Riparian Corridor",
        "RAIL_COUPLING": "Rail Coupling Nearby",
        "SEASONAL_FLOW": "Seasonal Flow",
        "GRATE": "Grate",
        "PASSABLE": "Passable",
        "OPENNESS": "Openness (%)",
        "UNDERPASS_VISIBILITY": "Visibility Through Underpass",
        "MOISTURE": "Moisure",
        "WATER_DEPTH": "Water Depth (cm)",
        "TRACKS_SIGNS": "Tracks & Sign",
        "TRACKS_SIGNS_DESCRIBE": "Describe Tracks & Sign",
        "HAND_EXCAV_REQ": "Hand Excavation Required",
        "MACHINE_EXCAV_REQ": "Machine Excavation Required",
        "CAMERA_INSTALL": "Camera Installation Possible",
        "CAMERA_THEFT": "Camera Theft Potential",
        "CAMERA_SUITABILITY": "Overall Camera Suitability",
        "UNDERPASS_PRIORITY": "Potential Underpass Priority",
        "PHOTO_NAME": "Photo Name(s)",
        "COMMENTS": "Comments"
    }

    point_col_order = list(point_mapper.values())
    table_col_order = list(table_mapper.values())

    print("...renaming columns")
    # rename columns
    culvert_gdf.rename(columns=point_mapper, inplace=True)
    related_df.rename(columns=table_mapper, inplace=True)

    # order columns
    culvert_gdf.loc[:, point_col_order]
    related_df.loc[:, table_col_order]

    # convert date columns to string
    date_columns = culvert_gdf.select_dtypes(include=['datetime']).columns
    for column in date_columns:
        culvert_gdf[column] = culvert_gdf[column].dt.strftime('%Y-%m-%d')

    date_columns_related = related_df.select_dtypes(include=['datetime']).columns
    for related_column in date_columns_related:
        related_df[related_column] = related_df[related_column].dt.strftime('%Y-%m-%d')

    print("...merging dataframes")
    # join culvert gdf and df 
    culvert_gdf_joined = culvert_gdf.merge(related_df, on='CHRIS Culvert ID')
    # culvert_gdf_joined = culvert_gdf.merge(related_df, on='SITE_ID')

    print("...converting float64 datatypes to float32 for compatible data export")
    for column in culvert_gdf_joined.columns:
        if culvert_gdf_joined[column].dtype == "Float64":
            culvert_gdf_joined[column] = culvert_gdf_joined[column].astype("float32")

    print("...projecting dataset to save as KML")
    # project to WGS84 for KML only
    culvert_gdf_kml = culvert_gdf_joined.to_crs(epsg=4326)

    # 1. Save to kml
    kml = simplekml.Kml()
    for _, row in culvert_gdf_kml.iterrows():
        coords = row["geometry"]
        kml.newpoint(
            name=str(row["OBJECTID_x"]),
            coords=[(coords.x, coords.y)]
        )
    kml_path = os.path.join(data_dir, f"{filename}.kml")
    kml.save(kml_path)
    print(f"    Data successfully saved to KML at {kml_path}")
    
    # 2. Save as csv
    csv_base_name = os.path.join(data_dir, f"{filename}")
    csv_point = culvert_gdf.to_csv(f"{csv_base_name}_culvert_location.csv", index=False)
    csv_table = related_df.to_csv(f"{csv_base_name}_culvert_assessment.csv", index=False)
    print(f"    Data successfully saved to CSV")

    # 3. Save as shapefile
    if not os.path.exists(os.path.join(data_dir, "shapefile")):
        os.makedirs(os.path.join(data_dir, "shapefile"))

    shapefile_path = os.path.join(data_dir, "shapefile", f"{filename}.shp")
    culvert_gdf_joined.to_file(shapefile_path)
    print(f"    Data successfully saved to Shapefile at {shapefile_path}")

    # zip the shapefile folder
    shapefile_dir = os.path.dirname(shapefile_path)
    shp_extensions = ['.shx', '.shp', '.prj', '.dbf', '.cpg']
    with zipfile.ZipFile(f"{shapefile_dir}.zip", mode="w") as archive:
        for ext in shp_extensions:
            file_path = os.path.join(shapefile_dir, f"{filename}{ext}")
            archive.write(file_path, os.path.basename(file_path))

    # delete shapefiles & shapefile folder
    for ext in shp_extensions:
        file_path = os.path.join(shapefile_dir, f"{filename}{ext}")
        if os.path.exists(file_path):
            os.remove(file_path)       
    shp_folder = os.path.join(data_dir, "shapefile")
    os.rmdir(shp_folder)

    return f"{filename}_culvert_location.csv", f"{filename}_culvert_assessment.csv", f"{filename}.kml", f"{filename}.zip"

# save and rename photos to a folder
def save_photos_to_folder(culvert_flayer, culvert_table, culvert_assessment_data, culvert_loc_df, data_name, photo_dir):
    """
    Saves photos to a folder
    """
    # create a list of OIDS
    lst_oids = culvert_loc_df['OBJECTID'].tolist()

    # get OIDs of related features
    # lst_check_oids = pd.DataFrame(culvert_assessment_data)['OBJECTID'].tolist()
    lst_check_oids = culvert_assessment_data.sdf['OBJECTID'].tolist()

    ########## ADD A CHECK FOR PHOTO RENAMING. IF NOT RENAMED, CALL RENAMING FUNCTION ##########
    # rename the photo (if necessary) - THIS NEEDS WORK
    # rename_culvert_loc_attachments(ago_flayer=culvert_flayer, flayer_properties=culvert_data, flayer_data=culvert_data)

    # rename_culvert_check_attachments(ago_flayer=culvert_table, flayer_properties=related_data, flayer_data=related_data)

    ###### COMMENTED OUT FOR NOW TO AVOID OVERWRITING
    # 2. Related table: rename and save attachments for each feature
    if lst_check_oids:
        for oid in lst_check_oids:

            lst_attachments = culvert_table.attachments.get_list(oid=oid)

            if lst_attachments:

                for attachment in lst_attachments:
                    attach_id = attachment['id']
                    culvert_table.attachments.download(oid=oid, attachment_id=attach_id, save_path=photo_dir)[0]

# zip all the files in the data_name folder
def zip_project_files(proj_dir, data_name):
    """
    Zips all files in the project directory
    """
    zip_file = os.path.join(f"{data_name}.zip")

    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(proj_dir):
            for file in files:

                # create the relative path of hte file with respect to the main folder
                relative_path = os.path.relpath(os.path.join(root, file), proj_dir)

                # write the file to the zip file
                zipf.write(os.path.join(root, file), relative_path)

    shutil.rmtree(proj_dir)

    return zip_file

# upload zipped folder to object storage
def upload_s3_object(s3_connection, bucket, s3_file_path, upload_file_path, content_type="application/zip", public=True, part_size = 15728640):
    '''
    Upload file to S3 Object Storage. s3_path parameter must include filename. Objects to set content type and public read permission.
    
            Parameters:
                    s3_connection (obj): Minio connection to S3 Object Storage bucket
                    s3_file_path: path to location in s3 bucket
                    upload_file_path: path to file to be uploaded
                    content_type (bool): type of content being uploaded
                    public (str): whether to make file publicly available (true = public)
                    part_size (int):

            Returns:
                    s3_object (obj): Minio object representing object/item in S3 Object Storage
    '''
    s3_object = s3_connection.fput_object(bucket, s3_file_path, upload_file_path, content_type, metadata={"x-amz-acl": "public-read"},part_size=part_size)
    print(f"File uploaded to s3 browser: {s3_file_path}")

    if public:
        url = s3_connection.get_presigned_url("GET", bucket, s3_file_path) # expires=datetime.timedelta(days=7))

    return s3_object, url


# send emails
def send_request_error_email(email, data_name, initial, start_date, end_date, error_message):
        # create the email message object
        msg = EmailMessage()
        # define the message subject
        msg['Subject'] = f'Culvert Assessment: Error in Data Request {data_name}'
        # get from excel, to-do
        msg['To'] = email
        msg['From'] = 'emma.armitage@gov.bc.ca'
        # define the message body in text and html
        text = f'Hello,\n\nThere has been an error processing the survey data for {data_name}. The error is:\n\n{error_message}\n\nPlease confirm you have entered valid initials and date range.'    
        html = f'<html><body><p>Hello,</p><p>There has been an error processing the survey data for {data_name}. The error is:</p><p>{error_message}</p><p>Please confirm you have entered valid initials and date range.</p></body></html>'
        # set the message body as text and html
        msg.set_content(text)
        msg.add_alternative(html, subtype='html')
        # send the email
        with smtplib.SMTP("apps.smtp.gov.bc.ca") as server:
            # start STMP session by issuing EHLO command
            server.ehlo()
            # put SMTP connection into TLS mode
            server.starttls()
            # identify yourself again as we enabled TLS
            server.ehlo()
            # send message via email.message.Message object
            server.send_message(msg)

def send_error_email(email, data_name, error_message):
        # create the email message object
        msg = EmailMessage()
        # define the message subject
        msg['Subject'] = f'PSCIS: Error in Project {data_name}'
        # get from excel, to-do
        msg['To'] = email
        msg['From'] = 'emma.armitage@gov.bc.ca'
        # define the message body in text and html
        text = f'Hello,\n\nThere has been an error processing the survey data for {data_name}. The error is:\n\n{error_message}\n\nPlease notify the contractor associated with the project.'    
        html = f'<html><body><p>Hello,</p><p>There has been an error processing the survey data for {data_name}. The error is:</p><p>{error_message}</p><p>Please notify the contractor associated with the project.</p></body></html>'
        # set the message body as text and html
        msg.set_content(text)
        msg.add_alternative(html, subtype='html')
        # send the email
        with smtplib.SMTP("apps.smtp.gov.bc.ca") as server:
            # start STMP session by issuing EHLO command
            server.ehlo()
            # put SMTP connection into TLS mode
            server.starttls()
            # identify yourself again as we enabled TLS
            server.ehlo()
            # send message via email.message.Message object
            server.send_message(msg)

def send_email(email, s3_link, data_name):
        # split presigned URL into dictionary of query parameters
        query = urllib.parse.parse_qs(s3_link)
        # create the email message object
        msg = EmailMessage()
        # define the message subject
        msg['Subject'] = f'Culvert Assessment: Data Location for {data_name}'
        # get from excel, to-do
        if email:
            msg['To'] = email
            msg['From'] = 'emma.armitage@gov.bc.ca'
            # define the message body in text and html
            text = f'Hello,\n\nThe data and photos for Badger Culvert Passability Assessment {data_name} can be downloaded by clicking the link below.\n\n{s3_link}\n\nWithin the downloaded zip file, you will find:\n\n- The Photos folder that contains photos taken for each assessment.\n- The Data folder that contains CSV, KML, and Shapefile documents showing all inputted data for your project.\n\nIf you have any questions, please contact Karina Lamy by email' 

            html = f'<html><body><p>Hello,</p><p>The data and photos for Badger Culvert Passability Assessment {data_name} can be downloaded by clicking the link below.</p><p>{s3_link}</p><p>Within the downloaded zip file, you will find:</p><ul><li><b>The Photos folder</b> that contains photos taken for each assessment.</li><li><b>The Data folder</b> that contains CSV, KML, and Shapefile documents showing all inputted data for your project.</li></ul><p>If you have any questions, please contact <b>Karina Lamy</b> by email</p></body></html>'
            # set the message body as text and html
            msg.set_content(text)
            msg.add_alternative(html, subtype='html')
            # send the email
            with smtplib.SMTP("apps.smtp.gov.bc.ca") as server:
                # start STMP session by issuing EHLO command
                server.ehlo()
                # put SMTP connection into TLS mode
                server.starttls()
                # identify yourself again as we enabled TLS
                server.ehlo()
                # send message via email.message.Message object
                server.send_message(msg)
        else:
            send_error_email('emma.armitage@gov.bc.ca',data_name,"There was an error sending the email to the Contractor. This may be because they entered their email or the project number incorrectly. Please investigate.")


if __name__ == "__main__":
    main()

