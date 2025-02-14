import os
from minio import Minio
from minio.error import S3Error
from arcgis.gis import GIS 

import badger_config

def run_app():

    ago_user, ago_pass, obj_store_user, obj_store_api_key, obj_store_host = get_input_parameters()
    report = BadgerReport(ago_user=ago_user, ago_pass=ago_pass, obj_store_user=obj_store_user, obj_store_api_key=obj_store_api_key, obj_store_host=obj_store_host)

    report.download_attachments()

    del report 

# get user login credentials
def get_input_parameters():
    """
    Function:
        Set up parameters

    Returns:
        tuple: user entered parameters required for tool execution
    """

    # get credentials
    ago_user = os.environ['AGO_USER']
    ago_pass = os.environ['AGO_PASS']
    obj_store_user = os.environ['OBJ_STORE_USER']
    obj_store_api_key = os.environ['OBJ_STORE_API_KEY']
    obj_store_host = os.environ['OBJ_STORE_HOST']

    return ago_user, ago_pass, obj_store_user, obj_store_api_key, obj_store_host


# connect to AGOL and object storage
class BadgerReport:
    def __init__(self, ago_user, ago_pass, obj_store_user, obj_store_api_key, obj_store_host) -> None:
        self.ago_user = ago_user
        self.ago_pass = ago_pass
        self.obj_store_user = obj_store_user
        self.obj_store_api_key = obj_store_api_key
        self.object_store_host = obj_store_host

        self.portal_url = badger_config.MAPHUB
        self.ago_badgers_simpcw = badger_config.BADGERS_SIMPCW

        self.badger_bucket = badger_config.BUCKET
        self.bucket_prefix = "Simpcw_Badger_Data"
        self.bucket_subfolder = "Simpcw_Badger_Photos"

        print("Connecting to MapHub")
        self.gis = GIS(url=self.portal_url, username=self.ago_user, password=self.ago_pass, expiration=9999)
        print("Connection successful")

        print("Connecting to object storage")
        self.s3_connection = Minio(obj_store_host, obj_store_user, obj_store_api_key)

        
    def __del__(self) -> None:
        print("Disconnecting from MapHub")
        del self.gis
        print("Closing object storage connection")
        del self.s3_connection
        
    def list_contents(self) -> list:
        folder_path = os.path.join(self.bucket_prefix, self.bucket_subfolder)

        objects = self.s3_connection.list_objects(bucket_name=self.badger_bucket, prefix=folder_path, recursive=True)

        lst_objects = [os.path.basename(obj.object_name) for obj in objects]

        return lst_objects
        
    def download_attachments(self) -> None:
        """
        Function:
            Master function to download attachments for all required layers in arcgis online
        Returns:
            None
            
        """
        lst_pictures = self.list_contents()

        self.copy_to_object_storage(ago_layer=self.ago_badgers_simpcw, layer_name="Badger Sightings Simpcw", picture="photo_name", lst_os_pictures=lst_pictures)

    #def copy_to_object_storage(self, ago_layer, layer_name, picture, lst_os_pictures, folder) -> None:
    def copy_to_object_storage(self, ago_layer, layer_name, picture, lst_os_pictures) -> None:
        """
        Function:
            Function used to download attachments from arcgis online layers and copy them to object storage.
        Returns:
            None
        """
        print(f"Downloading photos on the {layer_name} layer")

        # gets the AGOL content
        ago_item = self.gis.content.get(ago_layer)

        # gets the ago feature layer
        if layer_name == 'Badger Sightings Simpcw':
            ago_flayer = ago_item.layers[0]

        ago_fset = ago_flayer.query()
        all_features = ago_fset.features 
        print(all_features)
        if len(all_features) == 0:
            return
            
        # save all OIDs from the feature set in a list 
        lst_oids = ago_fset.sdf["objectid"].tolist() 

        # for each object id...
        for oid in lst_oids:
            
            # get a list of dictionaries containings information about attachments
            lst_attachments = ago_flayer.attachments.get_list(oid=oid)

            # check if there are attachments 
            if lst_attachments:

                # find the original feature 
                original_feature = [f for f in all_features if f.attributes["objectid"] == oid][0]

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
                        ostore_path = f"{self.bucket_prefix}/{self.bucket_subfolder}/{attach['name']}"

                        # Upload the file to MinIO bucket
                        try:
                            self.s3_connection.fput_object(self.badger_bucket, ostore_path, attach_file)
                            print(f"File {attach['name']} uploaded successfully to {self.badger_bucket}/{ostore_path}")
                        except S3Error as e:
                            print(f"Error uploading file {attach['name']} to MinIO: {e}")



if __name__ == '__main__':
    run_app()
