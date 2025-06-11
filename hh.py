import streamlit as st
import ibm_boto3
from ibm_botocore.client import Config
import pandas as pd
from io import BytesIO
import re
from datetime import datetime

# Streamlit app title
st.title("IBM Cloud Object Storage File Upload")

# IBM COS Credentials
credentials = {
    "apikey": "axCN_xatDLCDPi1YNw6WTtzefxNPoX9-2csNGUoByv3f",
    "iam_serviceid_crn": "crn:v1:bluemix:public:cloud-object-storage:global:a/fddc2a92db904306b413ed706665c2ff:e99c3906-0103-4257-bcba-e455e7ced9b7:bucket:schedulereport1",
    "endpoint_url": "https://s3.us-south.cloud-object-storage.appdomain.cloud",
    "auth_endpoint": "https://iam.cloud.ibm.com/identity/token",
    "bucket_name": "schedulereport1"
}
 
# Initialize session state for COS client and folders
if 'cos_client' not in st.session_state:
    st.session_state.cos_client = None
if 'folders' not in st.session_state:
    st.session_state.folders = []

# Function to initialize COS client
def init_cos_client():
    try:
        cos = ibm_boto3.client(
            service_name='s3',
            ibm_api_key_id=credentials['apikey'],
            ibm_service_instance_id=credentials['iam_serviceid_crn'],
            ibm_auth_endpoint=credentials['auth_endpoint'],
            config=Config(signature_version='oauth'),
            endpoint_url=credentials['endpoint_url']
        )
        # Verify bucket access
        cos.head_bucket(Bucket=credentials['bucket_name'])
        st.session_state.cos_client = cos
        return True
    except Exception as e:
        st.error(f"Failed to connect to IBM COS: {str(e)}")
        return False

# Function to list all folders in the bucket
def list_folders():
    if st.session_state.cos_client is None:
        st.warning("Not connected to IBM COS.")
        return []
    try:
        response = st.session_state.cos_client.list_objects_v2(Bucket=credentials['bucket_name'], Delimiter='/')
        folders = []
        if 'CommonPrefixes' in response:
            folders = [prefix['Prefix'].rstrip('/') for prefix in response['CommonPrefixes']]
        return folders
    except Exception as e:
        st.error(f"Error listing folders: {str(e)}")
        return []

# Function to validate file name format: name(DD-MM-YYYY).xlsx
def validate_filename(filename):
    pattern = r'^[a-zA-Z0-9\s]+\(\d{2}-\d{2}-\d{4}\)\.xlsx$'
    if not re.match(pattern, filename):
        return False
    # Extract date part and validate
    try:
        date_str = filename.split('(')[1].split(')')[0]
        datetime.strptime(date_str, '%d-%m-%Y')
        return True
    except ValueError:
        return False

# Function to upload file to selected folder
def upload_file_to_cos(file, folder):
    if st.session_state.cos_client is None:
        st.warning("Not connected to IBM COS.")
        return
    try:
        # Validate file name
        if not validate_filename(file.name):
            st.error("Invalid file name. File must be named as 'name(DD-MM-YYYY).xlsx' (e.g., Structure Work Tracker EWS LIG P4 all towers(05-05-2025).xlsx)")
            return
        
        # Ensure folder ends with '/'
        folder_key = f"{folder}/" if not folder.endswith('/') else folder
        file_key = f"{folder_key}{file.name}"
        # Read Excel file into BytesIO
        file_buffer = BytesIO(file.read())
        # Upload the file (IBM COS handles same-named files by overwriting)
        st.session_state.cos_client.upload_fileobj(file_buffer, credentials['bucket_name'], file_key)
        st.success(f"File '{file.name}' uploaded to '{folder_key}' successfully!")
    except Exception as e:
        st.error(f"Error uploading file: {str(e)}")
        if "AccessDenied" in str(e):
            st.error("This error typically means the IBM COS credentials lack write permissions for the bucket. Please check the IAM policies for your API key.")

# Automatically connect to IBM COS on app load
if st.session_state.cos_client is None:
    if init_cos_client():
        st.session_state.folders = list_folders()
    else:
        st.session_state.folders = []

# Display bucket name as title
st.header(f"Bucket: {credentials['bucket_name']}")

# Folder selection and file upload
if st.session_state.folders:
    st.info("File must be named as 'name(DD-MM-YYYY).xlsx' (e.g., Structure Work Tracker EWS LIG P4 all towers(05-05-2025).xlsx). Files with the same name will overwrite existing files in the bucket.")
    selected_folder = st.selectbox("Select Folder", st.session_state.folders)
    uploaded_file = st.file_uploader("Upload Excel File", type=['xlsx'])
    
    if uploaded_file is not None:
        if st.button("Upload File"):
            upload_file_to_cos(uploaded_file, selected_folder)
else:
    st.info("No folders available. Please check your IBM COS bucket configuration.")