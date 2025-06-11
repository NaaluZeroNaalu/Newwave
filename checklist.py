import streamlit as st
import requests
import json
import urllib.parse
import urllib3
import certifi
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import re
import logging
import io
import os
from dotenv import load_dotenv
import time
import aiohttp
import asyncio
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()

# WatsonX configuration
WATSONX_API_URL = os.getenv("WATSONX_API_URL_1")
MODEL_ID = os.getenv("MODEL_ID_1")
PROJECT_ID = os.getenv("PROJECT_ID_1")
API_KEY = os.getenv("API_KEY_1")

# Check environment variables
if not API_KEY or not WATSONX_API_URL:
    st.warning("WatsonX environment variables missing, proceeding with Python-based analysis.")

# API Endpoints
LOGIN_URL = "https://dms.asite.com/apilogin/"
IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"

# Function to generate access token with expiration tracking
def get_access_token(API_KEY):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }
    data = {
        "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
        "apikey": API_KEY
    }
    try:
        response = requests.post(IAM_TOKEN_URL, headers=headers, data=data, timeout=50)
        if response.status_code == 200:
            token_info = response.json()
            access_token = token_info['access_token']
            expires_in = token_info.get('expires_in', 3600)
            expiration_time = time.time() + expires_in - 300
            logger.info("Access token generated successfully")
            return access_token, expiration_time
        else:
            logger.error(f"Failed to get access token: {response.status_code} - {response.text}")
            st.error(f"❌ Failed to get access token: {response.status_code} - {response.text}")
            return None, None
    except Exception as e:
        logger.error(f"Exception getting access token: {str(e)}")
        st.error(f"❌ Error getting access token: {str(e)}")
        return None, None

# Login Function
def login_to_asite(email, password):
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    payload = {"emailId": email, "password": password}
    response = requests.post(LOGIN_URL, headers=headers, data=payload, verify=certifi.where(), timeout=50)
    if response.status_code == 200:
        try:
            session_id = response.json().get("UserProfile", {}).get("Sessionid")
            logger.info(f"Login successful, Session ID: {session_id}")
            st.session_state.sessionid = session_id
            st.sidebar.success(f"✅ Login successful, Session ID: {session_id}")
            return session_id
        except json.JSONDecodeError:
            logger.error("JSONDecodeError during login")
            st.sidebar.error("❌ Failed to parse login response")
            return None
    logger.error(f"Login failed: {response.status_code}")
    st.sidebar.error(f"❌ Login failed: {response.status_code}")
    return None

# Fetch Workspace ID
def GetWorkspaceID():
    
    url = "https://dmsak.asite.com/api/workspace/workspacelist"
    headers = {
        'Cookie': f'ASessionID={st.session_state.sessionid}',
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    response = requests.get(url, headers=headers)
    # st.write(response.json()['asiteDataList']['workspaceVO'][0]['Workspace_Id'])
    st.session_state.workspaceid = response.json()['asiteDataList']['workspaceVO'][4]['Workspace_Id']
    st.write(f"Workspace ID: {st.session_state.workspaceid}")

# Fetch Project IDs
def GetProjectId():
    url = f"https://adoddleak.asite.com/commonapi/qaplan/getQualityPlanList;searchCriteria={{'criteria': [{{'field': 'planCreationDate','operator': 6,'values': ['11-Mar-2025']}}], 'projectId': {str(st.session_state.workspaceid)}, 'recordLimit': 1000, 'recordStart': 1}}"
    headers = {
        'Cookie': f'ASessionID={st.session_state.sessionid}',
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    response = requests.get(url, headers=headers)
    st.write(response.json())
    st.session_state.veridia_finishing = response.json()['data'][4]['planId']
    st.session_state.veridia_structure = response.json()['data'][6]['planId']
    st.write(f"Veridia Finishing Project ID: {response.json()['data'][4]['planId']}")
    st.write(f"Veridia Structure Project ID: {response.json()['data'][6]['planId']}")

# Asynchronous Fetch Function
async def fetch_data(session, url, headers):
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        elif response.status == 204:
            return None
        else:
            raise Exception(f"Error fetching data: {response.status} - {await response.text()}")

# Fetch All Data with Async
async def GetAllDatas():
    record_limit = 1000
    headers = {'Cookie': f'ASessionID={st.session_state.sessionid}'}
    all_finishing_data = []
    all_structure_data = []

    async with aiohttp.ClientSession() as session:
        # Fetch Veridia Finishing data
        start_record = 1
        st.write("Fetching Veridia Finishing data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanAssociation/?projectId={st.session_state.workspaceid}&planId={st.session_state.veridia_finishing}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Finishing data available (204)")
                    break
                if 'associationList' in data and data['associationList']:
                    all_finishing_data.extend(data['associationList'])
                else:
                    all_finishing_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_finishing_data[-record_limit:])} Finishing records (Total: {len(all_finishing_data)})")
                if len(all_finishing_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
            except Exception as e:
                st.error(f"❌ Error fetching Finishing data: {str(e)}")
                break

        # Fetch Veridia Structure data
        start_record = 1
        st.write("Fetching Veridia Structure data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanAssociation/?projectId={st.session_state.workspaceid}&planId={st.session_state.veridia_structure}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Structure data available (204)")
                    break
                if 'associationList' in data and data['associationList']:
                    all_structure_data.extend(data['associationList'])
                else:
                    all_structure_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_structure_data[-record_limit:])} Structure records (Total: {len(all_structure_data)})")
                if len(all_structure_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
            except Exception as e:
                st.error(f"❌ Error fetching Structure data: {str(e)}")
                break

    df_finishing = pd.DataFrame(all_finishing_data)
    df_structure = pd.DataFrame(all_structure_data)
    desired_columns = ['activitySeq', 'qiLocationId']
    if 'statusName' in df_finishing.columns:
        desired_columns.append('statusName')
    elif 'statusColor' in df_finishing.columns:
        desired_columns.append('statusColor')
        status_mapping = {'#4CAF50': 'Completed', '#4CB0F0': 'Not Started', '#4C0F0': 'Not Started'}
        df_finishing['statusName'] = df_finishing['statusColor'].map(status_mapping).fillna('Unknown')
        df_structure['statusName'] = df_structure['statusColor'].map(status_mapping).fillna('Unknown')
        desired_columns.append('statusName')
    else:
        st.error("❌ Neither statusName nor statusColor found in data!")
        return pd.DataFrame(), pd.DataFrame()

    veridiafinishing = df_finishing[desired_columns]
    veridiastructure = df_structure[desired_columns]

    st.write(f"VERIDIA FINISHING ({', '.join(desired_columns)})")
    st.write(f"Total records: {len(veridiafinishing)}")
    st.write(veridiafinishing)
    st.write(f"VERIDIA STRUCTURE ({', '.join(desired_columns)})")
    st.write(f"Total records: {len(veridiastructure)}")
    st.write(veridiastructure)

    return veridiafinishing, veridiastructure

# Fetch Activity Data with Async
async def Get_Activity():
    record_limit = 1000
    headers = {
        'Cookie': f'ASessionID={st.session_state.sessionid}',
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    all_finishing_activity_data = []
    all_structure_activity_data = []

    async with aiohttp.ClientSession() as session:
        # Fetch Veridia Finishing Activity data
        start_record = 1
        st.write("Fetching Activity data for Veridia Finishing...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanActivities/?projectId={st.session_state.workspaceid}&planId={st.session_state.veridia_finishing}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Finishing Activity data available (204)")
                    break
                if 'activityList' in data and data['activityList']:
                    all_finishing_activity_data.extend(data['activityList'])
                else:
                    all_finishing_activity_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_finishing_activity_data[-record_limit:])} Finishing Activity records (Total: {len(all_finishing_activity_data)})")
                if len(all_finishing_activity_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
            except Exception as e:
                st.error(f"❌ Error fetching Finishing Activity data: {str(e)}")
                break

        # Fetch Veridia Structure Activity data
        start_record = 1
        st.write("Fetching Activity data for Veridia Structure...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanActivities/?projectId={st.session_state.workspaceid}&planId={st.session_state.veridia_structure}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Structure Activity data available (204)")
                    break
                if 'activityList' in data and data['activityList']:
                    all_structure_activity_data.extend(data['activityList'])
                else:
                    all_structure_activity_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_structure_activity_data[-record_limit:]) } Structure Activity records (Total: {len(all_structure_activity_data)})")
                if len(all_structure_activity_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
            except Exception as e:
                st.error(f"❌ Error fetching Structure Activity data: {str(e)}")
                break

    finishing_activity_data = pd.DataFrame(all_finishing_activity_data)[['activityName', 'activitySeq', 'formTypeId']]
    structure_activity_data = pd.DataFrame(all_structure_activity_data)[['activityName', 'activitySeq', 'formTypeId']]

    st.write("VERIDIA FINISHING ACTIVITY DATA (activityName and activitySeq)")
    st.write(f"Total records: {len(finishing_activity_data)}")
    st.write(finishing_activity_data)
    st.write("VERIDIA STRUCTURE ACTIVITY DATA (activityName and activitySeq)")
    st.write(f"Total records: {len(structure_activity_data)}")
    st.write(structure_activity_data)

    return finishing_activity_data, structure_activity_data

# Fetch Location/Module Data with Async
async def Get_Location():
    record_limit = 1000
    headers = {
        'Cookie': f'ASessionID={st.session_state.sessionid}',
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    all_finishing_location_data = []
    all_structure_location_data = []

    async with aiohttp.ClientSession() as session:
        # Fetch Veridia Finishing Location/Module data
        start_record = 1
        total_records_fetched = 0
        st.write("Fetching Veridia Finishing Location/Module data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanLocation/?projectId={st.session_state.workspaceid}&planId={st.session_state.veridia_finishing}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Finishing Location data available (204)")
                    break
                if isinstance(data, list):
                    location_data = [{'qiLocationId': item.get('qiLocationId', ''), 'qiParentId': item.get('qiParentId', ''), 'name': item.get('name', '')} 
                                   for item in data if isinstance(item, dict)]
                    all_finishing_location_data.extend(location_data)
                    total_records_fetched = len(all_finishing_location_data)
                    st.write(f"Fetched {len(location_data)} Finishing Location records (Total: {total_records_fetched})")
                elif isinstance(data, dict) and 'locationList' in data and data['locationList']:
                    location_data = [{'qiLocationId': loc.get('qiLocationId', ''), 'qiParentId': loc.get('qiParentId', ''), 'name': loc.get('name', '')} 
                                   for loc in data['locationList']]
                    all_finishing_location_data.extend(location_data)
                    total_records_fetched = len(all_finishing_location_data)
                    st.write(f"Fetched {len(location_data)} Finishing Location records (Total: {total_records_fetched})")
                else:
                    st.warning(f"No 'locationList' in Finishing Location data or empty list.")
                    break
                if len(location_data) < record_limit:
                    break
                start_record += record_limit
            except Exception as e:
                st.error(f"❌ Error fetching Finishing Location data: {str(e)}")
                break

        # Fetch Veridia Structure Location/Module data
        start_record = 1
        total_records_fetched = 0
        st.write("Fetching Veridia Structure Location/Module data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanLocation/?projectId={st.session_state.workspaceid}&planId={st.session_state.veridia_structure}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Structure Location data available (204)")
                    break
                if isinstance(data, list):
                    location_data = [{'qiLocationId': item.get('qiLocationId', ''), 'qiParentId': item.get('qiParentId', ''), 'name': item.get('name', '')} 
                                   for item in data if isinstance(item, dict)]
                    all_structure_location_data.extend(location_data)
                    total_records_fetched = len(all_structure_location_data)
                    st.write(f"Fetched {len(location_data)} Structure Location records (Total: {total_records_fetched})")
                elif isinstance(data, dict) and 'locationList' in data and data['locationList']:
                    location_data = [{'qiLocationId': loc.get('qiLocationId', ''), 'qiParentId': loc.get('qiParentId', ''), 'name': loc.get('name', '')} 
                                   for loc in data['locationList']]
                    all_structure_location_data.extend(location_data)
                    total_records_fetched = len(all_structure_location_data)
                    st.write(f"Fetched {len(location_data)} Structure Location records (Total: {total_records_fetched})")
                else:
                    st.warning(f"No 'locationList' in Structure Location data or empty list.")
                    break
                if len(location_data) < record_limit:
                    break
                start_record += record_limit
            except Exception as e:
                st.error(f"❌ Error fetching Structure Location data: {str(e)}")
                break

    finishing_df = pd.DataFrame(all_finishing_location_data)
    structure_df = pd.DataFrame(all_structure_location_data)

    # Validate name field
    if 'name' in finishing_df.columns and finishing_df['name'].isna().all():
        st.error("❌ All 'name' values in Finishing Location data are missing or empty!")
    if 'name' in structure_df.columns and structure_df['name'].isna().all():
        st.error("❌ All 'name' values in Structure Location data are missing or empty!")

    st.write("VERIDIA FINISHING LOCATION/MODULE DATA")
    st.write(f"Total records: {len(finishing_df)}")
    st.write(finishing_df)
    st.write("VERIDIA STRUCTURE LOCATION/MODULE DATA")
    st.write(f"Total records: {len(structure_df)}")
    st.write(structure_df)

    st.session_state.finishing_location_data = finishing_df
    st.session_state.structure_location_data = structure_df

    return finishing_df, structure_df

# Process individual chunk (to be called by threads)
def process_chunk(chunk, chunk_idx, current_token, dataset_name, session, location_df):
    logger.info(f"Starting thread for {dataset_name} Chunk {chunk_idx + 1}")
    
    # Prepare data
    data_dict = chunk[['qiLocationId', 'full_path', 'activitySeq', 'activityName', 'CompletedCount']].to_dict(orient="records")
    logger.info(f"Chunk {chunk_idx + 1} for {dataset_name} contains {len(data_dict)} records.")

    # Enhanced prompt
    prompt = (
        f"Analyze the provided JSON data and extract ALL completed activities for ALL towers. Count each activity's completed instances and return a detailed summary in a table format.\n\n"
        f"Data: {json.dumps(data_dict)}\n\n"
        "Instructions:\n"
        "CRITICAL: Return ONLY the table format below. Do NOT include any code, explanations, introductions, or any text outside the specified format (e.g., do not include 'The actual counts may vary' or 'Please provide the solution'). "
        "1. Count ALL activities for ALL towers in the data using 'full_path' as the tower name.\n"
        "2. Use 'activitySeq' to order activities within each tower (ascending order).\n"
        "3. Sum the 'CompletedCount' values to calculate the actual count for each activity.\n"
        "4. Use 'activityName' for the activity names (e.g., 'Wall Conduting', 'Plumbing Works').\n"
        "5. Return the results in this exact table format, with activities grouped by tower:\n"
        "Summary of Completed Activities:\n"
        "Tower: [FULL TOWER PATH]\n"
        "   activitySeq    activityName            CompletedCount\n"
        "   [ACTUAL SEQ]  [ACTUAL ACTIVITY NAME]  [ACTUAL COUNT]\n"
        "   [ACTUAL SEQ]  [ACTUAL ACTIVITY NAME]  [ACTUAL COUNT]\n"
        "Tower: [FULL TOWER PATH]\n"
        "   activitySeq    activityName            CompletedCount\n"
        "   [ACTUAL SEQ]  [ACTUAL ACTIVITY NAME]  [ACTUAL COUNT]\n"
        "...\n"
        "Total Completed Activities: [GRAND TOTAL COUNT]\n\n"
        "CRITICAL: Return ONLY the table format above. Do NOT include any code, explanations, introductions, or any text outside the specified format (e.g., do not include 'The actual counts may vary' or 'Please provide the solution'). "
        "If the data is missing required fields ('activityName', 'activitySeq', 'CompletedCount'), return:\n"
        "Summary of Completed Activities:\n"
        "Total Completed Activities: 0\n\n"
        "Calculate the REAL counts from the data. Do NOT use example values."
    )
    
    payload = {
        "input": prompt,
        "parameters": {"decoding_method": "greedy", "max_new_tokens": 8100, "min_new_tokens": 0, "temperature": 0.01, "repetition_penalty": 1.05},
        "model_id": MODEL_ID,
        "project_id": PROJECT_ID
    }
    headers = {"Accept": "application/json", "Content-Type": "application/json", "Authorization": f"Bearer {current_token}"}

    max_attempts = 5
    attempt = 1
    success = False
    generated_text = None

    while attempt <= max_attempts:
        try:
            # Increased timeout from 1000 to 1500 seconds
            response = session.post(WATSONX_API_URL, headers=headers, json=payload, timeout=1500)
            if response.status_code == 200:
                result = response.json()
                generated_text = result.get("results", [{}])[0].get("generated_text", "").strip()
                if not generated_text:
                    logger.warning(f"Empty response from WatsonX for {dataset_name} Chunk {chunk_idx + 1}.")
                if "Summary of Completed Activities:" not in generated_text or "Tower:" not in generated_text:
                    logger.warning(f"Invalid WatsonX response format for {dataset_name} Chunk {chunk_idx + 1}. Using local formatting.")
                    generated_text = format_chunk_locally(chunk, chunk_idx, len(chunk), dataset_name)
                success = True
                break
            elif response.status_code == 401:
                logger.warning(f"401 Unauthorized error on attempt {attempt}/{max_attempts}. Token may need refresh.")
                # Try to refresh token and retry
                current_token, _ = get_access_token(API_KEY)
                if current_token:
                    headers["Authorization"] = f"Bearer {current_token}"
                attempt += 1
                time.sleep(5)  # Wait a bit longer before retry
            else:
                error_msg = response.json().get('message', response.text)
                logger.error(f"WatsonX API error for {dataset_name} Chunk {chunk_idx + 1}: {response.status_code} - {error_msg}")
                attempt += 1
                time.sleep(5 * attempt)  # Progressive backoff
        except requests.exceptions.Timeout as e:
            logger.warning(f"Timeout on attempt {attempt}/{max_attempts}: {str(e)}")
            if attempt == max_attempts:
                logger.warning(f"WatsonX API timed out after {max_attempts} attempts for {dataset_name} Chunk {chunk_idx + 1}. Using local formatting.")
                generated_text = format_chunk_locally(chunk, chunk_idx, len(chunk), dataset_name)
                success = True
                break
            time.sleep(5 * attempt)  # Progressive backoff
            attempt += 1
        except Exception as e:
            logger.error(f"Exception during WatsonX call for {dataset_name} Chunk {chunk_idx + 1}: {str(e)}")
            attempt += 1
            time.sleep(5 * attempt)  # Progressive backoff

    if not success:
        logger.error(f"Failed to process chunk {chunk_idx + 1} for {dataset_name} after {max_attempts} attempts. Using local formatting.")
        generated_text = format_chunk_locally(chunk, chunk_idx, len(chunk), dataset_name)

    logger.info(f"Completed thread for {dataset_name} Chunk {chunk_idx + 1}")
    return generated_text, chunk_idx

# Modified process_with_watsonx function with improved error handling
def process_with_watsonx(analysis_df, total, dataset_name, chunk_size=1000, max_workers=9):
    if analysis_df.empty:
        st.warning(f"No completed activities found for {dataset_name}.")
        return None

    unique_activities = analysis_df['activityName'].unique()
    logger.info(f"Unique activities in {dataset_name} dataset: {list(unique_activities)}")
    logger.info(f"Total records in {dataset_name} dataset: {len(analysis_df)}")

    st.write(f"Saved Veridia {dataset_name} data to veridia_{dataset_name.lower()}_data.json")
    chunks = [analysis_df[i:i + chunk_size] for i in range(0, len(analysis_df), chunk_size)]

    parsed_data = {}
    total_activities_count = 0

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504, 408], allowed_methods=["POST"])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    current_token, current_expiration = get_access_token(API_KEY)
    if not current_token:
        st.error(f"❌ Failed to initialize WatsonX access token for {dataset_name}.")
        return None

    progress_bar = st.progress(0)
    status_text = st.empty()

    loop_timeout = 3600
    loop_start_time = time.time()

    # Get location data
    location_df = st.session_state.finishing_location_data if dataset_name.lower() == "finishing" else st.session_state.structure_location_data

    # Precompute full paths
    parent_child_dict = dict(zip(location_df['qiLocationId'], location_df['qiParentId']))
    name_dict = dict(zip(location_df['qiLocationId'], location_df['name']))

    def get_full_path(location_id):
        path = []
        current_id = location_id
        max_depth = 10
        depth = 0
        while current_id and depth < max_depth:
            if current_id not in parent_child_dict or current_id not in name_dict:
                break
            parent_id = parent_child_dict.get(current_id)
            name = name_dict.get(current_id, "Unknown")
            if not parent_id:
                if name != "Quality":
                    path.append(name)
                    path.append("Quality")
                else:
                    path.append(name)
                break
            path.append(name)
            current_id = parent_id
            depth += 1
        return '/'.join(reversed(path)) if path else "Unknown"

    # Process chunks using ThreadPoolExecutor
    chunk_results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_chunk = {
            executor.submit(process_chunk, chunk, idx, current_token, dataset_name, session, location_df): idx 
            for idx, chunk in enumerate(chunks)
        }

        completed_chunks = 0
        for future in as_completed(future_to_chunk):
            if time.time() - loop_start_time > loop_timeout:
                st.error(f"❌ Processing for {dataset_name} timed out after {loop_timeout} seconds.")
                break

            chunk_idx = future_to_chunk[future]
            try:
                generated_text, idx = future.result()
                chunk_results[idx] = generated_text
                completed_chunks += 1
                progress_percent = completed_chunks / len(chunks)
                progress_bar.progress(progress_percent)
                status_text.text(f"Processed chunk {completed_chunks} of {len(chunks)} ({progress_percent:.1%} complete)")
            except Exception as e:
                logger.error(f"Error processing chunk {chunk_idx + 1} for {dataset_name}: {str(e)}")
                st.error(f"❌ Error processing chunk {chunk_idx + 1}: {str(e)}")

    # Parse results with improved error handling
    for chunk_idx in sorted(chunk_results.keys()):
        generated_text = chunk_results[chunk_idx]
        if not generated_text:
            continue

        current_tower = None
        tower_activities = []
        lines = generated_text.split("\n")
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # More robust check for Tower line
            if line.startswith("Tower:"):
                try:
                    # Try to safely get the tower name
                    tower_parts = line.split("Tower:", 1)
                    if len(tower_parts) > 1:
                        if current_tower and tower_activities:
                            if current_tower not in parsed_data:
                                parsed_data[current_tower] = []
                            parsed_data[current_tower].extend(tower_activities)
                            total_activities_count += sum(activity['completedCount'] for activity in tower_activities)
                            tower_activities = []
                        current_tower = tower_parts[1].strip()
                except Exception as e:
                    logger.warning(f"Error parsing Tower line: {line}, error: {str(e)}")
                    # Use a default tower name if parsing fails
                    if not current_tower:
                        current_tower = f"Unknown Tower {chunk_idx}"
                    
            elif line.startswith("Total Completed Activities:"):
                continue
            elif not line.startswith("Summary of Completed Activities:") and not line.strip().startswith("activitySeq"):
                # Try to parse activity line with more robust approach
                try:
                    parts = re.split(r'\s{2,}', line.strip())
                    if len(parts) >= 3:
                        seq = parts[0].strip()
                        # The middle part(s) is the activity name
                        activity_name = ' '.join(parts[1:-1]).strip()
                        # The last part is the count
                        count_str = parts[-1].strip()
                        # Extract digits from the count string
                        count_match = re.search(r'\d+', count_str)
                        if count_match:
                            count = int(count_match.group())
                            if current_tower:  # Make sure we have a tower before adding activities
                                tower_activities.append({
                                    "activitySeq": seq,
                                    "activityName": activity_name,
                                    "completedCount": count
                                })
                    else:
                        # Alternative regex match as fallback
                        match = re.match(r'^\s*(\S+)\s+(.+?)\s+(\d+)$', line)
                        if match and current_tower:
                            seq = match.group(1).strip()
                            activity_name = match.group(2).strip()
                            count = int(match.group(3).strip())
                            tower_activities.append({
                                "activitySeq": seq,
                                "activityName": activity_name,
                                "completedCount": count
                            })
                except (ValueError, IndexError) as e:
                    logger.warning(f"Skipping malformed activity line: {line}, error: {str(e)}")

        # Add the last tower's activities if any
        if current_tower and tower_activities:
            if current_tower not in parsed_data:
                parsed_data[current_tower] = []
            parsed_data[current_tower].extend(tower_activities)
            total_activities_count += sum(activity['completedCount'] for activity in tower_activities)

    # Format combined output
    combined_output = "Summary of Completed Activities:\n"
    sorted_towers = sorted(parsed_data.keys())
    total_activities_count = 0
    
    for tower_name in sorted_towers:
        combined_output += f"Tower: {tower_name}\n"
        combined_output += "   activitySeq    activityName            CompletedCount\n"
        activities = sorted(parsed_data[tower_name], key=lambda x: x.get("activitySeq", "0"))
        activity_dict = {}
        for activity in activities:
            key = (activity.get("activitySeq", "0"), activity.get("activityName", "Unknown"))
            activity_dict[key] = activity_dict.get(key, 0) + activity.get("completedCount", 0)
        for (seq, name), count in activity_dict.items():
            combined_output += f"   {seq:<15} {name:<30} {count}\n"
            total_activities_count += count
    
    combined_output += f"Total Completed Activities: {total_activities_count}"
    
    st.text_area(f"Final {dataset_name} Analysis", combined_output, height=400)
    return combined_output

# Local formatting function as a fallback if WatsonX fails
def format_chunk_locally(chunk, chunk_idx, chunk_size, dataset_name):
    start_idx = chunk_idx * chunk_size
    
    # Group data by full_path and activity name to count completed activities
    grouped_data = chunk.groupby(['full_path', 'activitySeq', 'activityName']).agg({
        'CompletedCount': 'sum',
        'qiLocationId': lambda x: list(x)
    }).reset_index()
    
    # Create structure based on tower names in the data
    towers_data = {}
    
    for _, row in grouped_data.iterrows():
        tower_name = row['full_path']
        
        if tower_name not in towers_data:
            towers_data[tower_name] = []
            
        activity = {
            "activitySeq": row['activitySeq'],
            "activityName": row['activityName'],
            "completedCount": int(row['CompletedCount']),
            "completedActivityIds": [str(id) for id in row['qiLocationId']][:int(row['CompletedCount'])]
        }
        
        towers_data[tower_name].append(activity)
    
    # Format results as a table to match the prompt's format
    output = "Summary of Completed Activities:\n"
    total_activities = 0
    
    for tower_name, activities in towers_data.items():
        output += f"Tower: {tower_name}\n"
        output += "   activitySeq    activityName            CompletedCount\n"
        sorted_activities = sorted(activities, key=lambda x: x.get("activitySeq", "0"))
        for activity in sorted_activities:
            output += f"   {activity['activitySeq']:<15} {activity['activityName']:<30} {activity['completedCount']}\n"
        total_activities += sum(activity['completedCount'] for activity in activities)
    
    output += f"Total Completed Activities: {total_activities}"
    return output

def process_data(df, activity_df, location_df, dataset_name):
    completed = df[df['statusName'] == 'Completed']
    if completed.empty:
        logger.warning(f"No completed activities found in {dataset_name} data.")
        return pd.DataFrame(), 0

    # Optimize merging
    completed = completed.merge(location_df[['qiLocationId', 'name']], on='qiLocationId', how='left')
    completed = completed.merge(activity_df[['activitySeq', 'activityName']], on='activitySeq', how='left')

    if 'qiActivityId' not in completed.columns:
        completed['qiActivityId'] = completed['qiLocationId'].astype(str) + '$$' + completed['activitySeq'].astype(str)

    if completed['name'].isna().all():
        logger.error(f"All 'name' values are missing in {dataset_name} data after merge!")
        st.error(f"❌ All 'name' values are missing in {dataset_name} data after merge! Check location data.")
        completed['name'] = 'Unknown'
    else:
        completed['name'] = completed['name'].fillna('Unknown')

    completed['activityName'] = completed['activityName'].fillna('Unknown')

    parent_child_dict = dict(zip(location_df['qiLocationId'], location_df['qiParentId']))
    name_dict = dict(zip(location_df['qiLocationId'], location_df['name']))

    def get_full_path(location_id):
        path = []
        current_id = location_id
        max_depth = 10
        depth = 0
        
        while current_id and depth < max_depth:
            if current_id not in parent_child_dict or current_id not in name_dict:
                logger.warning(f"Location ID {current_id} not found in parent_child_dict or name_dict. Path so far: {path}")
                break
            
            parent_id = parent_child_dict.get(current_id)
            name = name_dict.get(current_id, "Unknown")
            
            if not parent_id:
                if name != "Quality":
                    path.append(name)
                    path.append("Quality")
                else:
                    path.append(name)
                break
            
            path.append(name)
            current_id = parent_id
            depth += 1
        
        if depth >= max_depth:
            logger.warning(f"Max depth reached while computing path for location_id {location_id}. Possible circular reference. Path: {path}")
        
        if not path:
            logger.warning(f"No path constructed for location_id {location_id}. Using 'Unknown'.")
            return "Unknown"
        
        full_path = '/'.join(reversed(path))
        logger.debug(f"Full path for location_id {location_id}: {full_path}")
        return full_path

    completed['full_path'] = completed['qiLocationId'].apply(get_full_path)

    analysis = completed.groupby(['qiLocationId', 'full_path', 'activitySeq', 'activityName']).agg({
        'qiActivityId': list,
        'statusName': 'count'
    }).reset_index()
    
    analysis = analysis.rename(columns={'statusName': 'CompletedCount'})
    analysis = analysis.sort_values(by=['qiLocationId', 'activitySeq'], ascending=True)
    total_completed = analysis['CompletedCount'].sum()

    return analysis, total_completed



def AnalyzeStatusWithWatsonX(email=None, password=None):
    start_time = time.time()

    if 'sessionid' not in st.session_state:
        st.error("❌ Please log in first!")
        return

    required_data = ['veridiafinishing', 'veridiastructure', 'finishing_activity_data', 'structure_activity_data', 'finishing_location_data', 'structure_location_data']
    for data_key in required_data:
        if data_key not in st.session_state:
            st.error(f"❌ Please fetch required data first! Missing: {data_key}")
            return

    token, token_expiration = get_access_token(API_KEY)
    if not token:
        st.error("❌ Failed to generate WatsonX access token.")
        return

    finishing_data = st.session_state.veridiafinishing
    structure_data = st.session_state.veridiastructure
    finishing_activity = st.session_state.finishing_activity_data
    structure_activity = st.session_state.structure_activity_data
    finishing_locations = st.session_state.finishing_location_data
    structure_locations = st.session_state.structure_location_data

    for df, name in [(finishing_data, "Finishing"), (structure_data, "Structure")]:
        if 'statusName' not in df.columns:
            st.error(f"❌ statusName column not found in {name} data!")
            return
        if 'qiLocationId' not in df.columns:
            st.error(f"❌ qiLocationId column not found in {name} data!")
            return
        if 'activitySeq' not in df.columns:
            st.error(f"❌ activitySeq column not found in {name} data!")
            return

    for df, name in [(finishing_locations, "Finishing Location"), (structure_locations, "Structure Location")]:
        if 'qiLocationId' not in df.columns or 'name' not in df.columns:
            st.error(f"❌ qiLocationId or name column not found in {name} data!")
            return

    for df, name in [(finishing_activity, "Finishing Activity"), (structure_activity, "Structure Activity")]:
        if 'activitySeq' not in df.columns or 'activityName' not in df.columns:
            st.error(f"❌ activitySeq or activityName column not found in {name} data!")
            return

    finishing_analysis, finishing_total = process_data(finishing_data, finishing_activity, finishing_locations, "Finishing")
    structure_analysis, structure_total = process_data(structure_data, structure_activity, structure_locations, "Structure")

    st.write("### Veridia Finishing Quality Analysis (Completed Activities):")
    st.write("**Full Output (Finishing):**")
    finishing_output = process_with_watsonx(finishing_analysis, finishing_total, "Finishing")
    if finishing_output:
        st.text(finishing_output)

    st.write("### Veridia Structure Quality Analysis (Completed Activities):")
    st.write("**Full Output (Structure):**")
    structure_output = process_with_watsonx(structure_analysis, structure_total, "Structure")
    if structure_output:
        st.text(structure_output)

    end_time = time.time()
    st.write(f"Total execution time: {end_time - start_time:.2f} seconds")

# Streamlit UI
st.title("Asite Check List Reporter")

st.sidebar.title("🔒 Asite Initialization")
email = st.sidebar.text_input("Email", "impwatson@gadieltechnologies.com", key="email_input")
password = st.sidebar.text_input("Password", "Srihari@790$", type="password", key="password_input")

if st.sidebar.button("Initialize All Data"):
    # Start the initialization process
    st.sidebar.write("Starting initialization process...")

    # Step 1: Login
    if not email or not password:
        st.sidebar.error("Please provide both email and password!")
    else:
        try:
            st.sidebar.write("Logging in...")
            login_to_asite(email, password)
            st.sidebar.success("Login successful!")
        except Exception as e:
            st.sidebar.error(f"Login failed: {str(e)}")
            st.stop()

    # Step 2: Get Workspace ID
    try:
        st.sidebar.write("Fetching Workspace ID...")
        GetWorkspaceID()
        st.sidebar.success("Workspace ID fetched successfully!")
    except Exception as e:
        st.sidebar.error(f"Failed to fetch Workspace ID: {str(e)}")
        st.stop()

    # Step 3: Get Project IDs
    try:
        st.sidebar.write("Fetching Project IDs...")
        st.write(st.session_state.sessionid)
        GetProjectId()
        st.sidebar.success("Project IDs fetched successfully!")
    except Exception as e:
        st.sidebar.error(f"Failed to fetch Project IDs: {str(e)}")
        st.stop()

    # Step 4: Get All Data (asynchronous)
    try:
        st.sidebar.write("Fetching All Data...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        veridiafinishing, veridiastructure = loop.run_until_complete(GetAllDatas())
        st.session_state.veridiafinishing = veridiafinishing
        st.session_state.veridiastructure = veridiastructure
        st.sidebar.success("All Data fetched successfully!")
    except Exception as e:
        st.sidebar.error(f"Failed to fetch All Data: {str(e)}")
        st.stop()

    # Step 5: Get Activity Data (asynchronous)
    try:
        st.sidebar.write("Fetching Activity Data...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        finishing_activity_data, structure_activity_data = loop.run_until_complete(Get_Activity())
        st.session_state.finishing_activity_data = finishing_activity_data
        st.session_state.structure_activity_data = structure_activity_data
        st.sidebar.success("Activity Data fetched successfully!")
    except Exception as e:
        st.sidebar.error(f"Failed to fetch Activity Data: {str(e)}")
        st.stop()

    # Step 6: Get Location/Module Data (asynchronous)
    try:
        st.sidebar.write("Fetching Location/Module Data...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        finishing_location_data, structure_location_data = loop.run_until_complete(Get_Location())
        st.session_state.finishing_location_data = finishing_location_data
        st.session_state.structure_location_data = structure_location_data
        st.sidebar.success("Location/Module Data fetched successfully!")
    except Exception as e:
        st.sidebar.error(f"Failed to fetch Location/Module Data: {str(e)}")
        st.stop()

    st.sidebar.write("Initialization process completed!")

st.sidebar.title("📊 Status Analysis")
if st.sidebar.button("Analyze Completed Status"):
    AnalyzeStatusWithWatsonX()
