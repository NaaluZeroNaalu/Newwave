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
import os
from dotenv import load_dotenv
import io
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import BytesIO
import base64
from typing import Tuple, Dict, Any 
# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# WatsonX configuration
WATSONX_API_URL = os.getenv("WATSONX_API_URL")
MODEL_ID = os.getenv("MODEL_ID")
PROJECT_ID = os.getenv("PROJECT_ID")
API_KEY = os.getenv("API_KEY")

# Check environment variables
if not all([API_KEY, WATSONX_API_URL, MODEL_ID, PROJECT_ID]):
    st.error("❌ Required environment variables (API_KEY, WATSONX_API_URL, MODEL_ID, PROJECT_ID) missing!")
    logger.error("Missing one or more required environment variables")
    st.stop()

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# API Endpoints
LOGIN_URL = "https://dms.asite.com/apilogin/"
SEARCH_URL = "https://adoddleak.asite.com/commonapi/formsearchapi/search"
IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"

# Function to generate access token
def get_access_token(API_KEY):
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    data = {"grant_type": "urn:ibm:params:oauth:grant-type:apikey", "apikey": API_KEY}
    try:
        response = requests.post(IAM_TOKEN_URL, headers=headers, data=data, verify=certifi.where(), timeout=50)
        if response.status_code == 200:
            token_info = response.json()
            logger.info("Access token generated successfully")
            return token_info['access_token']
        else:
            logger.error(f"Failed to get access token: {response.status_code} - {response.text}")
            st.error(f"❌ Failed to get access token: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Exception getting access token: {str(e)}")
        st.error(f"❌ Error getting access token: {str(e)}")
        return None

# Login Function
def login_to_asite(email, password):
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    payload = {"emailId": email, "password": password}
    response = requests.post(LOGIN_URL, headers=headers, data=payload, verify=certifi.where(), timeout=50)
    if response.status_code == 200:
        try:
            session_id = response.json().get("UserProfile", {}).get("Sessionid")
            logger.info(f"Login successful, Session ID: {session_id}")
            return session_id
        except json.JSONDecodeError:
            logger.error("JSONDecodeError during login")
            st.error("❌ Failed to parse login response")
            return None
    logger.error(f"Login failed: {response.status_code}")
    st.error(f"❌ Login failed: {response.status_code}")
    return None

# Fetch Data Function
def fetch_project_data(session_id, project_name, form_name, record_limit=1000):
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded", "Cookie": f"ASessionID={session_id}"}
    all_data = []
    start_record = 1
    total_records = None

    with st.spinner("Fetching data from Asite..."):
        while True:
            search_criteria = {"criteria": [{"field": "ProjectName", "operator": 1, "values": [project_name]}, {"field": "FormName", "operator": 1, "values": [form_name]}], "recordStart": start_record, "recordLimit": record_limit}
            search_criteria_str = json.dumps(search_criteria)
            encoded_payload = f"searchCriteria={urllib.parse.quote(search_criteria_str)}"
            response = requests.post(SEARCH_URL, headers=headers, data=encoded_payload, verify=certifi.where(), timeout=50)

            try:
                response_json = response.json()
                if total_records is None:
                    total_records = response_json.get("responseHeader", {}).get("results-total", 0)
                all_data.extend(response_json.get("FormList", {}).get("Form", []))
                st.info(f"🔄 Fetched {len(all_data)} / {total_records} records")
                if start_record + record_limit - 1 >= total_records:
                    break
                start_record += record_limit
            except Exception as e:
                logger.error(f"Error fetching data: {str(e)}")
                st.error(f"❌ Error fetching data: {str(e)}")
                break

    return {"responseHeader": {"results": len(all_data), "total_results": total_records}}, all_data, encoded_payload

# Process JSON Data
def process_json_data(json_data):
    data = []
    for item in json_data:
        form_details = item.get('FormDetails', {})
        created_date = form_details.get('FormCreationDate', None)
        expected_close_date = form_details.get('UpdateDate', None)
        form_status = form_details.get('FormStatus', None)
        
        discipline = None
        description = None
        custom_fields = form_details.get('CustomFields', {}).get('CustomField', [])
        for field in custom_fields:
            if field.get('FieldName') == 'CFID_DD_DISC':
                discipline = field.get('FieldValue', None)
            elif field.get('FieldName') == 'CFID_RTA_DES':
                description = BeautifulSoup(field.get('FieldValue', None) or '', "html.parser").get_text()

        days_diff = None
        if created_date and expected_close_date:
            try:
                created_date_obj = datetime.strptime(created_date.split('#')[0], "%d-%b-%Y")
                expected_close_date_obj = datetime.strptime(expected_close_date.split('#')[0], "%d-%b-%Y")
                days_diff = (expected_close_date_obj - created_date_obj).days
            except Exception as e:
                logger.error(f"Error calculating days difference: {str(e)}")
                days_diff = None

        data.append([days_diff, created_date, expected_close_date, description, form_status, discipline])

    df = pd.DataFrame(data, columns=['Days', 'Created Date (WET)', 'Expected Close Date (WET)', 'Description', 'Status', 'Discipline'])
    df['Created Date (WET)'] = pd.to_datetime(df['Created Date (WET)'].str.split('#').str[0], format="%d-%b-%Y", errors='coerce')
    df['Expected Close Date (WET)'] = pd.to_datetime(df['Expected Close Date (WET)'].str.split('#').str[0], format="%d-%b-%Y", errors='coerce')
    logger.debug(f"DataFrame columns after processing: {df.columns.tolist()}")  # Debug column names
    if df.empty:
        logger.warning("DataFrame is empty after processing")
        st.warning("⚠️ No data processed. Check the API response.")
    return df

# Generate NCR Report

@st.cache_data
def generate_ncr_report(df: pd.DataFrame, report_type: str, start_date=None, end_date=None, Until_Date=None) -> Tuple[Dict[str, Any], str]:
    with st.spinner(f"Generating {report_type} NCR Report..."):
        # Ensure DataFrame has no NaT values in critical columns
        df = df.copy()
        df = df[df['Created Date (WET)'].notna()]  # Drop rows where 'Created Date (WET)' is NaT
        
        # Define standard blocks
        standard_blocks = [
            "Block 1 (B1) Banquet Hall",
            "Block 5 (B5) Admin + Member Lounge + Creche + AV Room + Surveillance Room + Toilets",
            "Block 6 (B6) Toilets",
            "Block 7 (B7) Indoor Sports",
            "Block 9 (B9) Spa & Saloon",
            "Block 8 (B8) Squash Court",
            "Block 2 & 3 (B2 & B3) Cafe & Bar",
            "Block 4 (B4) Indoor Swimming Pool Changing Room & Toilets",
            "Block 11 (B11) Guest House",
            "Block 10 (B10) Gym"
        ]
        
        if report_type == "Closed":
            # Convert start_date and end_date to datetime
            try:
                start_date = pd.to_datetime(start_date) if start_date else df['Created Date (WET)'].min()
                end_date = pd.to_datetime(end_date) if end_date else df['Expected Close Date (WET)'].max()
            except ValueError as e:
                logging.error(f"Invalid date range: {str(e)}")
                st.error(f"❌ Invalid date range: {str(e)}")
                return {"error": "Invalid date range"}, ""
            
            # Drop rows where 'Expected Close Date (WET)' is NaT
            df = df[df['Expected Close Date (WET)'].notna()]
            
            filtered_df = df[
                (df['Status'] == 'Closed') &
                (df['Created Date (WET)'] >= start_date) &
                (df['Expected Close Date (WET)'] <= end_date) &
                (df['Days'] > 21)
            ].copy()
        else:  # Open report
            if Until_Date is None:
                logging.error("Open Until Date is required for Open NCR Report")
                st.error("❌ Open Until Date is required for Open NCR Report")
                return {"error": "Open Until Date is required"}, ""
            
            try:
                today = pd.to_datetime(Until_Date)
            except ValueError as e:
                logging.error(f"Invalid Open Until Date: {str(e)}")
                st.error(f"❌ Invalid Open Until Date: {str(e)}")
                return {"error": "Invalid Open Until Date"}, ""
                
            filtered_df = df[
                (df['Status'] == 'Open') &
                (df['Created Date (WET)'].notna())
            ].copy()
            filtered_df.loc[:, 'Days_From_Today'] = (today - pd.to_datetime(filtered_df['Created Date (WET)'])).dt.days
            filtered_df = filtered_df[filtered_df['Days_From_Today'] > 21].copy()

        if filtered_df.empty:
            return {"error": f"No {report_type} records found with duration > 21 days"}, ""

        filtered_df.loc[:, 'Created Date (WET)'] = filtered_df['Created Date (WET)'].astype(str)
        filtered_df.loc[:, 'Expected Close Date (WET)'] = filtered_df['Expected Close Date (WET)'].astype(str)

        processed_data = filtered_df.to_dict(orient="records")
        
        cleaned_data = []
        unique_records = set()  # To track unique records

        for record in processed_data:
            cleaned_record = {
                "Description": str(record.get("Description", "")),
                "Discipline": str(record.get("Discipline", "")),
                "Created Date (WET)": str(record.get("Created Date (WET)", "")),
                "Expected Close Date (WET)": str(record.get("Expected Close Date (WET)", "")),
                "Status": str(record.get("Status", "")),
                "Days": record.get("Days", 0),
                "Block": "Block 1 (B1) Banquet Hall"  # Default block
            }
            if report_type == "Open":
                cleaned_record["Days_From_Today"] = record.get("Days_From_Today", 0)

            description = cleaned_record["Description"].lower().strip()
            location = str(record.get("Location", "")).lower()
            
            # Skip duplicates
            if description in unique_records:
                continue
            unique_records.add(description)
            
            # Initialize Discipline_Category
            discipline = cleaned_record["Discipline"].strip().lower()
            if discipline == "none" or not discipline:
                logging.debug(f"Skipping record with invalid discipline: {discipline}")
                continue
            elif "structure" in discipline or "sw" in discipline:
                cleaned_record["Discipline_Category"] = "SW"
            elif "civil" in discipline or "finishing" in discipline or "fw" in discipline:
                cleaned_record["Discipline_Category"] = "FW"
            elif "hse" in discipline:
                cleaned_record["Discipline_Category"] = "MEP"  # Map HSE to MEP
            else:
                cleaned_record["Discipline_Category"] = "MEP"

            # Block categorization based on Location or Description
            assigned_block = None
            for block in standard_blocks:
                block_short = block.split("(")[1].split(")")[0].lower()  # e.g., "b1", "b5"
                if block_short in location or block_short in description:
                    cleaned_record["Block"] = block
                    assigned_block = block
                    break
            if not assigned_block:
                if "grid" in description or "city club" in description:
                    cleaned_record["Block"] = "Block 1 (B1) Banquet Hall"  # Default for structural issues
                logging.debug(f"Assigned block {cleaned_record['Block']} for description: {description}")
            
            cleaned_data.append(cleaned_record)

        # Deduplicate dictionaries
        cleaned_data = [dict(t) for t in {tuple(sorted(d.items())) for d in cleaned_data}]

        if not cleaned_data:
            return {report_type: {"Sites": {}, "Grand_Total": 0}}, ""

        # Local count for validation
        local_result = {report_type: {"Sites": {}, "Grand_Total": 0}}
        for record in cleaned_data:
            block = record["Block"]
            discipline = record["Discipline_Category"]
            if block not in local_result[report_type]["Sites"]:
                local_result[report_type]["Sites"][block] = {
                    "SW": 0,
                    "FW": 0,
                    "MEP": 0,
                    "Total": 0
                }
            local_result[report_type]["Sites"][block][discipline] += 1
            local_result[report_type]["Sites"][block]["Total"] += 1
            local_result[report_type]["Grand_Total"] += 1

        # WatsonX API call
        access_token = get_access_token(API_KEY)
        if not access_token:
            return {"error": "Failed to obtain access token"}, ""

        chunk_size = 3
        all_results = {report_type: {"Sites": {}, "Grand_Total": 0}}

        for i in range(0, len(cleaned_data), chunk_size):
            chunk = cleaned_data[i:i + chunk_size]
            st.write(f"Processing chunk {i // chunk_size + 1}: Records {i} to {min(i + chunk_size, len(cleaned_data))}")
            logging.info(f"Data sent to WatsonX for {report_type} chunk {i // chunk_size + 1}: {json.dumps(chunk, indent=2)}")

            prompt = (
                "IMPORTANT: RETURN ONLY A SINGLE VALID JSON OBJECT WITH THE EXACT FIELDS SPECIFIED BELOW. "
                "DO NOT GENERATE ANY CODE (e.g., Python, JavaScript). "
                "DO NOT INCLUDE ANY TEXT, EXPLANATIONS, OR MULTIPLE RESPONSES OUTSIDE THE JSON OBJECT. "
                "DO NOT WRAP THE JSON IN CODE BLOCKS (e.g., ```json). "
                "RETURN THE JSON OBJECT DIRECTLY.\n\n"
                f"Task: For each record in the provided data, group by 'Block' and collect 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', and 'Discipline' into arrays. "
                f"Extract modules from the 'Description' field for each record. Modules are identifiers like 'M1', 'M2', 'Common', etc., found in patterns such as 'Module-1', 'Module-3 & 4', 'M-1', 'Module1', 'Module 1 to 3', or 'Common' for common areas. "
                f"For module ranges (e.g., 'Module 1 to 3'), include all modules in the range (e.g., ['M1', 'M2', 'M3']). For lists or pairs (e.g., 'Module-3 & 4', 'Module-3&4', 'Module-1,2'), include each module (e.g., ['M3', 'M4'], ['M1', 'M2']). "
                f"If no modules are specified, the description mentions 'common area', contains structural elements (e.g., 'shear wall', 'column', 'grid'), or lacks module references, use ['Common']. "
                f"Count the records by 'Discipline_Category' ('SW', 'FW', 'MEP'), calculate the 'Total' for each 'Block', and count occurrences of each module within 'Modules' (e.g., M1, M2) within each 'Block'. "
                f"Finally, calculate the 'Grand_Total' as the total number of records processed.\n"
                f"Condition: Only include records where:\n"
                f"- Status is '{report_type}'.\n"
                f"- For report_type == 'Closed': Days > 21 (pre-calculated planned duration).\n"
                f"- For report_type == 'Open': Days_From_Today > 21 (already calculated in the data).\n"
                f"Use 'Block' values from the following list: {', '.join(standard_blocks)}. "
                f"Use 'Discipline_Category' values ('SW', 'FW', 'MEP'), and extracted 'Modules' values. Count each record exactly once.\n\n"
                "REQUIRED OUTPUT FORMAT (ONLY THESE FIELDS):\n"
                "{\n"
                f'  "{report_type}": {{\n'
                '    "Sites": {\n'
                '      "Block_Name": {\n'
                '        "Descriptions": ["description1", "description2"],\n'
                '        "Created Date (WET)": ["date1", "date2"],\n'
                '        "Expected Close Date (WET)": ["date1", "date2"],\n'
                '        "Status": ["status1", "status2"],\n'
                '        "Discipline": ["discipline1", "discipline2"],\n'
                '        "Modules": [["module1a", "module1b"], ["module2"]],\n'
                '        "SW": number,\n'
                '        "FW": number,\n'
                '        "MEP": number,\n'
                '        "Total": number,\n'
                '        "ModulesCount": {"module1": count1, "module2": count2}\n'
                '      }\n'
                '    },\n'
                '    "Grand_Total": number\n'
                '  }\n'
                '}\n\n'
                f"Data: {json.dumps(chunk)}\n"
                f"Return the result as a single JSON object with only the specified fields."
            )

            payload = {
                "input": prompt,
                "parameters": {
                    "decoding_method": "greedy",
                    "max_new_tokens": 8100,
                    "min_new_tokens": 0,
                    "temperature": 0.0
                },
                "model_id": MODEL_ID,
                "project_id": PROJECT_ID
            }
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }

            # Retry logic for WatsonX API
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["POST"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            http = requests.Session()
            http.mount("https://", adapter)

            try:
                response = http.post(WATSONX_API_URL, headers=headers, json=payload, verify=certifi.where(), timeout=600)
                logging.info(f"WatsonX API response status code: {response.status_code}")
                st.write(f"Debug - Response status code: {response.status_code}")

                if response.status_code == 200:
                    api_result = response.json()
                    generated_text = api_result.get("results", [{}])[0].get("generated_text", "").strip()
                    short_text = generated_text[:200] + "..." if len(generated_text) > 200 else generated_text
                    st.write(f"Debug - Raw response preview: {short_text}")
                    logging.debug(f"Parsed generated text: {generated_text}")

                    parsed_json = clean_and_parse_json(generated_text)
                    if parsed_json and report_type in parsed_json:
                        chunk_result = parsed_json[report_type]
                        chunk_grand_total = chunk_result.get("Grand_Total", 0)
                        expected_total = len(chunk)
                        if chunk_grand_total == expected_total:
                            for block, data in chunk_result["Sites"].items():
                                if block not in all_results[report_type]["Sites"]:
                                    all_results[report_type]["Sites"][block] = {
                                        "Descriptions": [],
                                        "Created Date (WET)": [],
                                        "Expected Close Date (WET)": [],
                                        "Status": [],
                                        "Discipline": [],
                                        "Modules": [],
                                        "SW": 0,
                                        "FW": 0,
                                        "MEP": 0,
                                        "Total": 0,
                                        "ModulesCount": {}
                                    }
                                all_results[report_type]["Sites"][block]["Descriptions"].extend(data["Descriptions"])
                                all_results[report_type]["Sites"][block]["Created Date (WET)"].extend(data["Created Date (WET)"])
                                all_results[report_type]["Sites"][block]["Expected Close Date (WET)"].extend(data["Expected Close Date (WET)"])
                                all_results[report_type]["Sites"][block]["Status"].extend(data["Status"])
                                all_results[report_type]["Sites"][block]["Discipline"].extend(data["Discipline"])
                                all_results[report_type]["Sites"][block]["Modules"].extend(data["Modules"])
                                all_results[report_type]["Sites"][block]["SW"] += data["SW"]
                                all_results[report_type]["Sites"][block]["FW"] += data["FW"]
                                all_results[report_type]["Sites"][block]["MEP"] += data["MEP"]
                                all_results[report_type]["Sites"][block]["Total"] += data["Total"]
                                for module, count in data["ModulesCount"].items():
                                    all_results[report_type]["Sites"][block]["ModulesCount"][module] = all_results[report_type]["Sites"][block]["ModulesCount"].get(module, 0) + count
                            all_results[report_type]["Grand_Total"] += chunk_grand_total
                            st.write(f"Successfully processed chunk {i // chunk_size + 1}")
                        else:
                            logging.warning(f"API Grand_Total {chunk_grand_total} does not match expected {expected_total}, falling back to local count")
                            st.warning(f"API returned incorrect count (Grand_Total: {chunk_grand_total}, expected: {expected_total}), using local count")
                            for record in chunk:
                                block = record["Block"]
                                discipline = record["Discipline_Category"]
                                if block not in all_results[report_type]["Sites"]:
                                    all_results[report_type]["Sites"][block] = {
                                        "Descriptions": [],
                                        "Created Date (WET)": [],
                                        "Expected Close Date (WET)": [],
                                        "Status": [],
                                        "Discipline": [],
                                        "Modules": [],
                                        "SW": 0,
                                        "FW": 0,
                                        "MEP": 0,
                                        "Total": 0,
                                        "ModulesCount": {}
                                    }
                                all_results[report_type]["Sites"][block]["Descriptions"].append(record["Description"])
                                all_results[report_type]["Sites"][block]["Created Date (WET)"].append(record["Created Date (WET)"])
                                all_results[report_type]["Sites"][block]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                                all_results[report_type]["Sites"][block]["Status"].append(record["Status"])
                                all_results[report_type]["Sites"][block]["Discipline"].append(record["Discipline"])
                                all_results[report_type]["Sites"][block]["Modules"].append(["Common"])
                                all_results[report_type]["Sites"][block][discipline] += 1
                                all_results[report_type]["Sites"][block]["Total"] += 1
                                all_results[report_type]["Sites"][block]["ModulesCount"]["Common"] = all_results[report_type]["Sites"][block]["ModulesCount"].get("Common", 0) + 1
                                all_results[report_type]["Grand_Total"] += 1
                    else:
                        logging.error("No valid JSON found in response")
                        st.error("❌ No valid JSON found in response")
                        st.write("Falling back to local count for this chunk")
                        for record in chunk:
                            block = record["Block"]
                            discipline = record["Discipline_Category"]
                            if block not in all_results[report_type]["Sites"]:
                                all_results[report_type]["Sites"][block] = {
                                    "Descriptions": [],
                                    "Created Date (WET)": [],
                                    "Expected Close Date (WET)": [],
                                    "Status": [],
                                    "Discipline": [],
                                    "Modules": [],
                                    "SW": 0,
                                    "FW": 0,
                                    "MEP": 0,
                                    "Total": 0,
                                    "ModulesCount": {}
                                }
                            all_results[report_type]["Sites"][block]["Descriptions"].append(record["Description"])
                            all_results[report_type]["Sites"][block]["Created Date (WET)"].append(record["Created Date (WET)"])
                            all_results[report_type]["Sites"][block]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                            all_results[report_type]["Sites"][block]["Status"].append(record["Status"])
                            all_results[report_type]["Sites"][block]["Discipline"].append(record["Discipline"])
                            all_results[report_type]["Sites"][block]["Modules"].append(["Common"])
                            all_results[report_type]["Sites"][block][discipline] += 1
                            all_results[report_type]["Sites"][block]["Total"] += 1
                            all_results[report_type]["Sites"][block]["ModulesCount"]["Common"] = all_results[report_type]["Sites"][block]["ModulesCount"].get("Common", 0) + 1
                            all_results[report_type]["Grand_Total"] += 1
                else:
                    error_msg = f"❌ WatsonX API error: {response.status_code} - {response.text}"
                    st.error(error_msg)
                    logging.error(error_msg)
                    st.write("Falling back to local count for this chunk")
                    for record in chunk:
                        block = record["Block"]
                        discipline = record["Discipline_Category"]
                        if block not in all_results[report_type]["Sites"]:
                            all_results[report_type]["Sites"][block] = {
                                "Descriptions": [],
                                "Created Date (WET)": [],
                                "Expected Close Date (WET)": [],
                                "Status": [],
                                "Discipline": [],
                                "Modules": [],
                                "SW": 0,
                                "FW": 0,
                                "MEP": 0,
                                "Total": 0,
                                "ModulesCount": {}
                            }
                        all_results[report_type]["Sites"][block]["Descriptions"].append(record["Description"])
                        all_results[report_type]["Sites"][block]["Created Date (WET)"].append(record["Created Date (WET)"])
                        all_results[report_type]["Sites"][block]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                        all_results[report_type]["Sites"][block]["Status"].append(record["Status"])
                        all_results[report_type]["Sites"][block]["Discipline"].append(record["Discipline"])
                        all_results[report_type]["Sites"][block]["Modules"].append(["Common"])
                        all_results[report_type]["Sites"][block][discipline] += 1
                        all_results[report_type]["Sites"][block]["Total"] += 1
                        all_results[report_type]["Sites"][block]["ModulesCount"]["Common"] = all_results[report_type]["Sites"][block]["ModulesCount"].get("Common", 0) + 1
                        all_results[report_type]["Grand_Total"] += 1
            except Exception as e:
                error_msg = f"❌ Exception during WatsonX call: {str(e)}"
                st.error(error_msg)
                logging.error(error_msg)
                st.write("Falling back to local count for this chunk")
                for record in chunk:
                    block = record["Block"]
                    discipline = record["Discipline_Category"]
                    if block not in all_results[report_type]["Sites"]:
                        all_results[report_type]["Sites"][block] = {
                            "Descriptions": [],
                            "Created Date (WET)": [],
                            "Expected Close Date (WET)": [],
                            "Status": [],
                            "Discipline": [],
                            "Modules": [],
                            "SW": 0,
                            "FW": 0,
                            "MEP": 0,
                            "Total": 0,
                            "ModulesCount": {}
                        }
                    all_results[report_type]["Sites"][block]["Descriptions"].append(record["Description"])
                    all_results[report_type]["Sites"][block]["Created Date (WET)"].append(record["Created Date (WET)"])
                    all_results[report_type]["Sites"][block]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                    all_results[report_type]["Sites"][block]["Status"].append(record["Status"])
                    all_results[report_type]["Sites"][block]["Discipline"].append(record["Discipline"])
                    all_results[report_type]["Sites"][block]["Modules"].append(["Common"])
                    all_results[report_type]["Sites"][block][discipline] += 1
                    all_results[report_type]["Sites"][block]["Total"] += 1
                    all_results[report_type]["Sites"][block]["ModulesCount"]["Common"] = all_results[report_type]["Sites"][block]["ModulesCount"].get("Common", 0) + 1
                    all_results[report_type]["Grand_Total"] += 1

        st.write(f"Debug - Final {report_type} result: {json.dumps(all_results, indent=2)}")
        return all_results, json.dumps(all_results)

def clean_and_parse_json(generated_text):
    # Remove code block markers if present
    cleaned_text = re.sub(r'```json|```python|```', '', generated_text).strip()
    
    # First attempt: Try to parse the text directly as JSON
    try:
        for line in cleaned_text.split('\n'):
            line = line.strip()
            if line.startswith('{') and line.endswith('}'):
                return json.loads(line)
        return json.loads(cleaned_text)
    except json.JSONDecodeError as e:
        logger.warning(f"Initial JSONDecodeError: {str(e)} - Cleaned response: {cleaned_text}")
    
    # Second attempt: If the response contains Python code with a print(json.dumps(...)),
    # extract the JSON from the output
    json_match = re.search(r'print$$ json\.dumps\((.*?),\s*indent=2 $$\)', cleaned_text, re.DOTALL)
    if json_match:
        json_str = json_match.group(1).strip()
        try:
            return eval(json_str)  # Safely evaluate the JSON string as a Python dict
        except Exception as e:
            logger.error(f"Failed to evaluate extracted JSON: {str(e)} - Extracted JSON: {json_str}")
    
    logger.error(f"JSONDecodeError: Unable to parse response - Cleaned response: {cleaned_text}")
    return None

@st.cache_data
def generate_ncr_Housekeeping_report(df, report_type, start_date=None, end_date=None, open_until_date=None):
    with st.spinner(f"Generating {report_type} Housekeeping NCR Report with WatsonX..."):
        today = pd.to_datetime(datetime.today().strftime('%Y/%m/%d'))
        closed_start = pd.to_datetime(start_date) if start_date else None
        closed_end = pd.to_datetime(end_date) if end_date else None
        open_until = pd.to_datetime(open_until_date)

        if report_type == "Closed":
            filtered_df = df[
                (df['Discipline'] == 'HSE') &
                (df['Status'] == 'Closed') &
                (df['Days'].notnull()) &
                (df['Days'] > 7)
            ].copy()
            if closed_start and closed_end:
                filtered_df = filtered_df[
                    (pd.to_datetime(filtered_df['Created Date (WET)']) >= closed_start) &
                    (pd.to_datetime(filtered_df['Expected Close Date (WET)']) <= closed_end)
                ].copy()
        else:  # Open
            filtered_df = df[
                (df['Discipline'] == 'HSE') &
                (df['Status'] == 'Open') &
                (pd.to_datetime(df['Created Date (WET)']).notna())
            ].copy()
            filtered_df.loc[:, 'Days_From_Today'] = (today - pd.to_datetime(filtered_df['Created Date (WET)'])).dt.days
            filtered_df = filtered_df[filtered_df['Days_From_Today'] > 7].copy()
            if open_until:
                filtered_df = filtered_df[
                    (pd.to_datetime(filtered_df['Created Date (WET)']) <= open_until)
                ].copy()

        if filtered_df.empty:
            return {"error": f"No {report_type} records found with duration > 7 days"}, ""

        filtered_df.loc[:, 'Created Date (WET)'] = filtered_df['Created Date (WET)'].astype(str)
        filtered_df.loc[:, 'Expected Close Date (WET)'] = filtered_df['Expected Close Date (WET)'].astype(str)

        processed_data = filtered_df.to_dict(orient="records")
        
        cleaned_data = []
        seen_descriptions = set()
        standard_blocks = [
            "Block 1 (B1) Banquet Hall",
            "Block 5 (B5) Admin + Member Lounge + Creche + AV Room + Surveillance Room + Toilets",
            "Block 6 (B6) Toilets",
            "Block 7 (B7) Indoor Sports",
            "Block 9 (B9) Spa & Saloon",
            "Block 8 (B8) Squash Court",
            "Block 2 & 3 (B2 & B3) Cafe & Bar",
            "Block 4 (B4) Indoor Swimming Pool Changing Room & Toilets",
            "Block 11 (B11) Guest House",
            "Block 10 (B10) Gym"
        ]
        for record in processed_data:
            description = str(record.get("Description", "")).strip()
            if description and description not in seen_descriptions:
                seen_descriptions.add(description)
                cleaned_record = {
                    "Description": description,
                    "Created Date (WET)": str(record.get("Created Date (WET)", "")),
                    "Expected Close Date (WET)": str(record.get("Expected Close Date (WET)", "")),
                    "Status": str(record.get("Status", "")),
                    "Days": record.get("Days", 0),
                    "Tower": "Block 1 (B1) Banquet Hall"  # Default block
                }

                desc_lower = description.lower()
                assigned_block = None
                for block in standard_blocks:
                    block_short = block.split("(")[1].split(")")[0].lower()  # e.g., "b1", "b5"
                    if block_short in desc_lower:
                        cleaned_record["Tower"] = block
                        assigned_block = block
                        break
                if not assigned_block:
                    cleaned_record["Tower"] = "Common_Area"
                    logger.debug(f"Tower set to Common_Area for description: {desc_lower}")

                cleaned_data.append(cleaned_record)

        st.write(f"Total {report_type} records to process: {len(cleaned_data)}")
        logger.debug(f"Processed data: {json.dumps(cleaned_data, indent=2)}")

        if not cleaned_data:
            return {"Housekeeping": {"Sites": {}, "Grand_Total": 0}}, ""

        access_token = get_access_token(API_KEY)
        if not access_token:
            return {"error": "Failed to obtain access token"}, ""

        result = {"Housekeeping": {"Sites": {}, "Grand_Total": 0}}
        chunk_size = 1
        total_chunks = (len(cleaned_data) + chunk_size - 1) // chunk_size

        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504, 429, 408],
            allowed_methods=["POST"],
            raise_on_redirect=True,
            raise_on_status=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        progress_placeholder = st.empty()
        status_placeholder = st.empty()
        error_placeholder = st.empty()
        progress_bar = progress_placeholder.progress(0)

        for i in range(0, len(cleaned_data), chunk_size):
            chunk = cleaned_data[i:i + chunk_size]
            current_chunk = i // chunk_size + 1
            progress = min((current_chunk / total_chunks) * 100, 100)
            progress_bar.progress(int(progress))
            status_placeholder.write(f"Processed {current_chunk}/{total_chunks} chunks ({int(progress)}%)")
            logger.debug(f"Chunk data: {json.dumps(chunk, indent=2)}")

            prompt = (
                "IMPORTANT: YOU MUST RETURN ONLY A SINGLE VALID JSON OBJECT WITH THE ACTUAL RESULTS. "
                "Return the result strictly as a single JSON object—no code, no explanations, no string literal like this ```, only the JSON."
                "DO NOT INCLUDE EXAMPLES, EXPLANATIONS, COMMENTS, OR ANY ADDITIONAL TEXT BEYOND THE JSON OBJECT. "
                "DO NOT WRAP THE JSON IN CODE BLOCKS (e.g., ```). "
                "DO NOT GENERATE EXAMPLE OUTPUTS FOR OTHER SCENARIOS. "
                "ONLY PROCESS THE PROVIDED DATA AND RETURN THE RESULT.\n\n"
                "Task: For Housekeeping NCRs, count EVERY record in the provided data by site ('Tower' field) where 'Discipline' is 'HSE' and 'Days' is greater than 7. "
                "The 'Description' MUST be counted if it contains ANY of the following housekeeping issues (match these keywords exactly as provided, case-insensitive): "
                "'housekeeping','cleaning','cleanliness','waste disposal','waste management','garbage','trash','rubbish','debris','litter','dust','untidy',"
                "'cluttered','accumulation of waste','construction waste','pile of garbage','poor housekeeping','material storage','construction debris',"
                "'cleaning schedule','garbage collection','waste bins','dirty','mess','unclean','disorderly','dirty floor','waste disposal area',"
                "'waste collection','cleaning protocol','sanitation','trash removal','waste accumulation','unkept area','refuse collection','workplace cleanliness'. "
                f"Use the 'Tower' values from the following list or 'Common': {', '.join(standard_blocks + ['Common_Area'])}. "
                "Collect 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', and 'Status' into arrays for each site. "
                "Assign each count to the 'Count' key, representing 'No. of Housekeeping NCRs beyond 7 days'. "
                "If no matches are found for a site, set its count to 0, but ensure all present sites in the data are listed. "
                "INCLUDE ONLY records where housekeeping is the PRIMARY concern and EXCLUDE records that are primarily about safety issues (e.g., descriptions focusing on 'safety precautions', 'PPE', 'fall protection').\n\n"
                "REQUIRED OUTPUT FORMAT (use this structure with the actual results):\n"
                "{\n"
                '  "Housekeeping": {\n'
                '    "Sites": {\n'
                '      "Site_Name1": {\n'
                '        "Descriptions": ["description1", "description2"],\n'
                '        "Created Date (WET)": ["date1", "date2"],\n'
                '        "Expected Close Date (WET)": ["date1", "date2"],\n'
                '        "Status": ["status1", "status2"],'
                '        "Count": number\n'
                '      },\n'
                '      "Site_Name2": {\n'
                '        "Descriptions": ["description1", "description2"],\n'
                '        "Created Date (WET)": ["date1", "date2"],\n'
                '        "Expected Close Date (WET)": ["date1", "date2"],\n'
                '        "Status": ["status1", "status2"],\n'
                '        "Count": number\n'
                '      }\n'
                '    },\n'
                '    "Grand_Total": number\n'
                '  }\n'
                '}\n\n'
                f"Data: {json.dumps(chunk)}\n"
            )

            payload = {
                "input": prompt,
                "parameters": {"decoding_method": "greedy", "max_new_tokens": 8100, "min_new_tokens": 0, "temperature": 0.001},
                "model_id": MODEL_ID,
                "project_id": PROJECT_ID
            }
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }

            try:
                logger.debug("Initiating WatsonX API call...")
                response = session.post(WATSONX_API_URL, headers=headers, json=payload, verify=certifi.where(), timeout=300)
                logger.info(f"WatsonX API response status: {response.status_code}")

                if response.status_code == 200:
                    api_result = response.json()
                    generated_text = api_result.get("results", [{}])[0].get("generated_text", "").strip()
                    logger.debug(f"Generated text for chunk {current_chunk}: {generated_text}")

                    if generated_text:
                        # Extract the JSON portion by finding the first complete JSON object
                        json_str = None
                        brace_count = 0
                        start_idx = None
                        for idx, char in enumerate(generated_text):
                            if char == '{':
                                if brace_count == 0:
                                    start_idx = idx
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0 and start_idx is not None:
                                    json_str = generated_text[start_idx:idx + 1]
                                    break

                        if json_str:
                            try:
                                parsed_json = json.loads(json_str)
                                chunk_result = parsed_json.get("Housekeeping", {})
                                chunk_sites = chunk_result.get("Sites", {})
                                chunk_grand_total = chunk_result.get("Grand_Total", 0)

                                for site, values in chunk_sites.items():
                                    if not isinstance(values, dict):
                                        logger.warning(f"Invalid site data for {site}: {values}, converting to dict")
                                        values = {
                                            "Count": int(values) if isinstance(values, (int, float)) else 0,
                                            "Descriptions": [],
                                            "Created Date (WET)": [],
                                            "Expected Close Date (WET)": [],
                                            "Status": []
                                        }
                                    
                                    if site not in result["Housekeeping"]["Sites"]:
                                        result["Housekeeping"]["Sites"][site] = {
                                            "Count": 0,
                                            "Descriptions": [],
                                            "Created Date (WET)": [],
                                            "Expected Close Date (WET)": [],
                                            "Status": []
                                        }
                                    
                                    if "Descriptions" in values and values["Descriptions"]:
                                        if not isinstance(values["Descriptions"], list):
                                            values["Descriptions"] = [str(values["Descriptions"])]
                                        result["Housekeeping"]["Sites"][site]["Descriptions"].extend(values["Descriptions"])
                                    
                                    if "Created Date (WET)" in values and values["Created Date (WET)"]:
                                        if not isinstance(values["Created Date (WET)"], list):
                                            values["Created Date (WET)"] = [str(values["Created Date (WET)"])]
                                        result["Housekeeping"]["Sites"][site]["Created Date (WET)"].extend(values["Created Date (WET)"])
                                    
                                    if "Expected Close Date (WET)" in values and values["Expected Close Date (WET)"]:
                                        if not isinstance(values["Expected Close Date (WET)"], list):
                                            values["Expected Close Date (WET)"] = [str(values["Expected Close Date (WET)"])]
                                        result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"].extend(values["Expected Close Date (WET)"])
                                    
                                    if "Status" in values and values["Status"]:
                                        if not isinstance(values["Status"], list):
                                            values["Status"] = [str(values["Status"])]
                                        result["Housekeeping"]["Sites"][site]["Status"].extend(values["Status"])
                                    
                                    count = values.get("Count", 0)
                                    if not isinstance(count, (int, float)):
                                        count = 0
                                    result["Housekeeping"]["Sites"][site]["Count"] += count
                                
                                result["Housekeeping"]["Grand_Total"] += chunk_grand_total
                                logger.debug(f"Successfully processed chunk {current_chunk}/{total_chunks}")
                            except json.JSONDecodeError as e:
                                logger.error(f"JSONDecodeError for chunk {current_chunk}: {str(e)} - Raw: {json_str}")
                                error_placeholder.error(f"Failed to parse JSON for chunk {current_chunk}: {str(e)}")
                                # Fallback: Manually process the chunk
                                for record in chunk:
                                    site = record["Tower"]
                                    if site not in result["Housekeeping"]["Sites"]:
                                        result["Housekeeping"]["Sites"][site] = {
                                            "Count": 0,
                                            "Descriptions": [],
                                            "Created Date (WET)": [],
                                            "Expected Close Date (WET)": [],
                                            "Status": []
                                        }
                                    result["Housekeeping"]["Sites"][site]["Descriptions"].append(record["Description"])
                                    result["Housekeeping"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                                    result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                                    result["Housekeeping"]["Sites"][site]["Status"].append(record["Status"])
                                    result["Housekeeping"]["Sites"][site]["Count"] += 1
                                    result["Housekeeping"]["Grand_Total"] += 1
                                logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
                        else:
                            logger.error(f"No valid JSON found in response for chunk {current_chunk}: {generated_text}")
                            error_placeholder.error(f"No valid JSON found in response for chunk {current_chunk}")
                            # Fallback: Manually process the chunk
                            for record in chunk:
                                site = record["Tower"]
                                if site not in result["Housekeeping"]["Sites"]:
                                    result["Housekeeping"]["Sites"][site] = {
                                        "Count": 0,
                                        "Descriptions": [],
                                        "Created Date (WET)": [],
                                        "Expected Close Date (WET)": [],
                                        "Status": []
                                    }
                                result["Housekeeping"]["Sites"][site]["Descriptions"].append(record["Description"])
                                result["Housekeeping"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                                result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                                result["Housekeeping"]["Sites"][site]["Status"].append(record["Status"])
                                result["Housekeeping"]["Sites"][site]["Count"] += 1
                                result["Housekeeping"]["Grand_Total"] += 1
                            logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
                    else:
                        logger.error(f"Empty WatsonX response for chunk {current_chunk}")
                        error_placeholder.error(f"Empty WatsonX response for chunk {current_chunk}")
                        # Fallback: Manually process the chunk
                        for record in chunk:
                            site = record["Tower"]
                            if site not in result["Housekeeping"]["Sites"]:
                                result["Housekeeping"]["Sites"][site] = {
                                    "Count": 0,
                                    "Descriptions": [],
                                    "Created Date (WET)": [],
                                    "Expected Close Date (WET)": [],
                                    "Status": []
                                }
                            result["Housekeeping"]["Sites"][site]["Descriptions"].append(record["Description"])
                            result["Housekeeping"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                            result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                            result["Housekeeping"]["Sites"][site]["Status"].append(record["Status"])
                            result["Housekeeping"]["Sites"][site]["Count"] += 1
                            result["Housekeeping"]["Grand_Total"] += 1
                        logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
                else:
                    logger.error(f"WatsonX API error for chunk {current_chunk}: {response.status_code} - {response.text}")
                    error_placeholder.error(f"WatsonX API error for chunk {current_chunk}: {response.status_code} - {response.text}")
                    # Fallback: Manually process the chunk
                    for record in chunk:
                        site = record["Tower"]
                        if site not in result["Housekeeping"]["Sites"]:
                            result["Housekeeping"]["Sites"][site] = {
                                "Count": 0,
                                "Descriptions": [],
                                "Created Date (WET)": [],
                                "Expected Close Date (WET)": [],
                                "Status": []
                            }
                        result["Housekeeping"]["Sites"][site]["Descriptions"].append(record["Description"])
                        result["Housekeeping"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                        result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                        result["Housekeeping"]["Sites"][site]["Status"].append(record["Status"])
                        result["Housekeeping"]["Sites"][site]["Count"] += 1
                        result["Housekeeping"]["Grand_Total"] += 1
                    logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
            except requests.exceptions.ReadTimeout as e:
                logger.error(f"ReadTimeoutError after retries for chunk {current_chunk}: {str(e)}")
                error_placeholder.error(f"Failed to connect to WatsonX API for chunk {current_chunk} after retries due to timeout: {str(e)}")
                # Fallback: Manually process the chunk
                for record in chunk:
                    site = record["Tower"]
                    if site not in result["Housekeeping"]["Sites"]:
                        result["Housekeeping"]["Sites"][site] = {
                            "Count": 0,
                            "Descriptions": [],
                            "Created Date (WET)": [],
                            "Expected Close Date (WET)": [],
                            "Status": []
                        }
                    result["Housekeeping"]["Sites"][site]["Descriptions"].append(record["Description"])
                    result["Housekeeping"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                    result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                    result["Housekeeping"]["Sites"][site]["Status"].append(record["Status"])
                    result["Housekeeping"]["Sites"][site]["Count"] += 1
                    result["Housekeeping"]["Grand_Total"] += 1
                logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
            except requests.exceptions.RequestException as e:
                logger.error(f"RequestException for chunk {current_chunk}: {str(e)}")
                error_placeholder.error(f"Failed to connect to WatsonX API for chunk {current_chunk}: {str(e)}")
                # Fallback: Manually process the chunk
                for record in chunk:
                    site = record["Tower"]
                    if site not in result["Housekeeping"]["Sites"]:
                        result["Housekeeping"]["Sites"][site] = {
                            "Count": 0,
                            "Descriptions": [],
                            "Created Date (WET)": [],
                            "Expected Close Date (WET)": [],
                            "Status": []
                        }
                    result["Housekeeping"]["Sites"][site]["Descriptions"].append(record["Description"])
                    result["Housekeeping"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                    result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                    result["Housekeeping"]["Sites"][site]["Status"].append(record["Status"])
                    result["Housekeeping"]["Sites"][site]["Count"] += 1
                    result["Housekeeping"]["Grand_Total"] += 1
                logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
            except Exception as e:
                logger.error(f"Unexpected error during WatsonX API call for chunk {current_chunk}: {str(e)}")
                error_placeholder.error(f"Unexpected error during WatsonX API call for chunk {current_chunk}: {str(e)}")
                # Fallback: Manually process the chunk
                for record in chunk:
                    site = record["Tower"]
                    if site not in result["Housekeeping"]["Sites"]:
                        result["Housekeeping"]["Sites"][site] = {
                            "Count": 0,
                            "Descriptions": [],
                            "Created Date (WET)": [],
                            "Expected Close Date (WET)": [],
                            "Status": []
                        }
                    result["Housekeeping"]["Sites"][site]["Descriptions"].append(record["Description"])
                    result["Housekeeping"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                    result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                    result["Housekeeping"]["Sites"][site]["Status"].append(record["Status"])
                    result["Housekeeping"]["Sites"][site]["Count"] += 1
                    result["Housekeeping"]["Grand_Total"] += 1
                logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")

        progress_bar.progress(100)
        status_placeholder.write(f"Processed {total_chunks}/{total_chunks} chunks (100%)")
        logger.debug(f"Final result before deduplication: {json.dumps(result, indent=2)}")

        for site in result["Housekeeping"]["Sites"]:
            if "Descriptions" in result["Housekeeping"]["Sites"][site]:
                result["Housekeeping"]["Sites"][site]["Descriptions"] = list(set(result["Housekeeping"]["Sites"][site]["Descriptions"]))
            if "Created Date (WET)" in result["Housekeeping"]["Sites"][site]:
                result["Housekeeping"]["Sites"][site]["Created Date (WET)"] = list(set(result["Housekeeping"]["Sites"][site]["Created Date (WET)"]))
            if "Expected Close Date (WET)" in result["Housekeeping"]["Sites"][site]:
                result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"] = list(set(result["Housekeeping"]["Sites"][site]["Expected Close Date (WET)"]))
            if "Status" in result["Housekeeping"]["Sites"][site]:
                result["Housekeeping"]["Sites"][site]["Status"] = list(set(result["Housekeeping"]["Sites"][site]["Status"]))
        
        logger.debug(f"Final result after deduplication: {json.dumps(result, indent=2)}")
        return result, json.dumps(result)
    

@st.cache_data
def generate_ncr_Safety_report(df, report_type, start_date=None, end_date=None, open_until_date=None):
    with st.spinner(f"Generating {report_type} Safety NCR Report with WatsonX..."):
        today = pd.to_datetime(datetime.today().strftime('%Y/%m/%d'))
        closed_start = pd.to_datetime(start_date) if start_date else None
        closed_end = pd.to_datetime(end_date) if end_date else None
        open_until = pd.to_datetime(open_until_date)

        if report_type == "Closed":
            filtered_df = df[
                (df['Discipline'] == 'HSE') &
                (df['Status'] == 'Closed') &
                (df['Days'].notnull()) &
                (df['Days'] > 7)
            ].copy()
            if closed_start and closed_end:
                filtered_df = filtered_df[
                    (pd.to_datetime(filtered_df['Created Date (WET)']) >= closed_start) &
                    (pd.to_datetime(filtered_df['Expected Close Date (WET)']) <= closed_end)
                ].copy()
        else:  # Open
            filtered_df = df[
                (df['Discipline'] == 'HSE') &
                (df['Status'] == 'Open') &
                (pd.to_datetime(df['Created Date (WET)']).notna())
            ].copy()
            filtered_df.loc[:, 'Days_From_Today'] = (today - pd.to_datetime(filtered_df['Created Date (WET)'])).dt.days
            filtered_df = filtered_df[filtered_df['Days_From_Today'] > 7].copy()
            if open_until:
                filtered_df = filtered_df[
                    (pd.to_datetime(filtered_df['Created Date (WET)']) <= open_until)
                ].copy()

        if filtered_df.empty:
            return {"error": f"No {report_type} records found with duration > 7 days"}, ""

        filtered_df.loc[:, 'Created Date (WET)'] = filtered_df['Created Date (WET)'].astype(str)
        filtered_df.loc[:, 'Expected Close Date (WET)'] = filtered_df['Expected Close Date (WET)'].astype(str)

        processed_data = filtered_df.to_dict(orient="records")
        
        cleaned_data = []
        seen_descriptions = set()
        standard_blocks = [
            "Block 1 (B1) Banquet Hall",
            "Block 5 (B5) Admin + Member Lounge + Creche + AV Room + Surveillance Room + Toilets",
            "Block 6 (B6) Toilets",
            "Block 7 (B7) Indoor Sports",
            "Block 9 (B9) Spa & Saloon",
            "Block 8 (B8) Squash Court",
            "Block 2 & 3 (B2 & B3) Cafe & Bar",
            "Block 4 (B4) Indoor Swimming Pool Changing Room & Toilets",
            "Block 11 (B11) Guest House",
            "Block 10 (B10) Gym"
        ]
        for record in processed_data:
            description = str(record.get("Description", "")).strip()
            if description and description not in seen_descriptions:
                seen_descriptions.add(description)
                cleaned_record = {
                    "Description": description,
                    "Created Date (WET)": str(record.get("Created Date (WET)", "")),
                    "Expected Close Date (WET)": str(record.get("Expected Close Date (WET)", "")),
                    "Status": str(record.get("Status", "")),
                    "Days": record.get("Days", 0),
                    "Tower": "Block 1 (B1) Banquet Hall"  # Default block
                }

                desc_lower = description.lower()
                assigned_block = None
                for block in standard_blocks:
                    block_short = block.split("(")[1].split(")")[0].lower()  # e.g., "b1", "b5"
                    if block_short in desc_lower:
                        cleaned_record["Tower"] = block
                        assigned_block = block
                        break
                if not assigned_block:
                    cleaned_record["Tower"] = "Common_Area"
                    logger.debug(f"Tower set to Common_Area for description: {desc_lower}")

                cleaned_data.append(cleaned_record)

        st.write(f"Total {report_type} records to process: {len(cleaned_data)}")
        logger.debug(f"Processed data: {json.dumps(cleaned_data, indent=2)}")

        if not cleaned_data:
            return {"Safety": {"Sites": {}, "Grand_Total": 0}}, ""

        access_token = get_access_token(API_KEY)
        if not access_token:
            return {"error": "Failed to obtain access token"}, ""

        result = {"Safety": {"Sites": {}, "Grand_Total": 0}}
        chunk_size = 1
        total_chunks = (len(cleaned_data) + chunk_size - 1) // chunk_size

        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504, 429, 408],
            allowed_methods=["POST"],
            raise_on_redirect=True,
            raise_on_status=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        progress_placeholder = st.empty()
        status_placeholder = st.empty()
        error_placeholder = st.empty()
        progress_bar = progress_placeholder.progress(0)

        for i in range(0, len(cleaned_data), chunk_size):
            chunk = cleaned_data[i:i + chunk_size]
            current_chunk = i // chunk_size + 1
            progress = min((current_chunk / total_chunks) * 100, 100)
            progress_bar.progress(int(progress))
            status_placeholder.write(f"Processed {current_chunk}/{total_chunks} chunks ({int(progress)}%)")
            logger.debug(f"Chunk data: {json.dumps(chunk, indent=2)}")

            prompt = (
                "IMPORTANT: YOU MUST RETURN ONLY A SINGLE VALID JSON OBJECT WITH THE ACTUAL RESULTS. "
                "DO NOT INCLUDE EXAMPLES, EXPLANATIONS, COMMENTS, OR ANY ADDITIONAL TEXT BEYOND THE JSON OBJECT. "
                "DO NOT WRAP THE JSON IN CODE BLOCKS (e.g., ```). "
                "DO NOT GENERATE EXAMPLE OUTPUTS FOR OTHER SCENARIOS. "
                "ONLY PROCESS THE PROVIDED DATA AND RETURN THE RESULT.\n\n"
                "Task: For Safety NCRs, count EVERY record in the provided data by site ('Tower' field) where 'Discipline' is 'HSE' and 'Days' is greater than 7. "
                "The 'Description' MUST be counted if it contains ANY of the following construction safety issues (match these keywords exactly as provided, case-insensitive): "
                "'safety precautions','temporary electricity','on-site labor is working without wearing safety belt','safety norms','Missing Cabin Glass – Tower Crane',"
                "'Crane Operator cabin front glass','site on priority basis lifeline is not fixed at the working place','operated only after Third Party Inspection and certification crane operated without TPIC',"
                "'We have found that safety precautions are not taken seriously at site Tower crane operator cabin front glass is missing while crane operator is working inside cabin.',"
                "'no barrier around','Lock and Key arrangement to restrict unauthorized operations, buzzer while operation, gates at landing platforms, catch net in the vicinity', "
                "'safety precautions are not taken seriously','firecase','Health and Safety Plan','noticed that submission of statistics report is regularly delayed',"
                "'crane operator cabin front glass is missing while crane operator is working inside cabin','labor is working without wearing safety belt', 'barricading', 'tank', 'safety shoes', "
                "'safety belt', 'helmet', 'lifeline', 'guard rails', 'fall protection', 'PPE', 'electrical hazard', 'unsafe platform', 'catch net', 'edge protection', 'TPI', 'scaffold', "
                "'lifting equipment', 'temporary electricity', 'dust suppression', 'debris chute', 'spill control', 'crane operator', 'halogen lamps', 'fall catch net', 'environmental contamination', 'fire hazard'. "
                f"Use the 'Tower' values from the following list or 'Common': {', '.join(standard_blocks + ['Common_Area'])}. "
                "Collect 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', and 'Status' into arrays for each site. "
                "Assign each count to the 'Count' key, representing 'No. of Safety NCRs beyond 7 days'. "
                "If no matches are found for a site, set its count to 0, but ensure all present sites in the data are listed. "
                "EXCLUDE records where 'housekeeping' is the PRIMARY safety concern (e.g., descriptions focusing solely on 'housekeeping' or 'cleaning').\n\n"
                "REQUIRED OUTPUT FORMAT (use this structure with the actual results):\n"
                "{\n"
                '  "Safety": {\n'
                '    "Sites": {\n'
                '      "Site_Name1": {\n'
                '        "Descriptions": ["description1", "description2"],\n'
                '        "Created Date (WET)": ["date1", "date2"],\n'
                '        "Expected Close Date (WET)": ["date1", "date2"],\n'
                '        "Status": ["status1", "status2"],\n'
                '        "Count": number\n'
                '      },\n'
                '      "Site_Name2": {\n'
                '        "Descriptions": ["description1", "description2"],\n'
                '        "Created Date (WET)": ["date1", "date2"],\n'
                '        "Expected Close Date (WET)": ["date1", "date2"],\n'
                '        "Status": ["status1", "status2"],\n'
                '        "Count": number\n'
                '      }\n'
                '    },\n'
                '    "Grand_Total": number\n'
                '  }\n'
                '}\n\n'
                f"Data: {json.dumps(chunk)}\n"
            )

            payload = {
                "input": prompt,
                "parameters": {"decoding_method": "greedy", "max_new_tokens": 8100, "min_new_tokens": 0, "temperature": 0.001},
                "model_id": MODEL_ID,
                "project_id": PROJECT_ID
            }
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }

            try:
                logger.debug("Initiating WatsonX API call...")
                response = session.post(WATSONX_API_URL, headers=headers, json=payload, verify=certifi.where(), timeout=300)
                logger.info(f"WatsonX API response status: {response.status_code}")

                if response.status_code == 200:
                    api_result = response.json()
                    generated_text = api_result.get("results", [{}])[0].get("generated_text", "").strip()
                    logger.debug(f"Generated text for chunk {current_chunk}: {generated_text}")

                    if generated_text:
                        # Extract the JSON portion by finding the first complete JSON object
                        json_str = None
                        brace_count = 0
                        start_idx = None
                        for idx, char in enumerate(generated_text):
                            if char == '{':
                                if brace_count == 0:
                                    start_idx = idx
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0 and start_idx is not None:
                                    json_str = generated_text[start_idx:idx + 1]
                                    break

                        if json_str:
                            try:
                                parsed_json = json.loads(json_str)
                                chunk_result = parsed_json.get("Safety", {})
                                chunk_sites = chunk_result.get("Sites", {})
                                chunk_grand_total = chunk_result.get("Grand_Total", 0)

                                for site, values in chunk_sites.items():
                                    if not isinstance(values, dict):
                                        logger.warning(f"Invalid site data for {site}: {values}, converting to dict")
                                        values = {
                                            "Count": int(values) if isinstance(values, (int, float)) else 0,
                                            "Descriptions": [],
                                            "Created Date (WET)": [],
                                            "Expected Close Date (WET)": [],
                                            "Status": []
                                        }
                                    
                                    if site not in result["Safety"]["Sites"]:
                                        result["Safety"]["Sites"][site] = {
                                            "Count": 0,
                                            "Descriptions": [],
                                            "Created Date (WET)": [],
                                            "Expected Close Date (WET)": [],
                                            "Status": []
                                        }
                                    
                                    if "Descriptions" in values and values["Descriptions"]:
                                        if not isinstance(values["Descriptions"], list):
                                            values["Descriptions"] = [str(values["Descriptions"])]
                                        result["Safety"]["Sites"][site]["Descriptions"].extend(values["Descriptions"])
                                    
                                    if "Created Date (WET)" in values and values["Created Date (WET)"]:
                                        if not isinstance(values["Created Date (WET)"], list):
                                            values["Created Date (WET)"] = [str(values["Created Date (WET)"])]
                                        result["Safety"]["Sites"][site]["Created Date (WET)"].extend(values["Created Date (WET)"])
                                    
                                    if "Expected Close Date (WET)" in values and values["Expected Close Date (WET)"]:
                                        if not isinstance(values["Expected Close Date (WET)"], list):
                                            values["Expected Close Date (WET)"] = [str(values["Expected Close Date (WET)"])]
                                        result["Safety"]["Sites"][site]["Expected Close Date (WET)"].extend(values["Expected Close Date (WET)"])
                                    
                                    if "Status" in values and values["Status"]:
                                        if not isinstance(values["Status"], list):
                                            values["Status"] = [str(values["Status"])]
                                        result["Safety"]["Sites"][site]["Status"].extend(values["Status"])
                                    
                                    count = values.get("Count", 0)
                                    if not isinstance(count, (int, float)):
                                        count = 0
                                    result["Safety"]["Sites"][site]["Count"] += count
                                
                                result["Safety"]["Grand_Total"] += chunk_grand_total
                                logger.debug(f"Successfully processed chunk {current_chunk}/{total_chunks}")
                            except json.JSONDecodeError as e:
                                logger.error(f"JSONDecodeError for chunk {current_chunk}: {str(e)} - Raw: {json_str}")
                                error_placeholder.error(f"Failed to parse JSON for chunk {current_chunk}: {str(e)}")
                                # Fallback: Manually process the chunk
                                for record in chunk:
                                    site = record["Tower"]
                                    if site not in result["Safety"]["Sites"]:
                                        result["Safety"]["Sites"][site] = {
                                            "Count": 0,
                                            "Descriptions": [],
                                            "Created Date (WET)": [],
                                            "Expected Close Date (WET)": [],
                                            "Status": []
                                        }
                                    result["Safety"]["Sites"][site]["Descriptions"].append(record["Description"])
                                    result["Safety"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                                    result["Safety"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                                    result["Safety"]["Sites"][site]["Status"].append(record["Status"])
                                    result["Safety"]["Sites"][site]["Count"] += 1
                                    result["Safety"]["Grand_Total"] += 1
                                logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
                        else:
                            logger.error(f"No valid JSON found in response for chunk {current_chunk}: {generated_text}")
                            error_placeholder.error(f"No valid JSON found in response for chunk {current_chunk}")
                            # Fallback: Manually process the chunk
                            for record in chunk:
                                site = record["Tower"]
                                if site not in result["Safety"]["Sites"]:
                                    result["Safety"]["Sites"][site] = {
                                        "Count": 0,
                                        "Descriptions": [],
                                        "Created Date (WET)": [],
                                        "Expected Close Date (WET)": [],
                                        "Status": []
                                    }
                                result["Safety"]["Sites"][site]["Descriptions"].append(record["Description"])
                                result["Safety"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                                result["Safety"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                                result["Safety"]["Sites"][site]["Status"].append(record["Status"])
                                result["Safety"]["Sites"][site]["Count"] += 1
                                result["Safety"]["Grand_Total"] += 1
                            logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
                    else:
                        logger.error(f"Empty WatsonX response for chunk {current_chunk}")
                        error_placeholder.error(f"Empty WatsonX response for chunk {current_chunk}")
                        # Fallback: Manually process the chunk
                        for record in chunk:
                            site = record["Tower"]
                            if site not in result["Safety"]["Sites"]:
                                result["Safety"]["Sites"][site] = {
                                    "Count": 0,
                                    "Descriptions": [],
                                    "Created Date (WET)": [],
                                    "Expected Close Date (WET)": [],
                                    "Status": []
                                }
                            result["Safety"]["Sites"][site]["Descriptions"].append(record["Description"])
                            result["Safety"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                            result["Safety"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                            result["Safety"]["Sites"][site]["Status"].append(record["Status"])
                            result["Safety"]["Sites"][site]["Count"] += 1
                            result["Safety"]["Grand_Total"] += 1
                        logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
                else:
                    logger.error(f"WatsonX API error for chunk {current_chunk}: {response.status_code} - {response.text}")
                    error_placeholder.error(f"WatsonX API error for chunk {current_chunk}: {response.status_code} - {response.text}")
                    # Fallback: Manually process the chunk
                    for record in chunk:
                        site = record["Tower"]
                        if site not in result["Safety"]["Sites"]:
                            result["Safety"]["Sites"][site] = {
                                "Count": 0,
                                "Descriptions": [],
                                "Created Date (WET)": [],
                                "Expected Close Date (WET)": [],
                                "Status": []
                            }
                        result["Safety"]["Sites"][site]["Descriptions"].append(record["Description"])
                        result["Safety"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                        result["Safety"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                        result["Safety"]["Sites"][site]["Status"].append(record["Status"])
                        result["Safety"]["Sites"][site]["Count"] += 1
                        result["Safety"]["Grand_Total"] += 1
                    logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
            except requests.exceptions.ReadTimeout as e:
                logger.error(f"ReadTimeoutError after retries for chunk {current_chunk}: {str(e)}")
                error_placeholder.error(f"Failed to connect to WatsonX API for chunk {current_chunk} after retries due to timeout: {str(e)}")
                # Fallback: Manually process the chunk
                for record in chunk:
                    site = record["Tower"]
                    if site not in result["Safety"]["Sites"]:
                        result["Safety"]["Sites"][site] = {
                            "Count": 0,
                            "Descriptions": [],
                            "Created Date (WET)": [],
                            "Expected Close Date (WET)": [],
                            "Status": []
                        }
                    result["Safety"]["Sites"][site]["Descriptions"].append(record["Description"])
                    result["Safety"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                    result["Safety"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                    result["Safety"]["Sites"][site]["Status"].append(record["Status"])
                    result["Safety"]["Sites"][site]["Count"] += 1
                    result["Safety"]["Grand_Total"] += 1
                logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
            except requests.exceptions.RequestException as e:
                logger.error(f"RequestException for chunk {current_chunk}: {str(e)}")
                error_placeholder.error(f"Failed to connect to WatsonX API for chunk {current_chunk}: {str(e)}")
                # Fallback: Manually process the chunk
                for record in chunk:
                    site = record["Tower"]
                    if site not in result["Safety"]["Sites"]:
                        result["Safety"]["Sites"][site] = {
                            "Count": 0,
                            "Descriptions": [],
                            "Created Date (WET)": [],
                            "Expected Close Date (WET)": [],
                            "Status": []
                        }
                    result["Safety"]["Sites"][site]["Descriptions"].append(record["Description"])
                    result["Safety"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                    result["Safety"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                    result["Safety"]["Sites"][site]["Status"].append(record["Status"])
                    result["Safety"]["Sites"][site]["Count"] += 1
                    result["Safety"]["Grand_Total"] += 1
                logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")
            except Exception as e:
                logger.error(f"Unexpected error during WatsonX API call for chunk {current_chunk}: {str(e)}")
                error_placeholder.error(f"Unexpected error during WatsonX API call for chunk {current_chunk}: {str(e)}")
                # Fallback: Manually process the chunk
                for record in chunk:
                    site = record["Tower"]
                    if site not in result["Safety"]["Sites"]:
                        result["Safety"]["Sites"][site] = {
                            "Count": 0,
                            "Descriptions": [],
                            "Created Date (WET)": [],
                            "Expected Close Date (WET)": [],
                            "Status": []
                        }
                    result["Safety"]["Sites"][site]["Descriptions"].append(record["Description"])
                    result["Safety"]["Sites"][site]["Created Date (WET)"].append(record["Created Date (WET)"])
                    result["Safety"]["Sites"][site]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                    result["Safety"]["Sites"][site]["Status"].append(record["Status"])
                    result["Safety"]["Sites"][site]["Count"] += 1
                    result["Safety"]["Grand_Total"] += 1
                logger.debug(f"Fallback processed chunk {current_chunk}/{total_chunks}")

        progress_bar.progress(100)
        status_placeholder.write(f"Processed {total_chunks}/{total_chunks} chunks (100%)")
        logger.debug(f"Final result before deduplication: {json.dumps(result, indent=2)}")

        for site in result["Safety"]["Sites"]:
            if "Descriptions" in result["Safety"]["Sites"][site]:
                result["Safety"]["Sites"][site]["Descriptions"] = list(result["Safety"]["Sites"][site]["Descriptions"])
            if "Created Date (WET)" in result["Safety"]["Sites"][site]:
                result["Safety"]["Sites"][site]["Created Date (WET)"] = list(result["Safety"]["Sites"][site]["Created Date (WET)"])
            if "Expected Close Date (WET)" in result["Safety"]["Sites"][site]:
                result["Safety"]["Sites"][site]["Expected Close Date (WET)"] = list(result["Safety"]["Sites"][site]["Expected Close Date (WET)"])
            if "Status" in result["Safety"]["Sites"][site]:
                result["Safety"]["Sites"][site]["Status"] = list(result["Safety"]["Sites"][site]["Status"])
        
        logger.debug(f"Final result after deduplication: {json.dumps(result, indent=2)}")
        return result, json.dumps(result)


@st.cache_data
def generate_consolidated_ncr_OpenClose_excel(combined_result, report_title="NCR"):
    # Create a new Excel writer
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        title_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': 'yellow',
            'border': 1,
            'font_size': 12
        })
        
        header_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })
        
        subheader_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        
        cell_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })
        
        site_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1
        })
        
        # Define standard sites
        standard_sites = [
            "Block 1 (B1) Banquet Hall",
            "Block 5 (B5) Admin + Member Lounge + Creche + AV Room + Surveillance Room + Toilets",
            "Block 6 (B6) Toilets",
            "Block 7 (B7) Indoor Sports",
            "Block 9 (B9) Spa & Saloon",
            "Block 8 (B8) Squash Court",
            "Block 2 & 3 (B2 & B3) Cafe & Bar",
            "Block 4 (B4) Indoor Swimming Pool Changing Room & Toilets",
            "Block 11 (B11) Guest House",
            "Block 10 (B10) Gym"
        ]
        
        # Extract day and month from report_title (format: "NCR: {day}_{month_name}")
        date_part = report_title.replace("NCR: ", "") if report_title.startswith("NCR: ") else "Date_Unknown"
        
        # Create summary worksheet (NCR Report)
        worksheet = workbook.add_worksheet('NCR Report')
        
        # Set column widths for summary sheet
        worksheet.set_column('A:A', 40)  # Site column
        worksheet.set_column('B:H', 12)  # Other columns
        
        # Write title
        worksheet.merge_range('A1:G1', f"NCR Summary Report: {date_part}", title_format)
        
        # Write headers for Open and Closed sections
        worksheet.merge_range('B2:D2', 'Open NCRs', subheader_format)
        worksheet.merge_range('E2:G2', 'Closed NCRs', subheader_format)
        
        # Write sub-headers
        headers = ['SW', 'FW', 'MEP', 'SW', 'FW', 'MEP']
        worksheet.write('A3', 'Site', header_format)
        for col, header in enumerate(headers, start=1):
            worksheet.write(2, col, header, header_format)
        
        # Initialize data for all standard sites
        open_data = {site: {'SW': 0, 'FW': 0, 'MEP': 0} for site in standard_sites}
        closed_data = {site: {'SW': 0, 'FW': 0, 'MEP': 0} for site in standard_sites}
        
        # Populate data from combined_result
        if 'Open' in combined_result and 'Sites' in combined_result['Open']:
            for site, values in combined_result['Open']['Sites'].items():
                if site in standard_sites:
                    open_data[site]['SW'] = values.get('SW', 0)
                    open_data[site]['FW'] = values.get('FW', 0)
                    open_data[site]['MEP'] = values.get('MEP', 0)
        
        if 'Closed' in combined_result and 'Sites' in combined_result['Closed']:
            for site, values in combined_result['Closed']['Sites'].items():
                if site in standard_sites:
                    closed_data[site]['SW'] = values.get('SW', 0)
                    closed_data[site]['FW'] = values.get('FW', 0)
                    closed_data[site]['MEP'] = values.get('MEP', 0)
        
        # Write data to summary sheet
        row = 3
        for site in standard_sites:
            worksheet.write(row, 0, site, site_format)
            worksheet.write(row, 1, open_data[site]['SW'], cell_format)
            worksheet.write(row, 2, open_data[site]['FW'], cell_format)
            worksheet.write(row, 3, open_data[site]['MEP'], cell_format)
            worksheet.write(row, 4, closed_data[site]['SW'], cell_format)
            worksheet.write(row, 5, closed_data[site]['FW'], cell_format)
            worksheet.write(row, 6, closed_data[site]['MEP'], cell_format)
            row += 1
        
        # Write totals
        worksheet.write(row, 0, 'Grand Total', header_format)
        for col in range(1, 7):
            formula = f'=SUM({chr(65+col)}4:{chr(65+col)}{row})'
            worksheet.write(row, col, formula, cell_format)
        
        # Create detailed data worksheets for Open and Closed NCRs
        for report_type in ['Open', 'Closed']:
            if report_type not in combined_result or 'Sites' not in combined_result[report_type]:
                continue
            
            worksheet = workbook.add_worksheet(f'{report_type} NCRs')
            
            # Set column widths for detailed sheet
            worksheet.set_column('A:A', 40)  # Site
            worksheet.set_column('B:B', 60)  # Description
            worksheet.set_column('C:D', 15)  # Dates
            worksheet.set_column('E:E', 10)  # Status
            worksheet.set_column('F:F', 15)  # Discipline
            worksheet.set_column('G:G', 20)  # Modules
            
            # Write title
            worksheet.merge_range('A1:G1', f'{report_type} NCR Detailed Report: {date_part}', title_format)
            
            # Write headers
            headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline', 'Modules']
            for col, header in enumerate(headers):
                worksheet.write(2, col, header, header_format)
            
            # Write data
            row = 3
            for site, values in combined_result[report_type]['Sites'].items():
                if site not in standard_sites:
                    continue
                descriptions = values.get('Descriptions', [])
                created_dates = values.get('Created Date (WET)', [])
                close_dates = values.get('Expected Close Date (WET)', [])
                statuses = values.get('Status', [])
                disciplines = values.get('Discipline', [])
                modules = values.get('Modules', [])
                
                # Ensure all lists have the same length
                max_len = max(len(descriptions), len(created_dates), len(close_dates), len(statuses), len(disciplines), len(modules))
                descriptions += [''] * (max_len - len(descriptions))
                created_dates += [''] * (max_len - len(created_dates))
                close_dates += [''] * (max_len - len(close_dates))
                statuses += [''] * (max_len - len(statuses))
                disciplines += [''] * (max_len - len(disciplines))
                modules += [[]] * (max_len - len(modules))
                
                for i in range(max_len):
                    worksheet.write(row, 0, site, site_format)
                    worksheet.write(row, 1, descriptions[i], cell_format)
                    worksheet.write(row, 2, created_dates[i], cell_format)
                    worksheet.write(row, 3, close_dates[i], cell_format)
                    worksheet.write(row, 4, statuses[i], cell_format)
                    worksheet.write(row, 5, disciplines[i], cell_format)
                    worksheet.write(row, 6, ', '.join(modules[i]) if modules[i] else 'Common', cell_format)
                    row += 1
            
            # Write total count
            worksheet.write(row, 0, 'Total NCRs', header_format)
            worksheet.write(row, 1, combined_result[report_type].get('Grand_Total', 0), cell_format)
    
    # Get the Excel file as bytes
    output.seek(0)
    return output.getvalue()
    
@st.cache_data
def generate_consolidated_ncr_Housekeeping_excel(combined_result, report_title="Housekeeping NCR"):
    # Create a new Excel writer
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        title_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': 'yellow',
            'border': 1,
            'font_size': 12
        })
        
        header_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })
        
        subheader_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        
        cell_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })
        
        site_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1
        })
        
        # Define standard sites
        standard_sites = [
            "Block 1 (B1) Banquet Hall",
            "Block 5 (B5) Admin + Member Lounge + Creche + AV Room + Surveillance Room + Toilets",
            "Block 6 (B6) Toilets",
            "Block 7 (B7) Indoor Sports",
            "Block 9 (B9) Spa & Saloon",
            "Block 8 (B8) Squash Court",
            "Block 2 & 3 (B2 & B3) Cafe & Bar",
            "Block 4 (B4) Indoor Swimming Pool Changing Room & Toilets",
            "Block 11 (B11) Guest House",
            "Block 10 (B10) Gym"
        ]
        
        # Extract day and month from report_title
        date_part = report_title.replace("Housekeeping NCR: ", "") if report_title.startswith("Housekeeping NCR: ") else "Date_Unknown"
        
        # Create summary worksheet
        worksheet = workbook.add_worksheet('Housekeeping NCR Report')
        
        # Set column widths for summary sheet
        worksheet.set_column('A:A', 40)  # Site column
        worksheet.set_column('B:C', 15)  # Open and Closed counts
        
        # Write title
        worksheet.merge_range('A1:C1', f"Housekeeping NCR Summary Report: {date_part}", title_format)
        
        # Write headers for Open and Closed sections
        worksheet.write('B2', 'Open NCRs', subheader_format)
        worksheet.write('C2', 'Closed NCRs', subheader_format)
        
        # Write sub-headers
        worksheet.write('A3', 'Site', header_format)
        worksheet.write('B3', 'Count', header_format)
        worksheet.write('C3', 'Count', header_format)
        
        # Initialize data for all standard sites
        open_data = {site: {'Count': 0} for site in standard_sites}
        closed_data = {site: {'Count': 0} for site in standard_sites}
        
        # Populate data from combined_result
        if 'Housekeeping' in combined_result:
            for report_type in ['Open', 'Closed']:
                if report_type in combined_result['Housekeeping'] and 'Sites' in combined_result['Housekeeping'][report_type]:
                    for site, values in combined_result['Housekeeping'][report_type]['Sites'].items():
                        if site in standard_sites:
                            data_dict = open_data if report_type == 'Open' else closed_data
                            data_dict[site]['Count'] = values.get('Count', 0)
        
        # Write data to summary sheet
        row = 3
        for site in standard_sites:
            worksheet.write(row, 0, site, site_format)
            worksheet.write(row, 1, open_data[site]['Count'], cell_format)
            worksheet.write(row, 2, closed_data[site]['Count'], cell_format)
            row += 1
        
        # Write totals
        worksheet.write(row, 0, 'Grand Total', header_format)
        worksheet.write(row, 1, f'=SUM(B4:B{row})', cell_format)
        worksheet.write(row, 2, f'=SUM(C4:C{row})', cell_format)
        
        # Create detailed data worksheets for Open and Closed NCRs
        for report_type in ['Open', 'Closed']:
            if 'Housekeeping' not in combined_result or report_type not in combined_result['Housekeeping'] or 'Sites' not in combined_result['Housekeeping'][report_type]:
                continue
            
            worksheet = workbook.add_worksheet(f'{report_type} Housekeeping NCRs')
            
            # Set column widths for detailed sheet
            worksheet.set_column('A:A', 40)  # Site
            worksheet.set_column('B:B', 60)  # Description
            worksheet.set_column('C:D', 15)  # Dates
            worksheet.set_column('E:E', 10)  # Status
            
            # Write title
            worksheet.merge_range('A1:E1', f'{report_type} Housekeeping NCR Detailed Report: {date_part}', title_format)
            
            # Write headers
            headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status']
            for col, header in enumerate(headers):
                worksheet.write(2, col, header, header_format)
            
            # Write data
            row = 3
            for site, values in combined_result['Housekeeping'][report_type]['Sites'].items():
                if site not in standard_sites:
                    continue
                descriptions = values.get('Descriptions', [])
                created_dates = values.get('Created Date (WET)', [])
                close_dates = values.get('Expected Close Date (WET)', [])
                statuses = values.get('Status', [])
                
                # Ensure all lists have the same length
                max_len = max(len(descriptions), len(created_dates), len(close_dates), len(statuses))
                descriptions += [''] * (max_len - len(descriptions))
                created_dates += [''] * (max_len - len(created_dates))
                close_dates += [''] * (max_len - len(close_dates))
                statuses += [''] * (max_len - len(statuses))
                
                for i in range(max_len):
                    worksheet.write(row, 0, site, site_format)
                    worksheet.write(row, 1, descriptions[i], cell_format)
                    worksheet.write(row, 2, created_dates[i], cell_format)
                    worksheet.write(row, 3, close_dates[i], cell_format)
                    worksheet.write(row, 4, statuses[i], cell_format)
                    row += 1
            
            # Write total count
            worksheet.write(row, 0, 'Total NCRs', header_format)
            worksheet.write(row, 1, combined_result['Housekeeping'][report_type].get('Grand_Total', 0), cell_format)
    
    # Get the Excel file as bytes
    output.seek(0)
    return output.getvalue()    
    
@st.cache_data
def generate_consolidated_ncr_Safety_excel(combined_result, report_title="Safety NCR"):
    # Create a new Excel writer
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        title_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': 'yellow',
            'border': 1,
            'font_size': 12
        })
        
        header_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })
        
        subheader_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        
        cell_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })
        
        site_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1
        })
        
        # Define standard sites
        standard_sites = [
            "Block 1 (B1) Banquet Hall",
            "Block 5 (B5) Admin + Member Lounge + Creche + AV Room + Surveillance Room + Toilets",
            "Block 6 (B6) Toilets",
            "Block 7 (B7) Indoor Sports",
            "Block 9 (B9) Spa & Saloon",
            "Block 8 (B8) Squash Court",
            "Block 2 & 3 (B2 & B3) Cafe & Bar",
            "Block 4 (B4) Indoor Swimming Pool Changing Room & Toilets",
            "Block 11 (B11) Guest House",
            "Block 10 (B10) Gym"
        ]
        
        # Extract day and month from report_title
        date_part = report_title.replace("Safety NCR: ", "") if report_title.startswith("Safety NCR: ") else "Date_Unknown"
        
        # Create summary worksheet
        worksheet = workbook.add_worksheet('Safety NCR Report')
        
        # Set column widths for summary sheet
        worksheet.set_column('A:A', 40)  # Site column
        worksheet.set_column('B:C', 15)  # Open and Closed counts
        
        # Write title
        worksheet.merge_range('A1:C1', f"Safety NCR Summary Report: {date_part}", title_format)
        
        # Write headers for Open and Closed sections
        worksheet.write('B2', 'Open NCRs', subheader_format)
        worksheet.write('C2', 'Closed NCRs', subheader_format)
        
        # Write sub-headers
        worksheet.write('A3', 'Site', header_format)
        worksheet.write('B3', 'Count', header_format)
        worksheet.write('C3', 'Count', header_format)
        
        # Initialize data for all standard sites
        open_data = {site: {'Count': 0} for site in standard_sites}
        closed_data = {site: {'Count': 0} for site in standard_sites}
        
        # Populate data from combined_result
        if 'Safety' in combined_result:
            for report_type in ['Open', 'Closed']:
                if report_type in combined_result['Safety'] and 'Sites' in combined_result['Safety'][report_type]:
                    for site, values in combined_result['Safety'][report_type]['Sites'].items():
                        if site in standard_sites:
                            data_dict = open_data if report_type == 'Open' else closed_data
                            data_dict[site]['Count'] = values.get('Count', 0)
        
        # Write data to summary sheet
        row = 3
        for site in standard_sites:
            worksheet.write(row, 0, site, site_format)
            worksheet.write(row, 1, open_data[site]['Count'], cell_format)
            worksheet.write(row, 2, closed_data[site]['Count'], cell_format)
            row += 1
        
        # Write totals
        worksheet.write(row, 0, 'Grand Total', header_format)
        worksheet.write(row, 1, f'=SUM(B4:B{row})', cell_format)
        worksheet.write(row, 2, f'=SUM(C4:C{row})', cell_format)
        
        # Create detailed data worksheets for Open and Closed NCRs
        for report_type in ['Open', 'Closed']:
            if 'Safety' not in combined_result or report_type not in combined_result['Safety'] or 'Sites' not in combined_result['Safety'][report_type]:
                continue
            
            worksheet = workbook.add_worksheet(f'{report_type} Safety NCRs')
            
            # Set column widths for detailed sheet
            worksheet.set_column('A:A', 40)  # Site
            worksheet.set_column('B:B', 60)  # Description
            worksheet.set_column('C:D', 15)  # Dates
            worksheet.set_column('E:E', 10)  # Status
            
            # Write title
            worksheet.merge_range('A1:E1', f'{report_type} Safety NCR Detailed Report: {date_part}', title_format)
            
            # Write headers
            headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status']
            for col, header in enumerate(headers):
                worksheet.write(2, col, header, header_format)
            
            # Write data
            row = 3
            for site, values in combined_result['Safety'][report_type]['Sites'].items():
                if site not in standard_sites:
                    continue
                descriptions = values.get('Descriptions', [])
                created_dates = values.get('Created Date (WET)', [])
                close_dates = values.get('Expected Close Date (WET)', [])
                statuses = values.get('Status', [])
                
                # Ensure all lists have the same length
                max_len = max(len(descriptions), len(created_dates), len(close_dates), len(statuses))
                descriptions += [''] * (max_len - len(descriptions))
                created_dates += [''] * (max_len - len(created_dates))
                close_dates += [''] * (max_len - len(close_dates))
                statuses += [''] * (max_len - len(statuses))
                
                for i in range(max_len):
                    worksheet.write(row, 0, site, site_format)
                    worksheet.write(row, 1, descriptions[i], cell_format)
                    worksheet.write(row, 2, created_dates[i], cell_format)
                    worksheet.write(row, 3, close_dates[i], cell_format)
                    worksheet.write(row, 4, statuses[i], cell_format)
                    row += 1
            
            # Write total count
            worksheet.write(row, 0, 'Total NCRs', header_format)
            worksheet.write(row, 1, combined_result['Safety'][report_type].get('Grand_Total', 0), cell_format)
    
    # Get the Excel file as bytes
    output.seek(0)
    return output.getvalue()
    
    
@st.cache_data
def generate_combined_excel_report(ncr_result, housekeeping_result, safety_result, report_title="NCR Report", project_name="", form_name="", closed_start=None, closed_end=None, open_end=None):
    """
    Generate an Excel file with sheets for NCR Report, Open NCR Details, Safety NCR,
    Housekeeping NCR, Safety NCR Details, and Housekeeping NCR Details, matching the EDEN_NCR.xlsx format.
    
    Args:
        ncr_result (dict): Dictionary containing NCR Open and Closed results.
        housekeeping_result (dict): Dictionary containing Housekeeping Open and Closed results.
        safety_result (dict): Dictionary containing Safety Open and Closed results.
        report_title (str): Title of the report.
        project_name (str): Name of the project.
        form_name (str): Name of the form.
        closed_start (datetime): Start date for closed reports.
        closed_end (datetime): End date for closed reports.
        open_end (datetime): Until date for open reports.
    
    Returns:
        bytes: Excel file as bytes.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        title_format = workbook.add_format({
            'bold': True, 'align': 'center', 'valign': 'vcenter', 'fg_color': 'yellow', 'border': 1, 'font_size': 12
        })
        header_format = workbook.add_format({
            'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True
        })
        subheader_format = workbook.add_format({
            'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1
        })
        cell_format = workbook.add_format({
            'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True
        })
        site_format = workbook.add_format({
            'align': 'left', 'valign': 'vcenter', 'border': 1
        })
        
        # Define standard sites (from EDEN_NCR.xlsx)
        standard_sites = [
            "Block 1 (B1) Banquet Hall",
            "Block 5 (B5) Admin + Member Lounge + Creche + AV Room + Surveillance Room + Toilets",
            "Block 6 (B6) Toilets",
            "Block 7 (B7) Indoor Sports",
            "Block 9 (B9) Spa & Saloon",
            "Block 8 (B8) Squash Court",
            "Block 2 & 3 (B2 & B3) Cafe & Bar",
            "Block 4 (B4) Indoor Swimming Pool Changing Room & Toilets",
            "Block 11 (B11) Guest House",
            "Block 10 (B10) Gym"
        ]
        
        # Log sites in ncr_result for debugging
        if ncr_result:
            open_sites = ncr_result.get('Open', {}).get('Sites', {}).keys()
            closed_sites = ncr_result.get('Closed', {}).get('Sites', {}).keys()
            logger.debug(f"Open NCR Sites: {list(open_sites)}")
            logger.debug(f"Closed NCR Sites: {list(closed_sites)}")
            for site in open_sites | closed_sites:
                if site not in standard_sites:
                    logger.warning(f"Site '{site}' not in standard_sites")
        
        # Extract day and month
        now = datetime.now()
        day = now.strftime("%d")
        month_name = now.strftime("%B")
        date_part = f"{day}_{month_name}"
        
        # NCR Report Sheet
        worksheet = workbook.add_worksheet('NCR Report')
        worksheet.set_column('A:A', 50)
        worksheet.set_column('B:G', 15)
        worksheet.set_column('H:H', 10)
        worksheet.merge_range('A1:H1', f'NCR Report: {date_part}', title_format)
        worksheet.merge_range('B2:D2', 'NCR resolved beyond 21 days', subheader_format)
        worksheet.merge_range('E2:G2', 'NCR open beyond 21 days', subheader_format)
        headers = ['Site', 'Civil Finishing', 'MEP', 'Structure', 'Civil Finishing', 'MEP', 'Structure', 'Total']
        for col, header in enumerate(headers):
            worksheet.write(2, col, header, header_format)
        
        # Initialize data
        ncr_open_data = {site: {'Civil Finishing': 0, 'MEP': 0, 'Structure': 0} for site in standard_sites}
        ncr_closed_data = {site: {'Civil Finishing': 0, 'MEP': 0, 'Structure': 0} for site in standard_sites}
        
        # Populate NCR data
        if ncr_result:
            if 'Open' in ncr_result and 'Sites' in ncr_result['Open']:
                for site, values in ncr_result['Open']['Sites'].items():
                    if site in standard_sites:
                        ncr_open_data[site]['Civil Finishing'] = values.get('FW', 0)
                        ncr_open_data[site]['MEP'] = values.get('MEP', 0)
                        ncr_open_data[site]['Structure'] = values.get('SW', 0)
                        logger.debug(f"Open NCR for {site}: {values}")
            if 'Closed' in ncr_result and 'Sites' in ncr_result['Closed']:
                for site, values in ncr_result['Closed']['Sites'].items():
                    if site in standard_sites:
                        ncr_closed_data[site]['Civil Finishing'] = values.get('FW', 0)
                        ncr_closed_data[site]['MEP'] = values.get('MEP', 0)
                        ncr_closed_data[site]['Structure'] = values.get('SW', 0)
                        logger.debug(f"Closed NCR for {site}: {values}")
        
        # Write NCR data
        row = 3
        for site in standard_sites:
            worksheet.write(row, 0, site, site_format)
            worksheet.write(row, 1, ncr_closed_data[site]['Civil Finishing'], cell_format)
            worksheet.write(row, 2, ncr_closed_data[site]['MEP'], cell_format)
            worksheet.write(row, 3, ncr_closed_data[site]['Structure'], cell_format)
            worksheet.write(row, 4, ncr_open_data[site]['Civil Finishing'], cell_format)
            worksheet.write(row, 5, ncr_open_data[site]['MEP'], cell_format)
            worksheet.write(row, 6, ncr_open_data[site]['Structure'], cell_format)
            total = (ncr_closed_data[site]['Civil Finishing'] + ncr_closed_data[site]['MEP'] + ncr_closed_data[site]['Structure'] +
                     ncr_open_data[site]['Civil Finishing'] + ncr_open_data[site]['MEP'] + ncr_open_data[site]['Structure'])
            worksheet.write(row, 7, total, cell_format)
            row += 1
        
        # Write totals
        worksheet.write(row, 0, 'Total All Towers', header_format)
        for col in range(1, 8):
            formula = f'=SUM({chr(65+col)}4:{chr(65+col)}{row})'
            worksheet.write(row, col, formula, cell_format)
        
        # Open NCR Details Sheet
        worksheet = workbook.add_worksheet('Open NCR Details')
        worksheet.set_column('A:A', 50)
        worksheet.set_column('B:B', 100)
        worksheet.set_column('C:D', 20)
        worksheet.set_column('E:E', 10)
        worksheet.set_column('F:F', 15)
        worksheet.merge_range('A1:F1', f'Open NCR Details: {date_part}', title_format)
        headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline']
        for col, header in enumerate(headers):
            worksheet.write(1, col, header, header_format)
        
        row = 2
        if ncr_result and 'Open' in ncr_result and 'Sites' in ncr_result['Open']:
            for site, values in ncr_result['Open']['Sites'].items():
                if site not in standard_sites:
                    continue
                descriptions = values.get('Descriptions', [])
                created_dates = values.get('Created Date (WET)', [])
                close_dates = values.get('Expected Close Date (WET)', [])
                statuses = values.get('Status', [])
                disciplines = values.get('Discipline', [])
                logger.debug(f"Open NCR Details for {site}: Descriptions={len(descriptions)}, Dates={len(created_dates)}")
                max_len = max(len(descriptions), len(created_dates), len(close_dates), len(statuses), len(disciplines))
                descriptions += [''] * (max_len - len(descriptions))
                created_dates += [''] * (max_len - len(created_dates))
                close_dates += [''] * (max_len - len(close_dates))
                statuses += [''] * (max_len - len(statuses))
                disciplines += [''] * (max_len - len(disciplines))
                for i in range(max_len):
                    if descriptions[i]:  # Only write if description exists
                        worksheet.write(row, 0, site, site_format)
                        worksheet.write(row, 1, descriptions[i], cell_format)
                        worksheet.write(row, 2, created_dates[i], cell_format)
                        worksheet.write(row, 3, close_dates[i], cell_format)
                        worksheet.write(row, 4, statuses[i], cell_format)
                        worksheet.write(row, 5, disciplines[i], cell_format)
                        row += 1
        if row == 2:
            worksheet.write(row, 0, 'No records found', cell_format)
            logger.warning("No Open NCR records found")
        
        # Safety NCR Sheet
        worksheet = workbook.add_worksheet('Safety NCR')
        worksheet.set_column('A:A', 50)
        worksheet.set_column('B:C', 20)
        worksheet.merge_range('A1:C1', f'Safety NCR: {date_part}', title_format)
        headers = ['Site', 'Closed Safety NCRs beyond 7 days', 'Open Safety NCRs beyond 7 days']
        for col, header in enumerate(headers):
            worksheet.write(2, col, header, header_format)
        
        safety_open_data = {site: {'Count': 0} for site in standard_sites}
        safety_closed_data = {site: {'Count': 0} for site in standard_sites}
        
        if safety_result and 'Safety' in safety_result:
            for report_type in ['Open', 'Closed']:
                if report_type in safety_result['Safety'] and 'Sites' in safety_result['Safety'][report_type]:
                    for site, values in safety_result['Safety'][report_type]['Sites'].items():
                        if site in standard_sites:
                            data_dict = safety_open_data if report_type == 'Open' else safety_closed_data
                            data_dict[site]['Count'] = values.get('Count', 0)
                            logger.debug(f"{report_type} Safety for {site}: {values}")
        
        row = 3
        for site in standard_sites:
            worksheet.write(row, 0, site, site_format)
            worksheet.write(row, 1, safety_closed_data[site]['Count'], cell_format)
            worksheet.write(row, 2, safety_open_data[site]['Count'], cell_format)
            row += 1
        
        worksheet.write(row, 0, 'Total All Blocks', header_format)
        for col in range(1, 3):
            formula = f'=SUM({chr(65+col)}4:{chr(65+col)}{row})'
            worksheet.write(row, col, formula, cell_format)
        
        # Housekeeping NCR Sheet
        worksheet = workbook.add_worksheet('Housekeeping NCR')
        worksheet.set_column('A:A', 50)
        worksheet.set_column('B:C', 20)
        worksheet.merge_range('A1:C1', f'Housekeeping NCR: {date_part}', title_format)
        headers = ['Site', 'Closed Housekeeping NCRs beyond 7 days', 'Open Housekeeping NCRs beyond 7 days']
        for col, header in enumerate(headers):
            worksheet.write(2, col, header, header_format)
        
        housekeeping_open_data = {site: {'Count': 0} for site in standard_sites}
        housekeeping_closed_data = {site: {'Count': 0} for site in standard_sites}
        
        if housekeeping_result and 'Housekeeping' in housekeeping_result:
            for report_type in ['Open', 'Closed']:
                if report_type in housekeeping_result['Housekeeping'] and 'Sites' in housekeeping_result['Housekeeping'][report_type]:
                    for site, values in housekeeping_result['Housekeeping'][report_type]['Sites'].items():
                        if site in standard_sites:
                            data_dict = housekeeping_open_data if report_type == 'Open' else housekeeping_closed_data
                            data_dict[site]['Count'] = values.get('Count', 0)
                            logger.debug(f"{report_type} Housekeeping for {site}: {values}")
        
        row = 3
        for site in standard_sites:
            worksheet.write(row, 0, site, site_format)
            worksheet.write(row, 1, housekeeping_closed_data[site]['Count'], cell_format)
            worksheet.write(row, 2, housekeeping_open_data[site]['Count'], cell_format)
            row += 1
        
        worksheet.write(row, 0, 'Total All Blocks', header_format)
        for col in range(1, 3):
            formula = f'=SUM({chr(65+col)}4:{chr(65+col)}{row})'
            worksheet.write(row, col, formula, cell_format)
        
        # Safety NCR Details Sheet
        worksheet = workbook.add_worksheet('Safety NCR Details')
        worksheet.set_column('A:A', 50)
        worksheet.set_column('B:B', 100)
        worksheet.set_column('C:D', 20)
        worksheet.set_column('E:E', 10)
        worksheet.set_column('F:F', 15)
        worksheet.merge_range('A1:F1', f'Safety NCR Details: {date_part}', title_format)
        headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline']
        for col, header in enumerate(headers):
            worksheet.write(1, col, header, header_format)
        
        row = 2
        if safety_result and 'Safety' in safety_result:
            for status in ['Open', 'Closed']:
                if status in safety_result['Safety'] and 'Sites' in safety_result['Safety'][status]:
                    for site, values in safety_result['Safety'][status]['Sites'].items():
                        if site not in standard_sites:
                            continue
                        descriptions = values.get('Descriptions', [])
                        created_dates = values.get('Created Date (WET)', [])
                        close_dates = values.get('Expected Close Date (WET)', [])
                        statuses = values.get('Status', [])
                        disciplines = values.get('Discipline', [])
                        logger.debug(f"{status} Safety Details for {site}: Descriptions={len(descriptions)}")
                        max_len = max(len(descriptions), len(created_dates), len(close_dates), len(statuses), len(disciplines))
                        descriptions += [''] * (max_len - len(descriptions))
                        created_dates += [''] * (max_len - len(created_dates))
                        close_dates += [''] * (max_len - len(close_dates))
                        statuses += [''] * (max_len - len(statuses))
                        disciplines += [''] * (max_len - len(disciplines))
                        for i in range(max_len):
                            if descriptions[i]:
                                worksheet.write(row, 0, site, site_format)
                                worksheet.write(row, 1, descriptions[i], cell_format)
                                worksheet.write(row, 2, created_dates[i], cell_format)
                                worksheet.write(row, 3, close_dates[i], cell_format)
                                worksheet.write(row, 4, statuses[i], cell_format)
                                worksheet.write(row, 5, disciplines[i], cell_format)
                                row += 1
        if row == 2:
            worksheet.write(row, 0, 'No records found', cell_format)
            logger.warning("No Safety NCR records found")
        
        # Housekeeping NCR Details Sheet
        worksheet = workbook.add_worksheet('Housekeeping NCR Details')
        worksheet.set_column('A:A', 50)
        worksheet.set_column('B:B', 100)
        worksheet.set_column('C:D', 20)
        worksheet.set_column('E:E', 10)
        worksheet.set_column('F:F', 15)
        worksheet.merge_range('A1:F1', f'Housekeeping NCR Details: {date_part}', title_format)
        headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline']
        for col, header in enumerate(headers):
            worksheet.write(1, col, header, header_format)
        
        row = 2
        if housekeeping_result and 'Housekeeping' in housekeeping_result:
            for status in ['Open', 'Closed']:
                if status in housekeeping_result['Housekeeping'] and 'Sites' in housekeeping_result['Housekeeping'][status]:
                    for site, values in housekeeping_result['Housekeeping'][status]['Sites'].items():
                        if site not in standard_sites:
                            continue
                        descriptions = values.get('Descriptions', [])
                        created_dates = values.get('Created Date (WET)', [])
                        close_dates = values.get('Expected Close Date (WET)', [])
                        statuses = values.get('Status', [])
                        disciplines = values.get('Discipline', [])
                        logger.debug(f"{status} Housekeeping Details for {site}: Descriptions={len(descriptions)}")
                        max_len = max(len(descriptions), len(created_dates), len(close_dates), len(statuses), len(disciplines))
                        descriptions += [''] * (max_len - len(descriptions))
                        created_dates += [''] * (max_len - len(created_dates))
                        close_dates += [''] * (max_len - len(close_dates))
                        statuses += [''] * (max_len - len(statuses))
                        disciplines += [''] * (max_len - len(disciplines))
                        for i in range(max_len):
                            if descriptions[i]:
                                worksheet.write(row, 0, site, site_format)
                                worksheet.write(row, 1, descriptions[i], cell_format)
                                worksheet.write(row, 2, created_dates[i], cell_format)
                                worksheet.write(row, 3, close_dates[i], cell_format)
                                worksheet.write(row, 4, statuses[i], cell_format)
                                worksheet.write(row, 5, disciplines[i], cell_format)
                                row += 1
        if row == 2:
            worksheet.write(row, 0, 'No records found', cell_format)
            logger.warning("No Housekeeping NCR records found")
    
    output.seek(0)
    return output.getvalue()

# Replace the project_dropdown function with this
def project_dropdown():
    project_options = [
        "WAVE CITY CLUB @ PSP 14A",
        "EWS_LIG Veridia PH04",
        "GH-8 Phase-2 (ELIGO) Wave City",
        "GH-8 Phase-3 (EDEN) Wave City",
        "Wave Oakwood, Wave City"
    ]
    project_name = st.sidebar.selectbox(
        "Project Name",
        options=project_options,
        index=project_options.index("Wave Oakwood, Wave City") if "Wave Oakwood, Wave City" in project_options else 0,
        key="project_name_selectbox",
        help="Select a project to fetch data and generate individual reports."
    )
    form_name = st.sidebar.text_input(
        "Form Name",
        "Non Conformity Report",
        key="form_name_input",
        help="Enter the form name for the report."
    )
    return project_name, form_name, project_options


# Streamlit UI
st.title("NCR Safety Housekeeping Reports")

# Initialize session state for each report type
if "ncr_df" not in st.session_state:
    st.session_state["ncr_df"] = None
if "safety_df" not in st.session_state:
    st.session_state["safety_df"] = None
if "housekeeping_df" not in st.session_state:
    st.session_state["housekeeping_df"] = None
if "session_id" not in st.session_state:
    st.session_state["session_id"] = None

# Sidebar: Asite Login Section
st.sidebar.title("🔒 Asite Login")
email = st.sidebar.text_input("Email", "impwatson@gadieltechnologies.com", key="email_input")
password = st.sidebar.text_input("Password","Srihari@790$", type="password", key="password_input")

# Sidebar: Login Button
if st.sidebar.button("Login", key="login_button"):
    session_id = login_to_asite(email, password)
    if session_id:
        st.session_state["session_id"] = session_id
        st.sidebar.success("✅ Login Successful")

# Sidebar: Project Data Section
st.sidebar.title("📂 Project Data")
project_name, form_name, project_options = project_dropdown()
st.session_state["project_options"] = project_options

# Sidebar: Fetch Data Button
if "session_id" in st.session_state:
    if st.sidebar.button("Fetch Data", key="fetch_data"):
        header, data, payload = fetch_project_data(st.session_state["session_id"], project_name, form_name)
        st.json(header)
        if data:
            df = process_json_data(data)
            st.session_state["ncr_df"] = df.copy()
            st.session_state["safety_df"] = df.copy()
            st.session_state["housekeeping_df"] = df.copy()
            st.dataframe(df)
            st.success("✅ Data fetched and processed successfully for all report types!")
else:
    st.sidebar.warning("Please login first to fetch data.")

# Report Generation Section
st.sidebar.title("📋 Combined NCR Report")
# Date inputs for filtering reports
if st.session_state["ncr_df"] is not None:
    ncr_df = st.session_state["ncr_df"]
    min_date = ncr_df.get('Expected Close Date (WET)', pd.Series([])).min()
    max_date = ncr_df.get('Expected Close Date (WET)', pd.Series([])).max()
    closed_start = st.sidebar.date_input("Closed Start Date", 
                                       min_date if pd.notna(min_date) else datetime.now(),
                                       key="ncr_closed_start")
    closed_end = st.sidebar.date_input("Closed End Date", 
                                     max_date if pd.notna(max_date) else datetime.now(),
                                     key="ncr_closed_end")
    open_end = st.sidebar.date_input("Open Until Date", datetime.now(),
                                   key="ncr_open_end")
else:
    closed_start = st.sidebar.date_input("Closed Start Date", datetime.now(),
                                      key="ncr_closed_start")
    closed_end = st.sidebar.date_input("Closed End Date", datetime.now(),
                                     key="ncr_closed_end")
    open_end = st.sidebar.date_input("Open Until Date", datetime.now(),
                                   key="ncr_open_end")

# Generate Combined NCR Report (Open & Close)
if st.sidebar.button("NCR(Open&Close) Report", key="generate_report_button"):
    if st.session_state["ncr_df"] is not None:
        ncr_df = st.session_state["ncr_df"]
        month_name = closed_end.strftime("%B")
        now = datetime.now()
        day = now.strftime("%d")  
        report_title = f"NCR: {day}_{month_name}"
        
        closed_result, closed_raw = generate_ncr_report(ncr_df, "Closed", closed_start, closed_end)
        open_result, open_raw = generate_ncr_report(ncr_df, "Open", open_end)

        combined_result = {}
        if "error" not in closed_result:
            combined_result["NCR resolved beyond 21 days"] = closed_result["Closed"]
        else:
            combined_result["NCR resolved beyond 21 days"] = {"error": closed_result["error"]}
        
        if "error" not in open_result:
            combined_result["NCR open beyond 21 days"] = open_result["Open"]
        else:
            combined_result["NCR open beyond 21 days"] = {"error": open_result["error"]}

        st.subheader("Combined NCR Report (JSON)")
        st.json(combined_result)
        
        excel_file = generate_consolidated_ncr_OpenClose_excel(combined_result, report_title)
        st.download_button(
            label="📥 Download Excel Report",
            data=excel_file,
            file_name=f"NCR_Report_{day}_{month_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.error("Please fetch data first!")

# Generate Safety NCR Report
if st.sidebar.button("Safety NCR Report", key="safety_ncr"):
    if st.session_state["safety_df"] is not None:
        safety_df = st.session_state["safety_df"]
        month_name = closed_end.strftime("%B")
        now = datetime.now()
        day = now.strftime("%d")
        report_title = f"Safety_NCR: {day}_{month_name}"

        closed_result, closed_raw = generate_ncr_Safety_report(
            safety_df,
            report_type="Closed",
            start_date=closed_start.strftime('%Y/%m/%d') if closed_start else None,
            end_date=closed_end.strftime('%Y/%m/%d') if closed_end else None,
            open_until_date=None
        )
        st.subheader("Closed Safety NCR Report (JSON)")
        st.json(closed_result)
        excel_closed = generate_consolidated_ncr_Safety_excel(closed_result, f"Safety: Closed - {month_name}_{day}")
        st.download_button(
            label="📥 Download Closed Safety Excel Report",
            data=excel_closed,
            file_name=f"Safety_NCR_Report_Closed_{month_name}_{datetime.now().strftime('%Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_safety_closed"
        )

        open_result, open_raw = generate_ncr_Safety_report(
            safety_df,
            report_type="Open",
            start_date=None,
            end_date=None,
            open_until_date=open_end.strftime('%Y/%m/%d') if open_end else None
        )
        st.subheader("Open Safety NCR Report (JSON)")
        st.json(open_result)
        excel_open = generate_consolidated_ncr_Safety_excel(open_result, f"Safety: Open - {month_name}")
        st.download_button(
            label="📥 Download Open Safety Excel Report",
            data=excel_open,
            file_name=f"Safety_NCR_Report_Open_{month_name}_{datetime.now().strftime('%Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_safety_open"
        )
    else:
        st.error("Please fetch data first!")

# Generate Housekeeping NCR Report
if st.sidebar.button("Housekeeping NCR Report", key="housekeeping_ncr"):
    if st.session_state["housekeeping_df"] is not None:
        housekeeping_df = st.session_state["housekeeping_df"]
        month_name = closed_end.strftime("%B")
        report_title = f"Housekeeping: {month_name}"

        closed_result, closed_raw = generate_ncr_Housekeeping_report(
            housekeeping_df,
            report_type="Closed",
            start_date=closed_start.strftime('%Y/%m/%d') if closed_start else None,
            end_date=closed_end.strftime('%Y/%m/%d') if closed_end else None,
            open_until_date=None
        )
        st.subheader("Closed Housekeeping NCR Report (JSON)")
        st.json(closed_result)
        excel_closed = generate_consolidated_ncr_Housekeeping_excel(closed_result, f"Housekeeping: Closed - {month_name}")
        st.download_button(
            label="📥 Download Closed Housekeeping Excel Report",
            data=excel_closed,
            file_name=f"Housekeeping_NCR_Report_Closed_{month_name}_{datetime.now().strftime('%Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_housekeeping_closed"
        )

        open_result, open_raw = generate_ncr_Housekeeping_report(
            housekeeping_df,
            report_type="Open",
            start_date=None,
            end_date=None,
            open_until_date=open_end.strftime('%Y/%m/%d') if open_end else None
        )
        st.subheader("Open Housekeeping NCR Report (JSON)")
        st.json(open_result)
        excel_open = generate_consolidated_ncr_Housekeeping_excel(open_result, f"Housekeeping: Open - {month_name}")
        st.download_button(
            label="📥 Download Open Housekeeping Excel Report",
            data=excel_open,
            file_name=f"Housekeeping_NCR_Report_Open_{month_name}_{datetime.now().strftime('%Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_housekeeping_open"
        )
    else:
        st.error("Please fetch data first!")

# All Reports Button
if st.sidebar.button("All_Report", key="All_Report"):
    if all(key in st.session_state and st.session_state[key] is not None 
           for key in ["ncr_df", "safety_df", "housekeeping_df"]):
        ncr_df = st.session_state["ncr_df"]
        safety_df = st.session_state["safety_df"]
        housekeeping_df = st.session_state["housekeeping_df"]
        if open_end is None:
            st.error("❌ Please select an Open Until Date.")
            logger.error("Open Until Date is None during All_Report generation")
        else:
            # Generate Combined NCR Report
            month_name = closed_end.strftime("%B")
            now = datetime.now()
            day = now.strftime("%d")
            report_title_ncr = f"NCR: {day}_{month_name}"
            closed_result_ncr, closed_raw_ncr = generate_ncr_report(ncr_df, "Closed", closed_start, closed_end)
            open_result_ncr, open_raw_ncr = generate_ncr_report(ncr_df, "Open", Until_Date=open_end)

            combined_result_ncr = {}
            if "error" not in closed_result_ncr:
                combined_result_ncr["Closed"] = closed_result_ncr["Closed"]
            else:
                combined_result_ncr["Closed"] = {"error": closed_result_ncr["error"]}
                logger.warning(f"Error in NCR Closed report: {closed_result_ncr['error']}")
            if "error" not in open_result_ncr:
                combined_result_ncr["Open"] = open_result_ncr["Open"]
            else:
                combined_result_ncr["Open"] = {"error": open_result_ncr["error"]}
                logger.warning(f"Error in NCR Open report: {open_result_ncr['error']}")

            # Generate Safety NCR Report
            report_title_safety = f"Safety_NCR: {day}_{month_name}"
            closed_result_safety, closed_raw_safety = generate_ncr_Safety_report(
                safety_df,
                report_type="Closed",
                start_date=closed_start.strftime('%Y/%m/%d') if closed_start else None,
                end_date=closed_end.strftime('%Y/%m/%d') if closed_end else None,
                open_until_date=None
            )
            open_result_safety, open_raw_safety = generate_ncr_Safety_report(
                safety_df,
                report_type="Open",
                start_date=None,
                end_date=None,
                open_until_date=open_end.strftime('%Y/%m/%d') if open_end else None
            )

            # Generate Housekeeping NCR Report
            report_title_housekeeping = f"Housekeeping: {day}_{month_name}"
            closed_result_housekeeping, closed_raw_housekeeping = generate_ncr_Housekeeping_report(
                housekeeping_df,
                report_type="Closed",
                start_date=closed_start.strftime('%Y/%m/%d') if closed_start else None,
                end_date=closed_end.strftime('%Y/%m/%d') if closed_end else None,
                open_until_date=None
            )
            open_result_housekeeping, open_raw_housekeeping = generate_ncr_Housekeeping_report(
                housekeeping_df,
                report_type="Open",
                start_date=None,
                end_date=None,
                open_until_date=open_end.strftime('%Y/%m/%d') if open_end else None
            )

            # Combine all results into a single dictionary
            all_reports = {
                "NCR": combined_result_ncr,
                "Safety": {
                    "Closed": closed_result_safety.get("Safety", {}),
                    "Open": open_result_safety.get("Safety", {})
                },
                "Housekeeping": {
                    "Closed": closed_result_housekeeping.get("Housekeeping", {}),
                    "Open": open_result_housekeeping.get("Housekeeping", {})
                }
            }

            # Debug: Log the all_reports structure
            logger.debug(f"all_reports structure: {json.dumps(all_reports, indent=2)}")

            # Display JSON outputs for verification
            st.subheader("NCR Report (JSON)")
            st.json(combined_result_ncr)
            st.subheader("Safety NCR Closed Report (JSON)")
            st.json(closed_result_safety)
            st.subheader("Safety NCR Open Report (JSON)")
            st.json(open_result_safety)
            st.subheader("Housekeeping NCR Closed Report (JSON)")
            st.json(closed_result_housekeeping)
            st.subheader("Housekeeping NCR Open Report (JSON)")
            st.json(open_result_housekeeping)

            # Generate and download a single Excel file with multiple sheets
            try:
                logger.debug(f"Calling generate_combined_excel_report with NCR: {combined_result_ncr}, "
                             f"Housekeeping: {all_reports['Housekeeping']}, "
                             f"Safety: {all_reports['Safety']}, "
                             f"Title: NCR_Report_{day}_{month_name}")
                excel_file = generate_combined_excel_report(
                    all_reports["NCR"],
                    {"Housekeeping": all_reports["Housekeeping"]},
                    {"Safety": all_reports["Safety"]},
                    f"NCR_Report_{day}_{month_name}",
                    project_name,
                    form_name,
                    closed_start,
                    closed_end,
                    open_end
                )
                st.download_button(
                    label="📥 Download All Reports Excel",
                    data=excel_file,
                    file_name=f"NCR_Report_{day}_{month_name}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_all_reports"
                )
            except Exception as e:
                st.error(f"❌ Failed to generate Excel report: {str(e)}")
                logger.error(f"Excel generation error: {str(e)}", exc_info=True)
    else:
        st.error("Please fetch data first!")
        logger.warning("Attempted to generate All_Report without fetched data")