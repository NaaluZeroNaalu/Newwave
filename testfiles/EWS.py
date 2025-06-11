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
import numpy as np
import xlsxwriter



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


def generate_report_title(report_type, end_date):
    """
    Generate a formatted title for the report based on the report type and end date.
    
    Args:
        report_type (str): Type of report ("NCR", "Housekeeping", "Safety").
        end_date (str or datetime): The end date for the report period.
    
    Returns:
        str: Formatted report title.
    """
    # Convert end_date to a string in a consistent format (e.g., YYYY-MM-DD)
    if end_date:
        try:
            end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            end_date_str = str(end_date)  # Fallback if date parsing fails
    else:
        end_date_str = "Unknown Date"

    # Create the title based on the report type
    return f"{report_type} Report - Up to {end_date_str}"

# Function to generate access token
def get_access_token(API_KEY_2):
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
def generate_ncr_report(df, report_type, start_date=None, end_date=None,open_until_date=None):
    with st.spinner(f"Generating {report_type} NCR Report..."):
        # Define standard_sites within the function to avoid scope issues
        standard_sites = [
            "EWS Tower 1",
            "EWS Tower 2",
            "EWS Tower 3",
            "LIG Tower 3",
            "LIG Tower 2",
            "LIG Tower 1"
        ]
        logger.debug(f"standard_sites defined: {standard_sites}")

        # Filter based on Created Date (WET) range and pre-calculated Days > 20
        if report_type == "Closed":
            filtered_df = df[
                (df['Status'] == 'Closed') &
                (df['Created Date (WET)'] >= pd.to_datetime(start_date)) &
                (df['Expected Close Date (WET)'] <= pd.to_datetime(end_date)) &
                (df['Days'] > 21)
            ].copy()
        else:  # Open
            today = pd.to_datetime(datetime.today().strftime('%Y/%m/%d'))  # Updated to use current date
            filtered_df = df[
                (df['Status'] == 'Open') &
                (df['Created Date (WET)'].notna())
            ].copy()
            filtered_df.loc[:, 'Days_From_Today'] = (today - pd.to_datetime(filtered_df['Created Date (WET)'])).dt.days
            filtered_df = filtered_df[filtered_df['Days_From_Today'] > 21].copy()

        if filtered_df.empty:
            return {"error": f"No {report_type} records found with duration > 20 days"}, ""

        filtered_df.loc[:, 'Created Date (WET)'] = filtered_df['Created Date (WET)'].astype(str)
        filtered_df.loc[:, 'Expected Close Date (WET)'] = filtered_df['Expected Close Date (WET)'].astype(str)

        processed_data = filtered_df.to_dict(orient="records")
        
        cleaned_data = []
        for record in processed_data:
            cleaned_record = {
                "Description": str(record.get("Description", "")),
                "Discipline": str(record.get("Discipline", "")),
                "Created Date (WET)": str(record.get("Created Date (WET)", "")),
                "Expected Close Date (WET)": str(record.get("Expected Close Date (WET)", "")),
                "Status": str(record.get("Status", "")),
                "Days": record.get("Days", 0),
                "Tower": "Unknown Site"  # Default site if standard_sites fails
            }
            if report_type == "Open":
                cleaned_record["Days_From_Today"] = int(record.get("Days_From_Today", 0))  # Ensure integer

            description = cleaned_record["Description"].lower()
            # Check for EWS or LIG towers
            tower_match = re.search(r'(ews|lig)\s*(tower)?\s*-?\s*(\d+)', description, re.IGNORECASE)
            if tower_match:
                category = tower_match.group(1).upper()  # EWS or LIG
                num = tower_match.group(3)  # Tower number
                cleaned_record["Tower"] = f"{category} Tower {num}"
                logger.debug(f"Matched tower: {tower_match.group(0)}, set Tower to {cleaned_record['Tower']}")
            else:
                # Use standard_sites[0] if available, otherwise fallback to "Unknown Site"
                if standard_sites:
                    cleaned_record["Tower"] = standard_sites[0]  # Default to "EWS Tower 1"
                    logger.debug(f"No tower match in description: {description}, defaulting to {cleaned_record['Tower']}")
                else:
                    cleaned_record["Tower"] = "Unknown Site"
                    logger.warning(f"standard_sites is empty, defaulting Tower to 'Unknown Site'")

            discipline = cleaned_record["Discipline"].strip().lower()
            if "structure" in discipline or "sw" in discipline:
                cleaned_record["Discipline_Category"] = "SW"
            elif "civil" in discipline or "finishing" in discipline or "fw" in discipline:
                cleaned_record["Discipline_Category"] = "FW"
            else:
                cleaned_record["Discipline_Category"] = "MEP"

            cleaned_data.append(cleaned_record)
            logger.debug(f"Processed record: {json.dumps(cleaned_record, indent=2)}")

        # Remove duplicates to prevent overcounting
        cleaned_data = [dict(t) for t in {tuple(sorted(d.items())) for d in cleaned_data}]

        st.write(f"Debug - Total {report_type} records to process: {len(cleaned_data)}")
        logger.debug(f"Processed data: {json.dumps(cleaned_data, indent=2)}")

        if not cleaned_data:
            return {report_type: {"Sites": {}, "Grand_Total": 0}}, ""

        access_token = get_access_token("IS5GyEBD3wWrNYG_eF57TBL-fW1KNdskezaQKPbA7Kxm")
        if not access_token:
            return {"error": "Failed to obtain access token"}, ""

        # Local count for validation (counts only, not descriptions/dates/status)
        local_result = {report_type: {"Sites": {}, "Grand_Total": 0}}
        for record in cleaned_data:
            tower = record["Tower"]
            discipline = record["Discipline_Category"]
            if tower not in local_result[report_type]["Sites"]:
                local_result[report_type]["Sites"][tower] = {"SW": 0, "FW": 0, "MEP": 0, "Total": 0}
            local_result[report_type]["Sites"][tower][discipline] += 1
            local_result[report_type]["Sites"][tower]["Total"] += 1
            local_result[report_type]["Grand_Total"] += 1

        chunk_size = 50
        all_results = {report_type: {"Sites": {}, "Grand_Total": 0}}

        for i in range(0, len(cleaned_data), chunk_size):
            chunk = cleaned_data[i:i + chunk_size]
            st.write(f"Processing chunk {i // chunk_size + 1}: Records {i} to {min(i + chunk_size, len(cleaned_data))}")
            logger.info(f"Data sent to WatsonX for {report_type} chunk {i // chunk_size + 1}: {json.dumps(chunk, indent=2)}")

            prompt = (
                "IMPORTANT: RETURN ONLY A SINGLE VALID JSON OBJECT WITH THE EXACT FIELDS SPECIFIED BELOW. "
                "DO NOT GENERATE ANY CODE (e.g., Python, JavaScript). "
                "DO NOT INCLUDE ANY TEXT, EXPLANATIONS, OR MULTIPLE RESPONSES OUTSIDE THE JSON OBJECT. "
                "DO NOT WRAP THE JSON IN CODE BLOCKS (e.g., ```json). "
                "RETURN THE JSON OBJECT DIRECTLY.\n\n"
                f"Task: For each record in the provided data, group by 'Tower' and collect 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', and 'Discipline' into arrays. "
                f"Also, count the records by 'Discipline_Category' ('SW', 'FW', 'MEP') and calculate the 'Total' for each 'Tower'. "
                f"Finally, calculate the 'Grand_Total' as the total number of records processed.\n"
                f"Condition: Only include records where:\n"
                f"- Status is '{report_type}'.\n"
                f"- For report_type == 'Closed': Days > 21 (pre-calculated planned duration).\n"
                f"- For report_type == 'Open': Days_From_Today > 21 (already calculated in the data).\n"
                "Use 'Tower' values exactly as provided. Count each record exactly once.\n\n"
                "REQUIRED OUTPUT FORMAT (ONLY THESE FIELDS):\n"
                "{\n"
                f'  "{report_type}": {{\n'
                '    "Sites": {\n'
                '      "Site_Name1": {\n'
                '        "Descriptions": ["description1", "description2"],\n'
                '        "Created Date (WET)": ["date1", "date2"],\n'
                '        "Expected Close Date (WET)": ["date1", "date2"],\n'
                '        "Status": ["status1", "status2"],\n'
                '        "Discipline": ["discipline1", "discipline2"],\n'
                '        "SW": number,\n'
                '        "FW": number,\n'
                '        "MEP": number,\n'
                '        "Total": number\n'
                '      }\n'
                '    },\n'
                '    "Grand_Total": number\n'
                '  }\n'
                '}\n\n'
                f"Data: {json.dumps(chunk)}\n"
                "Return the result as a single JSON object with only the specified fields."
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

            # Retry logic for the WatsonX API call
            @retry(
                stop=stop_after_attempt(3),
                wait=wait_fixed(5),
                retry=retry_if_exception_type(requests.exceptions.RequestException)
            )
            def call_watsonx_api():
                return requests.post(WATSONX_API_URL, headers=headers, json=payload, verify=certifi.where(), timeout=600)

            try:
                response = call_watsonx_api()
                logger.info(f"WatsonX API response status code: {response.status_code}")
                st.write(f"Debug - Response status code: {response.status_code}")

                if response.status_code == 200:
                    api_result = response.json()
                    generated_text = api_result.get("results", [{}])[0].get("generated_text", "").strip()
                    st.write(f"Debug - Raw response: {generated_text}")
                    logger.debug(f"Parsed generated text: {generated_text}")

                    parsed_json = clean_and_parse_json(generated_text)
                    if parsed_json and report_type in parsed_json:
                        chunk_result = parsed_json[report_type]
                        chunk_grand_total = chunk_result.get("Grand_Total", 0)
                        expected_total = len(chunk)
                        if chunk_grand_total == expected_total:
                            for site, data in chunk_result["Sites"].items():
                                if site not in all_results[report_type]["Sites"]:
                                    all_results[report_type]["Sites"][site] = {
                                        "Descriptions": [],
                                        "Created Date (WET)": [],
                                        "Expected Close Date (WET)": [],
                                        "Status": [],
                                        "Discipline": [],
                                        "SW": 0,
                                        "FW": 0,
                                        "MEP": 0,
                                        "Total": 0
                                    }
                                all_results[report_type]["Sites"][site]["Descriptions"].extend(data["Descriptions"])
                                all_results[report_type]["Sites"][site]["Created Date (WET)"].extend(data["Created Date (WET)"])
                                all_results[report_type]["Sites"][site]["Expected Close Date (WET)"].extend(data["Expected Close Date (WET)"])
                                all_results[report_type]["Sites"][site]["Status"].extend(data["Status"])
                                all_results[report_type]["Sites"][site]["Discipline"].extend(data["Discipline"])
                                all_results[report_type]["Sites"][site]["SW"] += data["SW"]
                                all_results[report_type]["Sites"][site]["FW"] += data["FW"]
                                all_results[report_type]["Sites"][site]["MEP"] += data["MEP"]
                                all_results[report_type]["Sites"][site]["Total"] += data["Total"]
                            all_results[report_type]["Grand_Total"] += chunk_grand_total
                            st.write(f"Debug - API result: {json.dumps(parsed_json, indent=2)}")
                        else:
                            logger.warning(f"API Grand_Total {chunk_grand_total} does not match expected {expected_total}, falling back to local count")
                            st.warning(f"API returned incorrect count (Grand_Total: {chunk_grand_total}, expected: {expected_total}), using local count")
                            for record in chunk:
                                tower = record["Tower"]
                                discipline = record["Discipline_Category"]
                                if tower not in all_results[report_type]["Sites"]:
                                    all_results[report_type]["Sites"][tower] = {
                                        "Descriptions": [],
                                        "Created Date (WET)": [],
                                        "Expected Close Date (WET)": [],
                                        "Status": [],
                                        "Discipline": [],
                                        "SW": 0,
                                        "FW": 0,
                                        "MEP": 0,
                                        "Total": 0
                                    }
                                all_results[report_type]["Sites"][tower]["Descriptions"].append(record["Description"])
                                all_results[report_type]["Sites"][tower]["Created Date (WET)"].append(record["Created Date (WET)"])
                                all_results[report_type]["Sites"][tower]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                                all_results[report_type]["Sites"][tower]["Status"].append(record["Status"])
                                all_results[report_type]["Sites"][tower]["Discipline"].append(record["Discipline"])
                                all_results[report_type]["Sites"][tower][discipline] += 1
                                all_results[report_type]["Sites"][tower]["Total"] += 1
                                all_results[report_type]["Grand_Total"] += 1
                    else:
                        logger.error("No valid JSON found in response")
                        st.error("❌ No valid JSON found in response")
                        st.write("Falling back to local count for this chunk")
                        for record in chunk:
                            tower = record["Tower"]
                            discipline = record["Discipline_Category"]
                            if tower not in all_results[report_type]["Sites"]:
                                all_results[report_type]["Sites"][tower] = {
                                    "Descriptions": [],
                                    "Created Date (WET)": [],
                                    "Expected Close Date (WET)": [],
                                    "Status": [],
                                    "Discipline": [],
                                    "SW": 0,
                                    "FW": 0,
                                    "MEP": 0,
                                    "Total": 0
                                }
                            all_results[report_type]["Sites"][tower]["Descriptions"].append(record["Description"])
                            all_results[report_type]["Sites"][tower]["Created Date (WET)"].append(record["Created Date (WET)"])
                            all_results[report_type]["Sites"][tower]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                            all_results[report_type]["Sites"][tower]["Status"].append(record["Status"])
                            all_results[report_type]["Sites"][tower]["Discipline"].append(record["Discipline"])
                            all_results[report_type]["Sites"][tower][discipline] += 1
                            all_results[report_type]["Sites"][tower]["Total"] += 1
                            all_results[report_type]["Grand_Total"] += 1
                else:
                    error_msg = f"❌ WatsonX API error: {response.status_code} - {response.text}"
                    st.error(error_msg)
                    logger.error(error_msg)
                    st.write("Falling back to local count for this chunk")
                    for record in chunk:
                        tower = record["Tower"]
                        discipline = record["Discipline_Category"]
                        if tower not in all_results[report_type]["Sites"]:
                            all_results[report_type]["Sites"][tower] = {
                                "Descriptions": [],
                                "Created Date (WET)": [],
                                "Expected Close Date (WET)": [],
                                "Status": [],
                                "Discipline": [],
                                "SW": 0,
                                "FW": 0,
                                "MEP": 0,
                                "Total": 0
                            }
                        all_results[report_type]["Sites"][tower]["Descriptions"].append(record["Description"])
                        all_results[report_type]["Sites"][tower]["Created Date (WET)"].append(record["Created Date (WET)"])
                        all_results[report_type]["Sites"][tower]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                        all_results[report_type]["Sites"][tower]["Status"].append(record["Status"])
                        all_results[report_type]["Sites"][tower]["Discipline"].append(record["Discipline"])
                        all_results[report_type]["Sites"][tower][discipline] += 1
                        all_results[report_type]["Sites"][tower]["Total"] += 1
                        all_results[report_type]["Grand_Total"] += 1
            except Exception as e:
                error_msg = f"❌ Exception during WatsonX call: {str(e)}"
                st.error(error_msg)
                logger.error(error_msg)
                st.write("Falling back to local count for this chunk")
                for record in chunk:
                    tower = record["Tower"]
                    discipline = record["Discipline_Category"]
                    if tower not in all_results[report_type]["Sites"]:
                        all_results[report_type]["Sites"][tower] = {
                            "Descriptions": [],
                            "Created Date (WET)": [],
                            "Expected Close Date (WET)": [],
                            "Status": [],
                            "Discipline": [],
                            "SW": 0,
                            "FW": 0,
                            "MEP": 0,
                            "Total": 0
                        }
                    all_results[report_type]["Sites"][tower]["Descriptions"].append(record["Description"])
                    all_results[report_type]["Sites"][tower]["Created Date (WET)"].append(record["Created Date (WET)"])
                    all_results[report_type]["Sites"][tower]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                    all_results[report_type]["Sites"][tower]["Status"].append(record["Status"])
                    all_results[report_type]["Sites"][tower]["Discipline"].append(record["Discipline"])
                    all_results[report_type]["Sites"][tower][discipline] += 1
                    all_results[report_type]["Sites"][tower]["Total"] += 1
                    all_results[report_type]["Grand_Total"] += 1

        # Validate counts only (Descriptions, dates, status, and discipline are not validated for equality)
        if all_results[report_type]["Grand_Total"] != local_result[report_type]["Grand_Total"]:
            logger.warning(f"Final API Grand_Total {all_results[report_type]['Grand_Total']} does not match local count {local_result[report_type]['Grand_Total']}, using local count")
            st.warning(f"API final count incorrect (Grand_Total: {all_results[report_type]['Grand_Total']}, expected: {local_result[report_type]['Grand_Total']}), using local count")
            all_results = {report_type: {"Sites": {}, "Grand_Total": 0}}
            for record in cleaned_data:
                tower = record["Tower"]
                discipline = record["Discipline_Category"]
                if tower not in all_results[report_type]["Sites"]:
                    all_results[report_type]["Sites"][tower] = {
                        "Descriptions": [],
                        "Created Date (WET)": [],
                        "Expected Close Date (WET)": [],
                        "Status": [],
                        "Discipline": [],
                        "SW": 0,
                        "FW": 0,
                        "MEP": 0,
                        "Total": 0
                    }
                all_results[report_type]["Sites"][tower]["Descriptions"].append(record["Description"])
                all_results[report_type]["Sites"][tower]["Created Date (WET)"].append(record["Created Date (WET)"])
                all_results[report_type]["Sites"][tower]["Expected Close Date (WET)"].append(record["Expected Close Date (WET)"])
                all_results[report_type]["Sites"][tower]["Status"].append(record["Status"])
                all_results[report_type]["Sites"][tower]["Discipline"].append(record["Discipline"])
                all_results[report_type]["Sites"][tower][discipline] += 1
                all_results[report_type]["Sites"][tower]["Total"] += 1
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
def generate_ncr_Safety_report(df, report_type, start_date=None, end_date=None, open_until_date=None):
    with st.spinner(f"Generating {report_type} Safety NCR Report with WatsonX..."):
        # Define standard_sites within the function
        standard_sites = [
            "EWS Tower 1",
            "EWS Tower 2",
            "EWS Tower 3",
            "LIG Tower 3",
            "LIG Tower 2",
            "LIG Tower 1"
        ]
        logger.debug(f"standard_sites defined: {standard_sites}")

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
                    "Tower": "Unknown Site"  # Default site
                }

                desc_lower = description.lower()
                tower_match = re.search(r'(ews|lig)\s*(tower)?\s*-?\s*(\d+)', desc_lower, re.IGNORECASE)
                if tower_match:
                    category = tower_match.group(1).upper()  # EWS or LIG
                    num = tower_match.group(3)  # Tower number
                    cleaned_record["Tower"] = f"{category} Tower {num}"
                    logger.debug(f"Matched tower: {tower_match.group(0)}, set Tower to {cleaned_record['Tower']}")
                else:
                    if standard_sites:
                        cleaned_record["Tower"] = standard_sites[0]  # Default to "EWS Tower 1"
                        logger.debug(f"No tower match in description: {desc_lower}, defaulting to {cleaned_record['Tower']}")
                    else:
                        cleaned_record["Tower"] = "Unknown Site"
                        logger.warning(f"standard_sites is empty, defaulting Tower to 'Unknown Site'")

                cleaned_data.append(cleaned_record)

        st.write(f"Total {report_type} records to process: {len(cleaned_data)}")
        logger.debug(f"Processed data: {json.dumps(cleaned_data, indent=2)}")

        if not cleaned_data:
            return {"Safety": {"Sites": {}, "Grand_Total": 0}}, ""

        access_token = get_access_token("API_KEY_2")
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
                "Use the 'Tower' values exactly as they appear in the data. "
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
        
        # Extract day and month from report_title (format: "NCR: {day}_{month_name}")
        # Example: "NCR: 24_April" -> "24_April"
        date_part = report_title.replace("NCR: ", "") if report_title.startswith("NCR: ") else "24_April"
        
        # Create summary worksheet (NCR Report)
        worksheet = workbook.add_worksheet('NCR Report')
        
        # Set column widths for summary sheet
        worksheet.set_column('A:A', 20)  # Site column
        worksheet.set_column('B:H', 12)  # Data columns
        
        # Get data from both sections
        resolved_data = combined_result.get("NCR resolved beyond 21 days", {})
        open_data = combined_result.get("NCR open beyond 21 days", {})
        
        if not isinstance(resolved_data, dict) or "error" in resolved_data:
            resolved_data = {"Sites": {}}
        if not isinstance(open_data, dict) or "error" in open_data:
            open_data = {"Sites": {}}
            
        resolved_sites = resolved_data.get("Sites", {})
        open_sites = open_data.get("Sites", {})
        
        # Define only the standard sites you want to include
        standard_sites = [
            "EWS Tower 1",
            "EWS Tower 2",
            "EWS Tower 3",
            "LIG Tower 3",
            "LIG Tower 2",
            "LIG Tower 1",
        ]
        
        # Normalize JSON site names to match standard_sites format
        def normalize_site_name(site):
            if site in standard_sites:
                return site
            # Match patterns like "EWS Tower 1" or "LIG Tower 2"
            match = re.search(r'(EWS|LIG)\s*(Tower)?\s*-?\s*(\d+)', site, re.IGNORECASE)
            if match:
                category = match.group(1).upper()  # EWS or LIG
                num = match.group(3)  # Tower number
                return f"{category} Tower {num}"
            return site  # Fallback to the original site name if no match

        # Create a reverse mapping for original keys to normalized names
        site_mapping = {k: normalize_site_name(k) for k in (resolved_sites.keys() | open_sites.keys())}
        
        # Sort the standard sites
        sorted_sites = sorted(standard_sites)
        
        # Title row for summary sheet
        worksheet.merge_range('A1:H1', report_title, title_format)
        
        # Header row for summary sheet
        row = 1
        worksheet.write(row, 0, 'Site', header_format)
        worksheet.merge_range(row, 1, row, 3, 'NCR resolved beyond 21 days', header_format)
        worksheet.merge_range(row, 4, row, 6, 'NCR open beyond 21 days', header_format)
        worksheet.write(row, 7, 'Total', header_format)
        
        # Subheaders for summary sheet
        row = 2
        categories = ['Civil Finishing', 'MEP', 'Structure']
        worksheet.write(row, 0, '', header_format)
        
        # Resolved subheaders
        for i, cat in enumerate(categories):
            worksheet.write(row, i+1, cat, subheader_format)
            
        # Open subheaders
        for i, cat in enumerate(categories):
            worksheet.write(row, i+4, cat, subheader_format)
            
        worksheet.write(row, 7, '', header_format)
        
        # Map our categories to the JSON data categories
        category_map = {
            'Civil Finishing': 'FW',
            'MEP': 'MEP',
            'Structure': 'SW'
        }
        
        # Data rows for summary sheet
        row = 3
        site_totals = {}
        
        for site in sorted_sites:
            worksheet.write(row, 0, site, site_format)
            
            # Find original key that maps to this normalized site
            original_resolved_key = next((k for k, v in site_mapping.items() if v == site), None)
            original_open_key = next((k for k, v in site_mapping.items() if v == site), None)
            
            site_total = 0
            
            # Resolved data
            for i, (display_cat, json_cat) in enumerate(category_map.items()):
                value = 0
                if original_resolved_key and original_resolved_key in resolved_sites:
                    value = resolved_sites[original_resolved_key].get(json_cat, 0)
                worksheet.write(row, i+1, value, cell_format)
                site_total += value
                
            # Open data
            for i, (display_cat, json_cat) in enumerate(category_map.items()):
                value = 0
                if original_open_key and original_open_key in open_sites:
                    value = open_sites[original_open_key].get(json_cat, 0)
                worksheet.write(row, i+4, value, cell_format)
                site_total += value
                
            # Total for this site
            worksheet.write(row, 7, site_total, cell_format)
            site_totals[site] = site_total
            row += 1
        
        # Helper function to write detailed NCR sheets
        def write_detail_sheet(sheet_name, data, title):
            detail_worksheet = workbook.add_worksheet(f"{sheet_name} {date_part}")
            # Set column widths for detail sheet
            detail_worksheet.set_column('A:A', 20)  # Site
            detail_worksheet.set_column('B:B', 60)  # Description
            detail_worksheet.set_column('C:D', 20)  # Dates
            detail_worksheet.set_column('E:E', 15)  # Status
            detail_worksheet.set_column('F:F', 15)  # Discipline

            # Write title
            detail_worksheet.merge_range('A1:F1', f"{title} {date_part}", title_format)

            # Write headers
            headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline']
            for col, header in enumerate(headers):
                detail_worksheet.write(1, col, header, header_format)

            # Write data
            row = 2
            for site, site_data in data.items():
                descriptions = site_data.get("Descriptions", [])
                created_dates = site_data.get("Created Date (WET)", [])
                close_dates = site_data.get("Expected Close Date (WET)", [])
                statuses = site_data.get("Status", [])
                disciplines = site_data.get("Discipline", [])

                # Ensure all lists have the same length
                max_length = max(len(descriptions), len(created_dates), len(close_dates), len(statuses), len(disciplines))
                for i in range(max_length):
                    detail_worksheet.write(row, 0, site, site_format)
                    detail_worksheet.write(row, 1, descriptions[i] if i < len(descriptions) else "", cell_format)
                    detail_worksheet.write(row, 2, created_dates[i] if i < len(created_dates) else "", cell_format)
                    detail_worksheet.write(row, 3, close_dates[i] if i < len(close_dates) else "", cell_format)
                    detail_worksheet.write(row, 4, statuses[i] if i < len(statuses) else "", cell_format)
                    detail_worksheet.write(row, 5, disciplines[i] if i < len(disciplines) else "", cell_format)
                    row += 1

        # Write detailed sheets for Closed and Open NCRs
        if resolved_sites:
            write_detail_sheet("Closed NCR Details", resolved_sites, "Closed NCR Details")
        if open_sites:
            write_detail_sheet("Open NCR Details", open_sites, "Open NCR Details")

        # Return the Excel file
        output.seek(0)
        return output

@st.cache_data
def generate_ncr_Housekeeping_report(df, report_type, start_date=None, end_date=None, open_until_date=None):
    with st.spinner(f"Generating {report_type} Housekeeping NCR Report with WatsonX..."):
        # Define standard_sites within the function (or use the global definition if you set it)
        standard_sites = [
            "EWS Tower 1",
            "EWS Tower 2",
            "EWS Tower 3",
            "LIG Tower 3",
            "LIG Tower 2",
            "LIG Tower 1"
        ]
        logger.debug(f"standard_sites defined: {standard_sites}")

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
                    "Tower": "Unknown Site"  # Default site
                }

                desc_lower = description.lower()
                tower_match = re.search(r'(ews|lig)\s*(tower)?\s*-?\s*(\d+)', desc_lower, re.IGNORECASE)
                if tower_match:
                    category = tower_match.group(1).upper()  # EWS or LIG
                    num = tower_match.group(3)  # Tower number
                    cleaned_record["Tower"] = f"{category} Tower {num}"
                    logger.debug(f"Matched tower: {tower_match.group(0)}, set Tower to {cleaned_record['Tower']}")
                else:
                    if standard_sites:
                        cleaned_record["Tower"] = standard_sites[0]  # Default to "EWS Tower 1"
                        logger.debug(f"No tower match in description: {desc_lower}, defaulting to {cleaned_record['Tower']}")
                    else:
                        cleaned_record["Tower"] = "Unknown Site"
                        logger.warning(f"standard_sites is empty, defaulting Tower to 'Unknown Site'")

                cleaned_data.append(cleaned_record)

        st.write(f"Total {report_type} records to process: {len(cleaned_data)}")
        logger.debug(f"Processed data: {json.dumps(cleaned_data, indent=2)}")

        if not cleaned_data:
            return {"Housekeeping": {"Sites": {}, "Grand_Total": 0}}, ""

        access_token = get_access_token("API_KEY_2")
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
                "Use the 'Tower' values exactly as they appear in the data. "
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
    
def generate_consolidated_ncr_Housekeeping_excel(combined_result, report_title="Housekeeping: Current Month"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
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
        cell_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        site_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1
        })
        description_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })
        
        # Extract report type (Closed or Open) and update report_title with current date
        report_type = "Closed" if "Closed" in report_title else "Open"
        now = datetime.now()  # Current date: April 24, 2025
        day = now.strftime("%d")
        month_name = now.strftime("%B")
        date_part = f"{day}_{month_name}"  # "24_April"
        report_title = f"Housekeeping: {report_type} - {date_part}"

        # Limit worksheet names to 31 characters
        def truncate_sheet_name(base_name, max_length=31):
            if len(base_name) > max_length:
                return base_name[:max_length - 3] + "..."  # Truncate and add ellipsis
            return base_name

        summary_sheet_name = truncate_sheet_name(f'Housekeeping NCR Report {date_part}')
        details_sheet_name = truncate_sheet_name(f'Housekeeping NCR Details {date_part}')

        # Create summary worksheet (Housekeeping NCR Report)
        worksheet_summary = workbook.add_worksheet(summary_sheet_name)
        worksheet_summary.set_column('A:A', 20)
        worksheet_summary.set_column('B:B', 15)
        
        data = combined_result.get("Housekeeping", {}).get("Sites", {})
        
        standard_sites = [
                "EWS Tower 1",
                "EWS Tower 2",
                "EWS Tower 3",
                "LIG Tower 3",
                "LIG Tower 2",
                "LIG Tower 1"
        ]
        
        def normalize_site_name(site):
            if site in standard_sites:
                return site
            match = re.search(r'(?:tower|t)[- ]?(\d+|2021|28)', site, re.IGNORECASE)
            if match:
                num = match.group(1).zfill(2)
                return f"Veridia-Tower{num}"
            return site

        site_mapping = {k: normalize_site_name(k) for k in data.keys()}
        sorted_sites = sorted(standard_sites)
        
        worksheet_summary.merge_range('A1:B1', report_title, title_format)
        row = 1
        worksheet_summary.write(row, 0, 'Site', header_format)
        worksheet_summary.write(row, 1, 'No. of Housekeeping NCRs beyond 7 days', header_format)
        
        row = 2
        for site in sorted_sites:
            worksheet_summary.write(row, 0, site, site_format)
            original_key = next((k for k, v in site_mapping.items() if v == site), None)
            if original_key and original_key in data:
                value = data[original_key].get("Count", 0)
            else:
                value = 0
            worksheet_summary.write(row, 1, value, cell_format)
            row += 1
        
        # Create details worksheet (Housekeeping NCR Details)
        worksheet_details = workbook.add_worksheet(details_sheet_name)
        worksheet_details.set_column('A:A', 20)
        worksheet_details.set_column('B:B', 60)
        worksheet_details.set_column('C:D', 20)
        worksheet_details.set_column('E:E', 15)
        worksheet_details.set_column('F:F', 15)  # Discipline column
        
        worksheet_details.merge_range('A1:F1', f"{report_title} - Details", title_format)
        
        headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline']
        row = 1
        for col, header in enumerate(headers):
            worksheet_details.write(row, col, header, header_format)
        
        row = 2
        for site in sorted_sites:
            original_key = next((k for k, v in site_mapping.items() if v == site), None)
            if original_key and original_key in data:
                site_data = data[original_key]
                descriptions = site_data.get("Descriptions", [])
                created_dates = site_data.get("Created Date (WET)", [])
                close_dates = site_data.get("Expected Close Date (WET)", [])
                statuses = site_data.get("Status", [])
                
                max_length = max(len(descriptions), len(created_dates), len(close_dates), len(statuses))
                for i in range(max_length):
                    worksheet_details.write(row, 0, site, site_format)
                    worksheet_details.write(row, 1, descriptions[i] if i < len(descriptions) else "", description_format)
                    worksheet_details.write(row, 2, created_dates[i] if i < len(created_dates) else "", cell_format)
                    worksheet_details.write(row, 3, close_dates[i] if i < len(close_dates) else "", cell_format)
                    worksheet_details.write(row, 4, statuses[i] if i < len(statuses) else "", cell_format)
                    worksheet_details.write(row, 5, "HSE", cell_format)  # Hardcode Discipline as "HSE"
                    row += 1
        
        output.seek(0)
        return output    
    
def generate_consolidated_ncr_Safety_excel(combined_result, report_title=None):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
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
        cell_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        site_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1
        })
        description_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })
        
        # Update report_title with current date in "Month Day" format (e.g., "April 24")
        now = datetime.now()  # Current date: April 24, 2025
        day = now.strftime("%d")
        month_name = now.strftime("%B")
        date_part = f"{month_name} {day}" 
        if report_title is None:
            report_title = f"Safety:{date_part}- Current Month "
        else:
            report_type = "Safety"  # Assuming safety reports; adjust if dynamic
            report_title = f"{date_part}: {report_type}"

        # Limit worksheet names to 31 characters
        def truncate_sheet_name(base_name, max_length=31):
            if len(base_name) > max_length:
                return base_name[:max_length - 3] + "..."  # Truncate and add ellipsis
            return base_name

        summary_sheet_name = truncate_sheet_name(f'Safety NCR Report {date_part}')
        details_sheet_name = truncate_sheet_name(f'Safety NCR Details {date_part}')

        # Create summary worksheet (Safety NCR Report)
        worksheet_summary = workbook.add_worksheet(summary_sheet_name)
        worksheet_summary.set_column('A:A', 20)
        worksheet_summary.set_column('B:B', 15)
        
        data = combined_result.get("Safety", {}).get("Sites", {})
        
        # Standard sites list (used across all reports)
        standard_sites = [
            "EWS Tower 1",
            "EWS Tower 2",
            "EWS Tower 3",
            "LIG Tower 3",
            "LIG Tower 2",
            "LIG Tower 1"
        ]
        
        def normalize_site_name(site):
            if site in standard_sites:
                return site
            match = re.search(r'(?:tower|t)[- ]?(\d+|2021|28)', site, re.IGNORECASE)
            if match:
                num = match.group(1).zfill(2)
                return f"Veridia-Tower{num}"
            return site

        site_mapping = {k: normalize_site_name(k) for k in data.keys()}
        sorted_sites = sorted(standard_sites)
        
        worksheet_summary.merge_range('A1:B1', report_title, title_format)
        row = 1
        worksheet_summary.write(row, 0, 'Site', header_format)
        worksheet_summary.write(row, 1, 'No. of Safety NCRs beyond 7 days', header_format)
        
        row = 2
        for site in sorted_sites:
            worksheet_summary.write(row, 0, site, site_format)
            original_key = next((k for k, v in site_mapping.items() if v == site), None)
            if original_key and original_key in data:
                value = data[original_key].get("Count", 0)
            else:
                value = 0
            worksheet_summary.write(row, 1, value, cell_format)
            row += 1
        
        # Create details worksheet (Safety NCR Details)
        worksheet_details = workbook.add_worksheet(details_sheet_name)
        worksheet_details.set_column('A:A', 20)
        worksheet_details.set_column('B:B', 60)
        worksheet_details.set_column('C:D', 20)
        worksheet_details.set_column('E:E', 15)
        worksheet_details.set_column('F:F', 15)

        worksheet_details.merge_range('A1:F1', f"{report_title} - Details", title_format)
        
        headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline']
        row = 1
        for col, header in enumerate(headers):
            if col == 5:  # Column F (Discipline)
                worksheet_details.write(row, col, header, title_format)  # Use title_format for Discipline
            else:
                worksheet_details.write(row, col, header, header_format)  # Use header_format for others
        
        row = 2
        for site in sorted_sites:
            original_key = next((k for k, v in site_mapping.items() if v == site), None)
            if original_key and original_key in data:
                site_data = data[original_key]
                descriptions = site_data.get("Descriptions", [])
                created_dates = site_data.get("Created Date (WET)", [])
                close_dates = site_data.get("Expected Close Date (WET)", [])
                statuses = site_data.get("Status", [])
                
                max_length = max(len(descriptions), len(created_dates), len(close_dates), len(statuses))
                for i in range(max_length):
                    worksheet_details.write(row, 0, site, site_format)
                    worksheet_details.write(row, 1, descriptions[i] if i < len(descriptions) else "", description_format)
                    worksheet_details.write(row, 2, created_dates[i] if i < len(created_dates) else "", cell_format)
                    worksheet_details.write(row, 3, close_dates[i] if i < len(close_dates) else "", cell_format)
                    worksheet_details.write(row, 4, statuses[i] if i < len(statuses) else "", cell_format)
                    worksheet_details.write(row, 5, "HSE", cell_format)
                    row += 1
        
        output.seek(0)
        return output
    
def generate_combined_excel_report(
    all_reports,
    open_result_housekeeping,
    closed_result_housekeeping,
    open_result_safety,
    closed_result_safety,
    report_title_ncr,
    report_title_housekeeping,
    report_title_safety
):
    """
    Generate a combined Excel report for NCR, Housekeeping, and Safety (Open and Closed).
    
    Args:
        all_reports (dict): Result of generate_ncr_report containing both Open and Closed NCR data.
        open_result_housekeeping (dict): Result of generate_ncr_Housekeeping_report for Open.
        closed_result_housekeeping (dict): Result of generate_ncr_Housekeeping_report for Closed.
        open_result_safety (dict): Result of generate_ncr_Safety_report for Open.
        closed_result_safety (dict): Result of generate_ncr_Safety_report for Closed.
        report_title_ncr (str): Title for the NCR section.
        report_title_housekeeping (str): Title for the Housekeeping section.
        report_title_safety (str): Title for the Safety section.
    
    Returns:
        io.BytesIO: Excel file in memory with multiple sheets for each report type.
    """
    # Define standard sites for consistent ordering
    standard_sites = [
        "EWS Tower 1",
        "EWS Tower 2",
        "EWS Tower 3",
        "LIG Tower 3",
        "LIG Tower 2",
        "LIG Tower 1"
    ]
    logger.debug(f"standard_sites in generate_combined_excel_report: {standard_sites}")

    # Initialize workbook
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book

        # Define cell formats
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
        cell_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        cell_format_left = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1
        })
        description_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })
        total_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })

        # Helper function to normalize site names
        def normalize_site_name(site):
            if site in standard_sites:
                return site
            match = re.search(r'(EWS|LIG)\s*(Tower)?\s*-?\s*(\d+)', site, re.IGNORECASE)
            if match:
                category = match.group(1).upper()
                num = match.group(3)
                return f"{category} Tower {num}"
            return site

        # Helper function to truncate sheet names (Excel has a 31-character limit)
        def truncate_sheet_name(name, max_length=31):
            if len(name) > max_length:
                return name[:max_length - 3] + "..."
            return name

        # --- NCR Report Sheet (Summary) ---
        worksheet_ncr = workbook.add_worksheet(truncate_sheet_name('NCR Summary'))
        worksheet_ncr.set_column('A:A', 20)  # Site column
        worksheet_ncr.set_column('B:H', 12)  # Data columns

        # Write title
        worksheet_ncr.merge_range('A1:H1', report_title_ncr, title_format)

        # Write headers
        row = 1
        worksheet_ncr.write(row, 0, 'Site', header_format)
        worksheet_ncr.merge_range(row, 1, row, 3, 'NCR Resolved Beyond 21 Days', header_format)
        worksheet_ncr.merge_range(row, 4, row, 6, 'NCR Open Beyond 21 Days', header_format)
        worksheet_ncr.write(row, 7, 'Total', header_format)

        # Subheaders
        row = 2
        categories = ['Civil Finishing', 'MEP', 'Structure']
        worksheet_ncr.write(row, 0, '', header_format)
        for i, cat in enumerate(categories):
            worksheet_ncr.write(row, i + 1, cat, header_format)
            worksheet_ncr.write(row, i + 4, cat, header_format)
        worksheet_ncr.write(row, 7, '', header_format)

        # Process NCR data
        resolved_data = all_reports.get("NCR resolved beyond 21 days", {}).get("Sites", {})
        open_data = all_reports.get("NCR open beyond 21 days", {}).get("Sites", {})
        site_mapping_ncr = {k: normalize_site_name(k) for k in (resolved_data.keys() | open_data.keys())}

        category_map = {
            'Civil Finishing': 'FW',
            'MEP': 'MEP',
            'Structure': 'SW'
        }

        # Write NCR summary data
        row = 3
        for site in standard_sites:
            worksheet_ncr.write(row, 0, site, cell_format_left)
            site_total = 0
            original_resolved_key = next((k for k, v in site_mapping_ncr.items() if v == site), None)
            original_open_key = next((k for k, v in site_mapping_ncr.items() if v == site), None)

            # Resolved data
            for i, (display_cat, json_cat) in enumerate(category_map.items()):
                value = resolved_data.get(original_resolved_key, {}).get(json_cat, 0)
                worksheet_ncr.write(row, i + 1, value, cell_format)
                site_total += value

            # Open data
            for i, (display_cat, json_cat) in enumerate(category_map.items()):
                value = open_data.get(original_open_key, {}).get(json_cat, 0)
                worksheet_ncr.write(row, i + 4, value, cell_format)
                site_total += value

            # Total for this site
            worksheet_ncr.write(row, 7, site_total, cell_format)
            row += 1

        # --- NCR Details Sheets ---
        def write_ncr_detail_sheet(sheet_name, data, title):
            worksheet = workbook.add_worksheet(truncate_sheet_name(sheet_name))
            worksheet.set_column('A:A', 20)
            worksheet.set_column('B:B', 60)
            worksheet.set_column('C:D', 20)
            worksheet.set_column('E:E', 15)
            worksheet.set_column('F:F', 15)

            # Write title
            worksheet.merge_range('A1:F1', title, title_format)

            # Write headers
            headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline']
            for col, header in enumerate(headers):
                worksheet.write(1, col, header, header_format)

            # Write data
            row = 2
            for site, site_data in data.items():
                normalized_site = normalize_site_name(site)
                if normalized_site not in standard_sites:
                    continue
                descriptions = site_data.get("Descriptions", [])
                created_dates = site_data.get("Created Date (WET)", [])
                close_dates = site_data.get("Expected Close Date (WET)", [])
                statuses = site_data.get("Status", [])
                disciplines = site_data.get("Discipline", [])

                max_length = max(len(descriptions), len(created_dates), len(close_dates), len(statuses), len(disciplines))
                for i in range(max_length):
                    worksheet.write(row, 0, normalized_site, cell_format_left)
                    worksheet.write(row, 1, descriptions[i] if i < len(descriptions) else "", description_format)
                    worksheet.write(row, 2, created_dates[i] if i < len(created_dates) else "", cell_format)
                    worksheet.write(row, 3, close_dates[i] if i < len(close_dates) else "", cell_format)
                    worksheet.write(row, 4, statuses[i] if i < len(statuses) else "", cell_format)
                    worksheet.write(row, 5, disciplines[i] if i < len(disciplines) else "", cell_format)
                    row += 1

        if resolved_data:
            write_ncr_detail_sheet('NCR Resolved Details', resolved_data, 'NCR Resolved Beyond 21 Days - Details')
        if open_data:
            write_ncr_detail_sheet('NCR Open Details', open_data, 'NCR Open Beyond 21 Days - Details')

        # --- Housekeeping Report Sheet (Summary) ---
        worksheet_hk = workbook.add_worksheet(truncate_sheet_name('Housekeeping Summary'))
        worksheet_hk.set_column('A:A', 20)
        worksheet_hk.set_column('B:D', 15)

        # Write title
        worksheet_hk.merge_range('A1:D1', report_title_housekeeping, title_format)

        # Write headers
        row = 1
        headers_hk = ['Site', 'Open', 'Closed', 'Total']
        for col, header in enumerate(headers_hk):
            worksheet_hk.write(row, col, header, header_format)

        # Process Housekeeping data
        open_hk_data = open_result_housekeeping.get("Housekeeping", {}).get("Sites", {})
        closed_hk_data = closed_result_housekeeping.get("Housekeeping", {}).get("Sites", {})
        site_mapping_hk = {k: normalize_site_name(k) for k in (open_hk_data.keys() | closed_hk_data.keys())}

        # Initialize grand totals to fix UnboundLocalError
        grand_total_open_hk = 0
        grand_total_closed_hk = 0
        grand_total_hk = 0

        # Write Housekeeping summary data
        row = 2
        for site in standard_sites:
            worksheet_hk.write(row, 0, site, cell_format_left)
            original_open_key = next((k for k, v in site_mapping_hk.items() if v == site), None)
            original_closed_key = next((k for k, v in site_mapping_hk.items() if v == site), None)

            open_count = open_hk_data.get(original_open_key, {}).get("Count", 0)
            closed_count = closed_hk_data.get(original_closed_key, {}).get("Count", 0)
            total_count = open_count + closed_count

            worksheet_hk.write(row, 1, open_count, cell_format)
            worksheet_hk.write(row, 2, closed_count, cell_format)
            worksheet_hk.write(row, 3, total_count, cell_format)

            # Update grand totals (for internal tracking, not written to sheet)
            grand_total_open_hk += open_count
            grand_total_closed_hk += closed_count
            grand_total_hk += total_count
            row += 1

        # No grand total row written (per requirement)

        # --- Housekeeping Details Sheets ---
        def write_hk_detail_sheet(sheet_name, data, title):
            worksheet = workbook.add_worksheet(truncate_sheet_name(sheet_name))
            worksheet.set_column('A:A', 20)
            worksheet.set_column('B:B', 60)
            worksheet.set_column('C:D', 20)
            worksheet.set_column('E:E', 15)
            worksheet.set_column('F:F', 15)

            # Write title
            worksheet.merge_range('A1:F1', title, title_format)

            # Write headers
            headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline']
            for col, header in enumerate(headers):
                worksheet.write(1, col, header, header_format)

            # Write data
            row = 2
            for site, site_data in data.items():
                normalized_site = normalize_site_name(site)
                if normalized_site not in standard_sites:
                    continue
                descriptions = site_data.get("Descriptions", [])
                created_dates = site_data.get("Created Date (WET)", [])
                close_dates = site_data.get("Expected Close Date (WET)", [])
                statuses = site_data.get("Status", [])

                max_length = max(len(descriptions), len(created_dates), len(close_dates), len(statuses))
                for i in range(max_length):
                    worksheet.write(row, 0, normalized_site, cell_format_left)
                    worksheet.write(row, 1, descriptions[i] if i < len(descriptions) else "", description_format)
                    worksheet.write(row, 2, created_dates[i] if i < len(created_dates) else "", cell_format)
                    worksheet.write(row, 3, close_dates[i] if i < len(close_dates) else "", cell_format)
                    worksheet.write(row, 4, statuses[i] if i < len(statuses) else "", cell_format)
                    worksheet.write(row, 5, "HSE", cell_format)  # Hardcode Discipline as HSE
                    row += 1

        if open_hk_data:
            write_hk_detail_sheet('HK Open Details', open_hk_data, 'Housekeeping Open Beyond 7 Days - Details')
        if closed_hk_data:
            write_hk_detail_sheet('HK Closed Details', closed_hk_data, 'Housekeeping Closed Beyond 7 Days - Details')

        # --- Safety Report Sheet (Summary) ---
        worksheet_safety = workbook.add_worksheet(truncate_sheet_name('Safety Summary'))
        worksheet_safety.set_column('A:A', 20)
        worksheet_safety.set_column('B:D', 15)

        # Write title
        worksheet_safety.merge_range('A1:D1', report_title_safety, title_format)

        # Write headers
        row = 1
        headers_safety = ['Site', 'Open', 'Closed', 'Total']
        for col, header in enumerate(headers_safety):
            worksheet_safety.write(row, col, header, header_format)

        # Process Safety data
        open_safety_data = open_result_safety.get("Safety", {}).get("Sites", {})
        closed_safety_data = closed_result_safety.get("Safety", {}).get("Sites", {})
        site_mapping_safety = {k: normalize_site_name(k) for k in (open_safety_data.keys() | closed_safety_data.keys())}

        # Initialize grand totals to fix UnboundLocalError
        grand_total_open_safety = 0
        grand_total_closed_safety = 0
        grand_total_safety = 0

        # Write Safety summary data
        row = 2
        for site in standard_sites:
            worksheet_safety.write(row, 0, site, cell_format_left)
            original_open_key = next((k for k, v in site_mapping_safety.items() if v == site), None)
            original_closed_key = next((k for k, v in site_mapping_safety.items() if v == site), None)

            open_count = open_safety_data.get(original_open_key, {}).get("Count", 0)
            closed_count = closed_safety_data.get(original_closed_key, {}).get("Count", 0)
            total_count = open_count + closed_count

            worksheet_safety.write(row, 1, open_count, cell_format)
            worksheet_safety.write(row, 2, closed_count, cell_format)
            worksheet_safety.write(row, 3, total_count, cell_format)

            # Update grand totals (for internal tracking, not written to sheet)
            grand_total_open_safety += open_count
            grand_total_closed_safety += closed_count
            grand_total_safety += total_count
            row += 1

        # No grand total row written (per requirement)

        # --- Safety Details Sheets ---
        def write_safety_detail_sheet(sheet_name, data, title):
            worksheet = workbook.add_worksheet(truncate_sheet_name(sheet_name))
            worksheet.set_column('A:A', 20)
            worksheet.set_column('B:B', 60)
            worksheet.set_column('C:D', 20)
            worksheet.set_column('E:E', 15)
            worksheet.set_column('F:F', 15)

            # Write title
            worksheet.merge_range('A1:F1', title, title_format)

            # Write headers
            headers = ['Site', 'Description', 'Created Date (WET)', 'Expected Close Date (WET)', 'Status', 'Discipline']
            for col, header in enumerate(headers):
                worksheet.write(1, col, header, header_format)

            # Write data
            row = 2
            for site, site_data in data.items():
                normalized_site = normalize_site_name(site)
                if normalized_site not in standard_sites:
                    continue
                descriptions = site_data.get("Descriptions", [])
                created_dates = site_data.get("Created Date (WET)", [])
                close_dates = site_data.get("Expected Close Date (WET)", [])
                statuses = site_data.get("Status", [])

                max_length = max(len(descriptions), len(created_dates), len(close_dates), len(statuses))
                for i in range(max_length):
                    worksheet.write(row, 0, normalized_site, cell_format_left)
                    worksheet.write(row, 1, descriptions[i] if i < len(descriptions) else "", description_format)
                    worksheet.write(row, 2, created_dates[i] if i < len(created_dates) else "", cell_format)
                    worksheet.write(row, 3, close_dates[i] if i < len(close_dates) else "", cell_format)
                    worksheet.write(row, 4, statuses[i] if i < len(statuses) else "", cell_format)
                    worksheet.write(row, 5, "HSE", cell_format)  # Hardcode Discipline as HSE
                    row += 1

        if open_safety_data:
            write_safety_detail_sheet('Safety Open Details', open_safety_data, 'Safety Open Beyond 7 Days - Details')
        if closed_safety_data:
            write_safety_detail_sheet('Safety Closed Details', closed_safety_data, 'Safety Closed Beyond 7 Days - Details')

    output.seek(0)
    return output

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

# Sidebar: Asite Login Section (comes first)
st.sidebar.title("🔒 Asite Login")
email = st.sidebar.text_input("Email", "impwatson@gadieltechnologies.com", key="email_input")
password = st.sidebar.text_input("Password", "Srihari@790$", type="password", key="password_input")

# Sidebar: Login Button (comes after Asite Login section)
if st.sidebar.button("Login", key="login_button"):
    session_id = login_to_asite(email, password)
    if session_id:
        st.session_state["session_id"] = session_id
        st.sidebar.success("✅ Login Successful")

# Sidebar: Project Data Section (comes after Login button)
st.sidebar.title("📂 Project Data")
project_name, form_name, project_options = project_dropdown()
st.session_state["project_options"] = project_options

# Sidebar: Fetch Data Button (comes after Project Data section)
if st.sidebar.button("Fetch Data", key="fetch_data"):
    header, data, payload = fetch_project_data(st.session_state["session_id"], project_name, form_name)
    st.json(header)
    if data:
        df = process_json_data(data)
        # Store the fetched DataFrame in fetched_data
        st.session_state['fetched_data'] = df
        # Assign the same fetched data to all three DataFrames
        st.session_state["ncr_df"] = df.copy()
        st.session_state["safety_df"] = df.copy()
        st.session_state["housekeeping_df"] = df.copy()
        st.dataframe(df)
        st.success("✅ Data fetched and processed successfully for all report types!")

# Report Generation Section (remains as is, after Fetch Data)
st.sidebar.title("📋 Combined NCR Report")
# Date inputs for filtering reports
if st.session_state["ncr_df"] is not None:
    ncr_df = st.session_state["ncr_df"]
    # Set default dates based on DataFrame
    default_closed_start = ncr_df['Expected Close Date (WET)'].min()
    default_closed_end = ncr_df['Expected Close Date (WET)'].max()
    default_open_end = ncr_df['Expected Close Date (WET)'].max()
else:
    # Set default dates to today if no data is available
    default_closed_start = datetime.today()
    default_closed_end = datetime.today()
    default_open_end = datetime.today()

# Date inputs with on_change to update session state
def update_closed_start():
    st.session_state['closed_start_date'] = st.session_state["ncr_closed_start"]

def update_closed_end():
    st.session_state['closed_end_date'] = st.session_state["ncr_closed_end"]

def update_open_end():
    st.session_state['open_until_date'] = st.session_state["ncr_open_end"]

closed_start = st.sidebar.date_input(
    "Closed Start Date",
    value=default_closed_start,
    key="ncr_closed_start",
    on_change=update_closed_start
)
closed_end = st.sidebar.date_input(
    "Closed End Date",
    value=default_closed_end,
    key="ncr_closed_end",
    on_change=update_closed_end
)
open_end = st.sidebar.date_input(
    "Open Until Date",
    value=default_open_end,
    key="ncr_open_end",
    on_change=update_open_end
)

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
# All Reports Combined Button
if st.sidebar.button("All_Report", disabled=not (st.session_state.get('fetched_data') is not None and not st.session_state['fetched_data'].empty)):
    try:
        # Ensure fetched data exists and is not empty
        if st.session_state.get('fetched_data') is None or st.session_state['fetched_data'].empty:
            st.error("No data available. Please fetch data first.")
            logger.error("No fetched data available for All_Report")
            st.stop()

        # Get the DataFrames
        ncr_df = st.session_state.get('ncr_df')
        safety_df = st.session_state.get('safety_df')
        housekeeping_df = st.session_state.get('housekeeping_df')

        if any(df is None or df.empty for df in [ncr_df, safety_df, housekeeping_df]):
            st.error("Required DataFrames not found or empty. Please fetch data again.")
            logger.error("One or more DataFrames missing or empty for All_Report")
            st.stop()

        # Get date inputs from session state or use defaults
        closed_start = st.session_state.get('closed_start_date', datetime.today())
        closed_end = st.session_state.get('closed_end_date', datetime.today())
        open_end = st.session_state.get('open_until_date', datetime.today())

        # Generate report titles with current date
        month_name = closed_end.strftime("%B")
        day = datetime.now().strftime("%d")
        report_title_ncr = f" EWS NCR: {day}_{month_name}"
        report_title_housekeeping = f"Housekeeping: {day}_{month_name}"
        report_title_safety = f"Safety: {day}_{month_name}"

        # Generate NCR Reports (Open and Closed)
        with st.spinner("Generating NCR Reports..."):
            closed_result, closed_raw = generate_ncr_report(ncr_df, "Closed", closed_start, closed_end)
            open_result, open_raw = generate_ncr_report(ncr_df, "Open", open_end)

            all_reports = {}
            if "error" not in closed_result:
                all_reports["NCR resolved beyond 21 days"] = closed_result["Closed"]
            else:
                all_reports["NCR resolved beyond 21 days"] = {"error": closed_result["error"]}
            if "error" not in open_result:
                all_reports["NCR open beyond 21 days"] = open_result["Open"]
            else:
                all_reports["NCR open beyond 21 days"] = {"error": open_result["error"]}

            # Store in session state
            st.session_state['ncr_report'] = all_reports

        # Generate Housekeeping Reports (Open and Closed)
        with st.spinner("Generating Housekeeping Reports..."):
            closed_result_hk, closed_raw_hk = generate_ncr_Housekeeping_report(
                housekeeping_df,
                report_type="Closed",
                start_date=closed_start.strftime('%Y/%m/%d') if closed_start else None,
                end_date=closed_end.strftime('%Y/%m/%d') if closed_end else None,
                open_until_date=None
            )
            open_result_hk, open_raw_hk = generate_ncr_Housekeeping_report(
                housekeeping_df,
                report_type="Open",
                start_date=None,
                end_date=None,
                open_until_date=open_end.strftime('%Y/%m/%d') if open_end else None
            )

            # Store in session state
            st.session_state['open_hk_report'] = open_result_hk
            st.session_state['closed_hk_report'] = closed_result_hk

        # Generate Safety Reports (Open and Closed)
        with st.spinner("Generating Safety Reports..."):
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

            # Store in session state
            st.session_state['open_safety_report'] = open_result_safety
            st.session_state['closed_safety_report'] = closed_result_safety

        # Generate Combined Excel Report
        with st.spinner("Generating Combined Excel Report..."):
            excel_file = generate_combined_excel_report(
                all_reports,
                open_result_hk,
                closed_result_hk,
                open_result_safety,
                closed_result_safety,
                report_title_ncr,
                report_title_housekeeping,
                report_title_safety
            )

        # Provide download button
        st.success("✅ Combined report generated successfully!")
        st.download_button(
            label="📥 Download Combined Report",
            data=excel_file,
            file_name=f"Combined_NCR_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_combined_report"
        )

    except Exception as e:
        st.error(f"Error generating combined report: {str(e)}")
        logger.error(f"Combined report error: {str(e)}", exc_info=True)