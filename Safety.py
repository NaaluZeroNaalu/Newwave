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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    st.error("❌ Required environment variables missing!")
    logger.error("Missing required environment variables")
    st.stop()

# API Endpoints
LOGIN_URL = "https://dms.asite.com/apilogin/"
SEARCH_URL = "https://adoddleak.asite.com/commonapi/formsearchapi/search"
IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"
FALLBACK_IAM_TOKEN_URL = "https://iam.test.cloud.ibm.com/identity/token"

# Function to generate access token with reduced timeout
def get_access_token(API_KEY):
    session = requests.Session()
    retry_strategy = Retry(
        total=7,
        backoff_factor=1,  # Reduced backoff for faster retries
        status_forcelist=[500, 502, 503, 504, 429],
        allowed_methods=["POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    data = {"grant_type": "urn:ibm:params:oauth:grant-type:apikey", "apikey": API_KEY}
    
    for attempt in range(2):
        try:
            response = session.post(IAM_TOKEN_URL, headers=headers, data=data, verify=certifi.where(), timeout=60)
            if response.status_code == 200:
                token_info = response.json()
                logger.info("Access token generated successfully from primary endpoint")
                return token_info['access_token']
            else:
                logger.error(f"Failed to get access token from primary: {response.status_code}")
        except Exception as e:
            logger.error(f"Exception in primary endpoint: {str(e)}")
        
        logger.info("Switching to fallback IAM endpoint")
        try:
            response = session.post(FALLBACK_IAM_TOKEN_URL, headers=headers, data=data, verify=certifi.where(), timeout=60)
            if response.status_code == 200:
                token_info = response.json()
                logger.info("Access token generated successfully from fallback endpoint")
                return token_info['access_token']
            else:
                logger.error(f"Failed to get access token from fallback: {response.status_code}")
        except Exception as e:
            logger.error(f"Exception in fallback endpoint: {str(e)}")
    
    logger.error("All attempts to get access token failed")
    st.error("❌ All attempts to get access token failed")
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
    logger.debug(f"DataFrame columns after processing: {df.columns.tolist()}")
    if df.empty:
        logger.warning("DataFrame is empty after processing")
        st.warning("⚠️ No data processed. Check the API response.")
    return df


def generate_ncr_report(df, report_type, start_date=None, end_date=None, open_until_date="2025/04/21"):
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
                    "Tower": "External Development"
                }

                desc_lower = description.lower()
                if any(phrase in desc_lower for phrase in ["veridia clubhouse", "veridia-clubhouse", "veridia club"]):
                    cleaned_record["Tower"] = "Veridia-Club"
                    logger.debug(f"Matched 'Veridia Clubhouse', setting Tower to Veridia-Club")
                else:
                    tower_match = re.search(r"(tower|t)\s*-?\s*(\d+|2021|28)", desc_lower, re.IGNORECASE)
                    cleaned_record["Tower"] = f"Veridia-Tower{tower_match.group(2).zfill(2)}" if tower_match else "Common_Area"
                    logger.debug(f"Tower set to {cleaned_record['Tower']}")

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
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504, 429, 408],
            allowed_methods=["POST"],
            raise_on_redirect=True,
            raise_on_status=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        progress_bar = st.progress(0)
        for i in range(0, len(cleaned_data), chunk_size):
            chunk = cleaned_data[i:i + chunk_size]
            current_chunk = i // chunk_size + 1
            progress = min((current_chunk / total_chunks) * 100, 100)
            progress_bar.progress(int(progress))
            st.write(f"Processed {current_chunk}/{total_chunks} chunks ({int(progress)}%)")
            logger.debug(f"Chunk data: {json.dumps(chunk, indent=2)}")

            prompt = (
                "IMPORTANT: YOU MUST RETURN ONLY A SINGLE VALID JSON OBJECT. DO NOT INCLUDE ANY TEXT, EXPLANATION, OR MULTIPLE RESPONSES.\n\n"
                "Task: For Safety NCRs, count EVERY record in the provided data by site ('Tower' field) where 'Discipline' is 'HSE' and 'Days' is greater than 7. The 'Description' MUST be counted if it contains ANY of the following construction safety issues: 'safety precautions','temporary electricity','on-site labor is working without wearing safety belt','safety norms','Missing Cabin Glass – Tower Crane','Crane Operator cabin front glass','site on priority basis lifeline is not fixed at the working place','operated only after Third Party Inspection and certification crane operated without TPIC','We have found that safety precautions are not taken seriously at site Tower crane operator cabin front glass is missing while crane operator is working inside cabin.','no barrier around','Lock and Key arrangement to restrict unauthorized operations, buzzer while operation, gates at landing platforms, catch net in the vicinity', 'safety precautions are not taken seriously','firecase','Health and Safety Plan','noticed that submission of statistics report is regularly delayed','crane operator cabin front glass is missing while crane operator is working inside cabin','labor is working without wearing safety belt', 'barricading', 'tank', 'safety shoes', 'safety belt', 'helmet', 'lifeline', 'guard rails', 'fall protection', 'PPE', 'electrical hazard', 'unsafe platform', 'catch net', 'edge protection', 'TPI', 'scaffold', 'lifting equipment', 'temporary electricity', 'dust suppression', 'debris chute', 'spill control', 'crane operator', 'halogen lamps', 'fall catch net', 'environmental contamination', 'fire hazard'. Use the 'Tower' values exactly as they appear in the data (e.g., 'Veridia-Club', 'Veridia-Tower01', 'Common_Area'). Assign each count to the 'Count' key, representing 'No. of Safety NCRs beyond 7 days'. If no matches are found for a site, set its count to 0, but ensure all present sites are listed. EXCLUDE records where 'housekeeping' is the PRIMARY safety concern (e.g., descriptions focusing solely on 'housekeeping' or 'cleaning' ).\n\n"
                "REQUIRED OUTPUT FORMAT (exactly this structure with actual counts):\n"
                "{\n"
                '  "Safety": {\n'
                '    "Sites": {\n'
                '      "Site_Name1": {\n'
                '         "Descriptions": ["description1", "description2"],\n'
                '        "Count": number\n'
                '      },\n'
                '      "Site_Name2": {\n'
                '        "Descriptions": ["description1", "description2"],\n'
                '        "Count": number\n'
                '      }\n'
                '    },\n'
                '    "Grand_Total": number\n'
                '  }\n'
                '}\n\n'
                f"Data: {json.dumps(chunk)}\n"
                "Return the result strictly as a single JSON object—no code, no explanations, no string literal like this ```, only the JSON."
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
                response = session.post(WATSONX_API_URL, headers=headers, json=payload, verify=certifi.where(), timeout=100)
                logger.info(f"WatsonX API response status: {response.status_code}")

                if response.status_code == 200:
                    api_result = response.json()
                    generated_text = api_result.get("results", [{}])[0].get("generated_text", "").strip()
                    logger.debug(f"Generated text: {generated_text}")

                    if generated_text:
                        json_match = re.search(r'({[\s\S]*?})(?=\n\{|\Z)', generated_text)
                        if json_match:
                            json_str = json_match.group(1)
                            try:
                                parsed_json = json.loads(json_str)
                                chunk_result = parsed_json.get("Safety", {})
                                chunk_sites = chunk_result.get("Sites", {})
                                chunk_grand_total = chunk_result.get("Grand_Total", 0)

                                for site, values in chunk_sites.items():
                                    if not isinstance(values, dict):
                                        logger.warning(f"Invalid site data for {site}: {values}, converting to dict")
                                        values = {"Count": int(values) if isinstance(values, (int, float)) else 0, "Descriptions": []}
                                    
                                    if site not in result["Safety"]["Sites"]:
                                        result["Safety"]["Sites"][site] = {"Count": 0, "Descriptions": []}
                                    
                                    if "Descriptions" in values and values["Descriptions"]:
                                        if not isinstance(values["Descriptions"], list):
                                            values["Descriptions"] = [str(values["Descriptions"])]
                                        result["Safety"]["Sites"][site]["Descriptions"].extend(values["Descriptions"])
                                    
                                    count = values.get("Count", 0)
                                    if not isinstance(count, (int, float)):
                                        count = 0
                                    result["Safety"]["Sites"][site]["Count"] += count
                                
                                result["Safety"]["Grand_Total"] += chunk_grand_total
                            except json.JSONDecodeError as e:
                                logger.error(f"JSONDecodeError: {str(e)} - Raw: {generated_text}")
                                st.error(f"Failed to parse JSON: {str(e)}")
                                continue
                        else:
                            logger.error("No valid JSON in response")
                            st.error("No valid JSON in response")
                            continue
                    else:
                        logger.error("Empty WatsonX response")
                        st.error("Empty WatsonX response")
                        continue
                else:
                    logger.error(f"WatsonX API error: {response.status_code} - {response.text}")
                    continue
            except Exception as e:
                logger.error(f"Exception during WatsonX call: {str(e)}")
                st.error(f"Exception during WatsonX call: {str(e)}")
                continue

        progress_bar.progress(100)

        for site in result["Safety"]["Sites"]:
            if "Descriptions" in result["Safety"]["Sites"][site]:
                result["Safety"]["Sites"][site]["Descriptions"] = list(set(result["Safety"]["Sites"][site]["Descriptions"]))
                
        return result, json.dumps(result)


    
# Generate Excel Report with Descriptions for Current Month
def generate_consolidated_ncr_excel(combined_result, report_title="Safety: Current Month"):
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
        
        worksheet = workbook.add_worksheet('Safety NCR Report')
        worksheet.set_column('A:A', 20)
        worksheet.set_column('B:B', 15)
        worksheet.set_column('C:C', 60)  # Wider column for descriptions
        
        data = combined_result.get("Safety", {}).get("Sites", {})
        
        standard_sites = [
            "Veridia-Club",
            "Veridia-Tower01",
            "Veridia-Tower02",
            "Veridia-Tower03",
            "Veridia-Tower04",
            "Veridia-Tower05",
            "Veridia-Tower06",
            "Veridia-Tower07",
            "Common_Area",
            "Veridia-Commercial",
            "External Development"
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
        
        worksheet.merge_range('A1:C1', report_title, title_format)
        row = 1
        worksheet.write(row, 0, 'Site', header_format)
        worksheet.write(row, 1, 'No. of Safety NCRs beyond 7 days', header_format)
        worksheet.write(row, 2, 'Description', header_format)
        
        row = 2
        for site in sorted_sites:
            worksheet.write(row, 0, site, site_format)
            original_key = next((k for k, v in site_mapping.items() if v == site), None)
            if original_key and original_key in data:
                value = data[original_key].get("Count", 0)
                descriptions = data[original_key].get("Descriptions", [])
                description_text = "\n".join(descriptions) if descriptions else ""
            else:
                value = 0
                description_text = ""
            worksheet.write(row, 1, value, cell_format)
            worksheet.write(row, 2, description_text, description_format)
            row += 1
        
        output.seek(0)
        return output

st.title("Safety NCR Reporter")

if "df" not in st.session_state:
    st.session_state["df"] = None

st.sidebar.title("🔒 Asite Login")
email = st.sidebar.text_input("Email", "impwatson@gadieltechnologies.com", key="email_input")
password = st.sidebar.text_input("Password", "Srihari@790$", type="password", key="password_input")
if st.sidebar.button("Login"):
    session_id = login_to_asite(email, password)
    if session_id:
        st.session_state["session_id"] = session_id
        st.sidebar.success("✅ Login Successful")

st.sidebar.title("📂 Project Data")
project_name = st.sidebar.text_input("Project Name", "Wave Oakwood, Wave City")
form_name = st.sidebar.text_input("Form Name", "Non Conformity Report")
if "session_id" in st.session_state and st.sidebar.button("Fetch Data"):
    header, data, payload = fetch_project_data(st.session_state["session_id"], project_name, form_name)
    st.json(header)
    if data:
        df = process_json_data(data)
        st.session_state["df"] = df
        st.dataframe(df)

if st.session_state["df"] is not None:
    df = st.session_state["df"]
    
    st.sidebar.title("📋 Combined NCR Report")
    closed_start = st.sidebar.date_input("Closed Start Date", df['Expected Close Date (WET)'].min())
    closed_end = st.sidebar.date_input("Closed End Date", df['Expected Close Date (WET)'].max(), key="closed_end")
    open_end = st.sidebar.date_input("Open Until Date", df['Expected Close Date (WET)'].max(), key="open_end")
    
    if st.sidebar.button("Generate Reports"):
        month_name = closed_end.strftime("%B")  # Use closed_end month for both reports
        # Generate Closed Report
        closed_result, closed_raw = generate_ncr_report(
            df,
            report_type="Closed",
            start_date=closed_start.strftime('%Y/%m/%d') if closed_start else None,
            end_date=closed_end.strftime('%Y/%m/%d') if closed_end else None,
            open_until_date=None
        )
        st.subheader("Closed Safety NCR Report (JSON)")
        st.json(closed_result)
        st.session_state.safetyclosedf = closed_result
        st.session_state.safetyclose  = generate_consolidated_ncr_excel(closed_result, f"Safety: Closed - {month_name}")
        st.download_button(
            label="📥 Download Closed Excel Report",
            data=st.session_state.safetyclose,
            file_name=f"Safety_NCR_Report_Closed_{month_name}_{datetime.now().strftime('%Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Generate Open Report
        open_result, open_raw = generate_ncr_report(
            df,
            report_type="Open",
            start_date=None,
            end_date=None,
            open_until_date=open_end.strftime('%Y/%m/%d') if open_end else None
        )
        st.subheader("Open Safety NCR Report (JSON)")
        st.json(open_result)
        st.session_state.safetyopendf = open_result
        st.session_state.safetyopen = generate_consolidated_ncr_excel(open_result, f"Safety: Open - {month_name}")
        # st.subheader("Safety NCR Report (JSON)")
        # st.json(result)
        # st.session_state.safetydf = result
        
        # st.session_state.safety = generate_consolidated_ncr_excel(result, report_title)
        st.download_button(
            label="📥 Download Excel Report",
            data=st.session_state.safetyopen,
            file_name=f"Safety_NCR_Report_{datetime.now().strftime('%B_%Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )