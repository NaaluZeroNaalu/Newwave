import streamlit as st
import pandas as pd
import io
import re
from io import BytesIO
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import urllib.parse
import ibm_boto3
from ibm_botocore.client import Config
import io

COS_API_KEY = "ehl6KMyT95fwzKf7sPW_X3eKFppy_24xbm4P1Yk-jqyU"
COS_SERVICE_INSTANCE_ID = "crn:v1:bluemix:public:cloud-object-storage:global:a/fddc2a92db904306b413ed706665c2ff:e99c3906-0103-4257-bcba-e455e7ced9b7:bucket:projectreportnew"
COS_ENDPOINT = "https://s3.us-south.cloud-object-storage.appdomain.cloud"
COS_BUCKET = "projectreportnew"



st.session_state.cos_client = ibm_boto3.client(
    's3',
    ibm_api_key_id=COS_API_KEY,
    ibm_service_instance_id=COS_SERVICE_INSTANCE_ID,
    config=Config(signature_version='oauth'),
    endpoint_url=COS_ENDPOINT
)


#------------------FOR NCR---------
if 'ncr' not in st.session_state:
    st.session_state.ncr = None

if 'ncrdf' not in st.session_state:
    st.session_state.ncrdf = None
#------------------FOR NCR---------

if 'file' not in st.session_state:
    st.session_state.file = None

if 'filedf' not in st.session_state:
    st.session_state.filedf = None

if 'structure_and_finishing' not in st.session_state:
    st.session_state.structure_and_finishing = None

if 'safdf' not in st.session_state:
    st.session_state.safdf = None

if 'shedule' not in st.session_state:
    st.session_state.shedule = None

if 'sheduledf' not in st.session_state:
    st.session_state.shedulef = None


#--------sessions for safety--------------

if 'safetyopen' not in st.session_state:
    st.session_state.safetyopen = None

if 'safetyclose' not in st.session_state:
    st.session_state.safetyclose = None

if 'safetyclosedf' not in st.session_state:
    st.session_state.safetyclosedf = None

if 'safetyopendf' not in st.session_state:
    st.session_state.safetyopendf = None

#--------sessions for safety--------------

#--------sessions for house--------------

if 'houseopen' not in st.session_state:
    st.session_state.houseopen = None

if 'houseclose' not in st.session_state:
    st.session_state.houseclose = None

if 'houseopendf' not in st.session_state:
    st.session_state.houseopendf = None

if 'houseclosedf' not in st.session_state:
    st.session_state.houseclosedf = None

#--------sessions for house--------------

def create_combined_excel():
    wb = Workbook()
    # Remove default sheet
    wb.remove(wb.active)
    
    # Dictionary mapping session state keys to sheet names
    reports = {
        'ncr': 'NCR_Report',
        'structure_and_finishing': 'Structure_and_Finishing',
        'shedule': 'Schedule',
        'safety': 'Safety',
        'house': 'House'
    }
    
    for key, sheet_name in reports.items():
        if st.session_state.get(key) is not None:
            # Convert BytesIO to DataFrame
            df = pd.read_excel(st.session_state[key])
            ws = wb.create_sheet(sheet_name)
            for r in dataframe_to_rows(df, index=False, header=True):
                ws.append(r)
    
    # Save to BytesIO
    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output

st.divider()
if st.session_state.ncr == None:
    st.write("Run to get output")
else:
    st.write("NCR Report")
    # st.write(st.session_state.ncrdf)
   
    st.download_button(
            label="📥 Download Combined Excel Report",
            data=st.session_state.ncr,
            file_name=f"NCR_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    

st.divider()


if st.session_state.structure_and_finishing == None:
    st.write("Run to get output")
else:
    st.write("structure_and_finishing")
    st.dataframe(st.session_state.structure_and_finishingdf)
    # json_datas = st.session_state.structure_and_finishingdf.to_json(orient='records', lines=False)
    # st.json(st.session_state.structure_and_finishingdf.to_json(orient='records', lines=False))
    # st.dataframe(st.session_state.structure_and_finishingdf.to_json(orient='records', lines=False))

    # df = pd.DataFrame(st.session_state.structure_and_finishingdf.to_json(orient='records', lines=False))

    # Display the DataFrame in Streamlit
    # st.title("Project Progress Overview")
    # st.dataframe(df)

    # Updated data (restructured)
    


    st.download_button(
            label="📥 Download Excel Report",
            data=st.session_state.structure_and_finishing,
            file_name=f"Structure_and_finishing_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )  
   

st.divider()
if st.session_state.shedule == None:
    st.write("Run to get output")
else:
    st.write("Shedule Report")
    st.write(st.session_state.sheduledf)  
    
    st.download_button(
            label="📥 Download Excel Report",
            data=st.session_state.shedule,
            file_name=f"Shedule_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )  
   
st.divider()
if st.session_state.safetyopen == None:
    st.write("Run to get output")
else:
    st.write("Safety Report")
    st.title("Safety Open")
    # st.write(st.session_state.safetyopendf) 
    if 'error' in st.session_state.safetyopendf:
        st.write("no records found in safety open")
    
    if 'error' in st.session_state.safetyclosedf:
        st.write("no records found in safety close")
    else:
        site_data = []
        for site, details in st.session_state.safetyclosedf["Safety"]["Sites"].items():
            for idx, desc in enumerate(details["Descriptions"], 1):
                site_data.append({
                    "Site": site,
                    "Issue #": idx,
                    "Description": desc
                })

    # Creating a DataFrame
    df = pd.DataFrame(site_data)

    # Streamlit app display
    st.title("Safety Close")
    st.table(df)
    # st.write(st.session_state.safetyclosedf)  
    st.download_button(
            label="📥 Download Excel Report",
            data=st.session_state.safetyclose,
            file_name=f"Safety_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )  
    
    

st.divider()
if st.session_state.houseopendf == None:
    st.write("Run to get output")
else:
    st.write("House Report")
    st.write("House Open")
    if 'error' in st.session_state.houseopendf:
        st.write("No records found in house open")
    else:
        st.write(st.session_state.houseopendf)
    st.write("House Close")

    if 'error' in st.session_state.houseclosedf:
        st.write("No records found in house close")
    else:

        st.write(st.session_state.houseclosedf)

        st.download_button(
                label="📥 Download Excel Report",
                data=st.session_state.houseclose,
                file_name=f"House_Report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ) 
        
def generate_ncr_excel(writer, combined_result, report_title="NCR"):
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
        'border': 1
    })
    site_format = workbook.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'border': 1
    })
    
    # Create worksheet
    worksheet = workbook.add_worksheet('NCR')
    
    # Set column widths
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
    
    # Define standard sites
    standard_sites = [
        "Veridia-Club",
        "Veridia- Tower 01",
        "Veridia- Tower 02",
        "Veridia- Tower 03",
        "Veridia- Tower 04",
        "Veridia- Tower 05",
        "Veridia- Tower 06",
        "Veridia- Tower 07",
        "Veridia-Commercial",
        "External Development",
        "Common_Area"
    ]
    
    # Normalize site names
    def normalize_site_name(site):
        if site in ["Veridia-Club", "Veridia-Commercial"]:
            return site
        match = re.search(r'(?:tower|t)[- ]?(\d+)', site, re.IGNORECASE)
        if match:
            num = match.group(1).zfill(2)
            return f"Veridia- Tower {num}"
        return site

    # Create a reverse mapping for original keys to normalized names
    site_mapping = {k: normalize_site_name(k) for k in (resolved_sites.keys() | open_sites.keys())}
    
    # Sort the standard sites
    sorted_sites = sorted(standard_sites)
    
    # Title row
    worksheet.merge_range('A1:H1', report_title, title_format)
    
    # Header row
    row = 1
    worksheet.write(row, 0, 'Site', header_format)
    worksheet.merge_range(row, 1, row, 3, 'NCR resolved beyond 21 days', header_format)
    worksheet.merge_range(row, 4, row, 6, 'NCR open beyond 21 days', header_format)
    worksheet.write(row, 7, 'Total', header_format)
    
    # Subheaders
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
    
    # Map categories to JSON data categories
    category_map = {
        'Civil Finishing': 'FW',
        'MEP': 'MEP',
        'Structure': 'SW'
    }
    
    # Data rows
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

def generate_housekeeping_excel(writer, combined_result, report_title="Housekeeping: Current Month"):
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
    
    # Create worksheet
    worksheet = workbook.add_worksheet('Housekeeping')
    worksheet.set_column('A:A', 20)
    worksheet.set_column('B:B', 15)
    worksheet.set_column('C:C', 60)
    
    # Get data
    data = combined_result.get("Housekeeping", {}).get("Sites", {})
    
    # Dynamically generate standard sites from data
    standard_sites = sorted(set(data.keys()) | set([
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
    ]))
    
    def normalize_site_name(site):
        if site in standard_sites:
            return site
        match = re.search(r'(?:tower|t)[- ]?(\d+|2021|28)', site, re.IGNORECASE)
        if match:
            num = match.group(1).zfill(2)
            return f"Veridia-Tower{num}"
        return site

    # Normalize site names
    site_mapping = {k: normalize_site_name(k) for k in data.keys()}
    
    # Write title
    worksheet.merge_range('A1:C1', report_title, title_format)
    
    # Write headers
    row = 1
    worksheet.write(row, 0, 'Site', header_format)
    worksheet.write(row, 1, 'No. of Housekeeping NCRs open beyond 7 days', header_format)
    worksheet.write(row, 2, 'Description', header_format)
    
    # Write data
    row = 2
    for site in standard_sites:
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

def generate_safety_excel(writer, combined_result, report_title="Safety: Current Month"):
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
    
    # Create worksheet
    worksheet = workbook.add_worksheet('Safety')
    worksheet.set_column('A:A', 20)
    worksheet.set_column('B:B', 15)
    worksheet.set_column('C:C', 60)
    
    # Get data
    data = combined_result.get("Safety", {}).get("Sites", {})
    
    # Define standard sites
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

    # Normalize site names
    site_mapping = {k: normalize_site_name(k) for k in data.keys()}
    sorted_sites = sorted(standard_sites)
    
    # Write title
    worksheet.merge_range('A1:C1', report_title, title_format)
    
    # Write headers
    row = 1
    worksheet.write(row, 0, 'Site', header_format)
    worksheet.write(row, 1, 'No. of Safety NCRs open beyond 7 days', header_format)
    worksheet.write(row, 2, 'Description', header_format)
    
    # Write data
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
        
# Corrected required_keys to match session state variables
required_keys = ['ncr', 'structure_and_finishing', 'shedule', 'safetyopen', 'houseopen']
all_sessions_valid = all(st.session_state.get(key) is not None for key in required_keys)

if all_sessions_valid:
    # Create a BytesIO buffer to store the Excel file
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # 1. NCR Report
        generate_ncr_excel(writer, st.session_state.ncrdf, report_title="NCR")

        # 2. Structure and Finishing Report
        if isinstance(st.session_state.structure_and_finishingdf, pd.DataFrame):
            st.session_state.structure_and_finishingdf.to_excel(writer, sheet_name="Structure_and_Finishing", index=False)
        else:
            pd.DataFrame().to_excel(writer, sheet_name="Structure_and_Finishing", index=False)

        # 3. Schedule Report
        try:
            schedule_df = pd.read_excel(st.session_state.shedule)
            schedule_df.to_excel(writer, sheet_name="Schedule", index=False)
        except Exception as e:
            st.warning(f"Failed to read Schedule Excel data: {e}")
            pd.DataFrame(columns=['Activity']).to_excel(writer, sheet_name="Schedule", index=False)

        # 4. Safety Report
            generate_safety_excel(writer, st.session_state.safetyopendf, report_title="Safety: Current Month")

            # 5. Housekeeping Report
            generate_housekeeping_excel(writer, st.session_state.houseopendf, report_title="Housekeeping: Current Month")

    # Reset the buffer position to the start
    output.seek(0)

    # Provide the download button for the combined Excel file
    st.download_button(
        label="📥 Download All Reports",
        data=output,
        file_name="All_Reports.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.warning("Please generate all reports before downloading the combined file.")
# required_keys = ['ncr', 'structure_and_finishing', 'shedule', 'safety', 'house']
# all_sessions_valid = all(st.session_state.get(key) is not None for key in required_keys)

# # Read the file from disk as bytes
# file_path = "combined_report.xlsx"  # <-- Change this to your actual file path
# try:
#     with open(file_path, "rb") as f:
#         file_bytes = f.read()

#     # Show download button only if all session states are valid
#     if all_sessions_valid:
#         st.download_button(
#             label="📥 Download All Reports",
#             data=file_bytes,
#             file_name="All_Reports.xlsx",
#             mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#         )
# except FileNotFoundError:
#     st.error(f"File not found at path: {file_path}")