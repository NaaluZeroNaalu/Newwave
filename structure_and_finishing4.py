import streamlit as st
import pandas as pd
import requests
import json
import io
import re

WATSONX_API_URL = "https://us-south.ml.cloud.ibm.com/ml/v1/text/generation?version=2023-05-29"
MODEL_ID = "meta-llama/llama-3-2-90b-vision-instruct"
PROJECT_ID = "4152f31e-6a49-40aa-9b62-0ecf629aae42"
API_KEY = "KEmIMzkw273qBcek8IdF-aShRUvFwH7K4psARTqOvNjI"

datas = []
tower4f = None


if 'eligotg' not in st.session_state:
    st.session_state.eligotg = None
if 'eligoth' not in st.session_state:
    st.session_state.eligoth = None
if 'veridia4' not in st.session_state:
    st.session_state.veridia4 = None
if 'veridia5' not in st.session_state:
    st.session_state.veridia5 = None

def GetAccesstoken():
    auth_url = "https://iam.cloud.ibm.com/identity/token"
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }
    
    data = {
        "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
        "apikey": API_KEY
    }
    response = requests.post(auth_url, headers=headers, data=data)
    
    if response.status_code != 200:
        st.write(f"Failed to get access token: {response.text}")
        return None
    else:
        token_info = response.json()
        return token_info['access_token']
    


def generatePrompt(datas):
    body = {
        "input": f"""
    
      Read all data from this table carefully:
         
        {datas}.
        
        i want percentage of tower as a number, the tower name is in the table and provide the data in the following JSON format for json loads
        Convert the decimal value into a percentage string (e.g., 0.04 → "4%")
        json fromat
        {{
         'tower_name': 'tower_name',
         'percentage': 'percentage_value'
        }}

        Note: Return the result strictly as a JSON object—no code, no explanations, and dont add any notes, and steps please only the JSON like previous output.

        """, 
        "parameters": {
            "decoding_method": "greedy",
            "max_new_tokens": 8100,
            "min_new_tokens": 0,
            "stop_sequences": [";"],
            "repetition_penalty": 1.05,
            "temperature": 0.5
        },
        "model_id": MODEL_ID,
        "project_id": PROJECT_ID
    }
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {GetAccesstoken()}"
    }
    
    if not headers["Authorization"]:
        return "Error: No valid access token."
    
    response = requests.post(WATSONX_API_URL, headers=headers, json=body)
    
    if response.status_code != 200:
        st.write(f"Failed to generate prompt: {response.text}")
        return "Error generating prompt"
    # st.write(json_datas)
    return response.json()['results'][0]['generated_text'].strip()



def structure4567(file):
    count = 0
    sheets = ["Tower 4", "Tower 5", "Tower 6", "Tower 7"]
    reenames = ["Tower4", "Tower6", "Tower7", "Tower5"]
    # st.write(f"Processing file: `{file.name}`")
    
    try:
        # excel_data = pd.ExcelFile(file)
        excel_data = file
        for sheet in excel_data.sheet_names:
            if sheet in sheets:
                st.markdown(f"#### Sheet: `{sheet}`")
                df = excel_data.parse(sheet, header=None)
                # st.dataframe(df.head()[[3, 6]])
                percentage = json.loads(generatePrompt(df.head()[[3, 6]]))['percentage']
                datas.append({
                    "Project":"Eden",
                    "Tower Name": reenames[count],
                    "Structure": percentage,
                    "Finishing":"0%"
                })
                count = count + 1
                # datas.sort(key=lambda x: int(re.search(r'\d+', x["Tower Name"]).group()))
    except Exception as e:
        st.write(f"Error processing file: {e}")
    # st.write(datas)
                #   st.write(datas)
    

def Tower_4_Finishing_Tracker(file):
    sheets = ["TOWER 4 FINISHING."]
    # st.write(f"Processing file: `{file}`")
    try:
        # excel_data = pd.ExcelFile(file)
        excel_data = file
        for sheet in excel_data.sheet_names:
             if sheet in sheets:
                #   st.markdown(f"#### Sheet: `{sheet}`")
                  df = excel_data.parse(sheet, header=None)
                #   st.dataframe(df.head()[[5, 12]])
                #   st.write(generatePrompt(df.head()[[5, 12]]))
                #   datas.append(json.loads(generatePrompt(df.head()[[5, 12]])))
                  percentage = json.loads(generatePrompt(df.head()[[5, 12]]))
                #   st.write(percentage)
                  st.session_state.veridia4 = percentage['percentage']
                 
                #   datas.append({
                #       "Project":"Veridia",
                #       "Tower Name": "Tower 4 Finishing",
                #       "Structure": "0%",
                #       "Finishing":percentage['percentage']
                #   })
                #   st.write(generatePrompt(df.head()[[5, 12]]))
    
    except Exception as e:
        st.error(f"Error in structure4567: {e}")

def Tower_5_Finishing_Tracker(file):
    sheets = ["TOWER 5 FINISHING."]
    # st.write(f"Processing file: `{file}`")
    try:
        # excel_data = pd.ExcelFile(file)
        excel_data = file
        for sheet in excel_data.sheet_names:
             if sheet in sheets:
                  st.markdown(f"#### Sheet: `{sheet}`")
                  df = excel_data.parse(sheet, header=None)
                  st.dataframe(df.head()[[5, 12]])
                #   st.write(generatePrompt(df.head()[[5, 12]]))
                #   datas.append(json.loads(generatePrompt(df.head()[[5, 12]])))
                  percentage = json.loads(generatePrompt(df.head()[[5, 12]]))
                  st.session_state.veridia5 = percentage['percentage']
                #   st.write(percentage)
                #   datas.append({
                #       "Project":"Veridia",
                #       "Tower Name": "Tower 5 Finishing",
                #       "Structure": "0%",
                #       "Finishing": percentage['percentage']
                #   })
                #   st.write(generatePrompt(df.head()[[5, 12]]))
    except Exception as e:
        st.error(f"Error in structure4567: {e}")

def Tower_G_Finishing_Tracker(file):
    sheets = ["Tower G Finishing"]
    st.write(f"Processing file: `{file}`")
    try:
        # excel_data = pd.ExcelFile(file)
        excel_data = file
        for sheet in excel_data.sheet_names:
             if sheet in sheets:
                  st.markdown(f"#### Sheet: `{sheet}`")
                  df = excel_data.parse(sheet, header=None)
                #   st.dataframe(df.head()[[5, 12]])
                #   st.write(generatePrompt(df.head()[[5, 12]]))
                #   datas.append(json.loads(generatePrompt(df.head()[[5, 12]])))
                  percentage = json.loads(generatePrompt(df.head()[[5, 12]]))
                  st.session_state.eligotg = percentage['percentage']
                #   st.write(percentage)
                #   datas.append({
                #       "Project":"Eligo",
                #       "Tower Name": "Tower G Finishing",
                #       "Structure": "0%",
                #       "Finishing": percentage['percentage']
                #   })
                #   st.write(generatePrompt(df.head()[[5, 12]]))
    except Exception as e:
        st.error(f"Error in structure4567: {e}")

def Tower_H_Finishing_Tracker(file):
    sheets = ["Pre- Construction Activities"]
    # st.write(f"Processing file: `{file}`")
    try:
        # excel_data = pd.ExcelFile(file)
        excel_data = file
        for sheet in excel_data.sheet_names:
             if sheet in sheets:
                  st.markdown(f"#### Sheet: `{sheet}`")
                  df = excel_data.parse(sheet, header=None)
                #   st.dataframe(df.head()[[6, 12]])
                #   st.write(generatePrompt(df.head()[[6, 12]]))
                #   datas.append(json.loads(generatePrompt(df.head()[[6, 12]])))
                #   a = generatePrompt(df.head()[[6, 12]].columns['Activity','percentage'])
                  a = generatePrompt(df.head()[[6, 12]].rename(columns={df.columns[6]: 'Activity', df.columns[12]: 'Percentage'}))    
                #   st.write(a)
                  percentage = json.loads(a)
                  st.session_state.eligoth = percentage['percentage']
                #   st.write(percentage)
                #   datas.append({
                #       "Project":"Eligo",
                #       "Tower Name": "Tower H Finishing",
                #        "Structure": "0%",
                #       "Finishing": percentage['percentage']
                #   })
                #   st.write(generatePrompt(df.head()[[5, 12]]))
    except Exception as e:
        st.error(f"Error in structure4567: {e}")


# def Getprecentage(file):
#     if file:
#         for uploaded_file in file:
#             # st.markdown(f"### 📁 File: `{uploaded_file.name}`")
#             if uploaded_file.startswith("Structure Work Tracker Tower 4,5,6 & 7"):
#                 response = st.session_state.cos_client.get_object(Bucket="projectreport", Key=uploaded_file)
#                 excel = pd.ExcelFile(io.BytesIO(response['Body'].read()))
#                 # st.markdown(f"### 📁 File: `{uploaded_file.name}`")     
#                 structure4567(excel)
#             elif uploaded_file.startswith("Tower 4 Finishing Tracker"):
#                 # st.markdown(f"### 📁 File: `{uploaded_file.name}`") 
#                 response = st.session_state.cos_client.get_object(Bucket="projectreport", Key=uploaded_file)
#                 excel = pd.ExcelFile(io.BytesIO(response['Body'].read())) 
#                 Tower_4_Finishing_Tracker(excel)   
#             elif uploaded_file.startswith("Tower 5 Finishing Tracker"):
#                 # st.markdown(f"### 📁 File: `{uploaded_file.name}`")
#                 response = st.session_state.cos_client.get_object(Bucket="projectreport", Key=uploaded_file)
#                 excel = pd.ExcelFile(io.BytesIO(response['Body'].read()))  
#                 Tower_5_Finishing_Tracker(excel)   
#             elif uploaded_file.startswith("Tower G Finishing Tracker"):
#                 # st.markdown(f"### 📁 File: `{uploaded_file.name}`")  
#                 response = st.session_state.cos_client.get_object(Bucket="projectreport", Key=uploaded_file)
#                 excel = pd.ExcelFile(io.BytesIO(response['Body'].read()))
#                 Tower_G_Finishing_Tracker(excel) 
#             elif uploaded_file.startswith("Tower H Finishing Tracker"):
#                 # st.markdown(f"### 📁 File: `{uploaded_file.name}`")  
#                 response = st.session_state.cos_client.get_object(Bucket="projectreport", Key=uploaded_file)
#                 excel = pd.ExcelFile(io.BytesIO(response['Body'].read()))
#                 Tower_H_Finishing_Tracker(excel) 

     
#     return datas
def Getprecentage(file):
    if file:
        for uploaded_file in file:
            # st.markdown(f"### 📁 File: `{uploaded_file.name}`")
            if "Structure Work Tracker Tower 4,5,6 & 7" in uploaded_file:
                response = st.session_state.cos_client.get_object(Bucket="projectreportnew", Key=uploaded_file)
                excel = pd.ExcelFile(io.BytesIO(response['Body'].read()))
                # st.markdown(f"### 📁 File: `{uploaded_file.name}`")     
                structure4567(excel)
            elif "Tower 4 Finishing Tracker" in uploaded_file:
                # st.markdown(f"### 📁 File: `{uploaded_file.name}`") 
                response = st.session_state.cos_client.get_object(Bucket="projectreportnew", Key=uploaded_file)
                excel = pd.ExcelFile(io.BytesIO(response['Body'].read())) 
                Tower_4_Finishing_Tracker(excel)   
            elif "Tower 5 Finishing Tracker" in uploaded_file:
                # st.markdown(f"### 📁 File: `{uploaded_file.name}`")
                response = st.session_state.cos_client.get_object(Bucket="projectreportnew", Key=uploaded_file)
                excel = pd.ExcelFile(io.BytesIO(response['Body'].read()))  
                Tower_5_Finishing_Tracker(excel)   
            elif "Tower G Finishing Tracker" in uploaded_file:
                # st.markdown(f"### 📁 File: `{uploaded_file.name}`")  
                response = st.session_state.cos_client.get_object(Bucket="projectreportnew", Key=uploaded_file)
                excel = pd.ExcelFile(io.BytesIO(response['Body'].read()))
                Tower_G_Finishing_Tracker(excel) 
            elif "Tower H Finishing Tracker" in uploaded_file:
                # st.markdown(f"### 📁 File: `{uploaded_file.name}`")  
                response = st.session_state.cos_client.get_object(Bucket="projectreportnew", Key=uploaded_file)
                excel = pd.ExcelFile(io.BytesIO(response['Body'].read()))
                Tower_H_Finishing_Tracker(excel)

            # st.write(datas) 

    return datas

# uploaded_files = st.file_uploader("Upload Excel files", type=["xlsx", "xls"], accept_multiple_files=True)

# if uploaded_files:
    
#     for uploaded_file in uploaded_files:
#         st.markdown(f"### 📁 File: `{uploaded_file.name}`")
#         if uploaded_file.name.startswith("Structure Work Tracker Tower 4,5,6 & 7"):
#             st.markdown(f"### 📁 File: `{uploaded_file.name}`")     
#             structure4567(uploaded_file)
        # elif uploaded_file.name.startswith("Tower 4 Finishing Tracker"):
        #     st.markdown(f"### 📁 File: `{uploaded_file.name}`")  
        #     Tower_4_Finishing_Tracker(uploaded_file)   
        # elif uploaded_file.name.startswith("Tower 5 Finishing Tracker"):
        #     st.markdown(f"### 📁 File: `{uploaded_file.name}`")  
        #     Tower_5_Finishing_Tracker(uploaded_file)   
        # elif uploaded_file.name.startswith("Tower G Finishing Tracker"):
        #     st.markdown(f"### 📁 File: `{uploaded_file.name}`")  
        #     Tower_G_Finishing_Tracker(uploaded_file) 
        # elif uploaded_file.name.startswith("Tower H Finishing Tracker"):
        #     st.markdown(f"### 📁 File: `{uploaded_file.name}`")  
        #     Tower_H_Finishing_Tracker(uploaded_file) 
    # veridia = pd.DataFrame(Getprecentage(uploaded_files))
    # st.write(veridia)

# def Getprecentage(file):
#     # Initialize a list to collect data from all processing functions
#     if file:
#         for uploaded_file in file:
#             st.markdown(f"### 📁 File: `{uploaded_file.name}`")
#             # file_name = uploaded_file.name.lower()  # Normalize to lowercase for matching
#             try:
#                 if "structure work tracker tower 4,5,6 & 7" in file_name:
#                     st.write("Processing Structure Work Tracker Tower 4,5,6 & 7")
#                     structure4567(uploaded_file)
                    
#                 elif "tower 4 finishing tracker" in file_name:
#                     st.write("Processing Tower 4 Finishing Tracker")
#                     Tower_4_Finishing_Tracker(uploaded_file)
                    
#                 elif "tower 5 finishing tracker" in file_name:
#                     st.write("Processing Tower 5 Finishing Tracker")
#                     Tower_5_Finishing_Tracker(uploaded_file)
                    
#                 elif "tower g finishing tracker" in file_name:
#                     st.write("Processing Tower G Finishing Tracker")
#                     Tower_G_Finishing_Tracker(uploaded_file)
                    
#                 elif "tower h finishing tracker" in file_name:
#                     st.write("Processing Tower H Finishing Tracker")
#                     Tower_H_Finishing_Tracker(uploaded_file)
                   
#                 else:
#                     st.warning(f"No matching processor for file: {uploaded_file.name}")
               
#             except Exception as e:
#                 st.error(f"Error processing {uploaded_file.name}: {str(e)}")
#     else:
#         st.warning("No files uploaded.")
    
    # return datas

