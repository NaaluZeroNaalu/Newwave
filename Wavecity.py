import streamlit as st
import pandas as pd
import requests
import json
import time
import math
import numpy as np



WATSONX_API_URL = "https://us-south.ml.cloud.ibm.com/ml/v1/text/generation?version=2023-05-29"
MODEL_ID = "meta-llama/llama-3-3-70b-instruct"
PROJECT_ID = "4152f31e-6a49-40aa-9b62-0ecf629aae42"
API_KEY = "KS5iR_XHOYc4N_xoId6YcXFjZR2ikINRdAyc2w2o18Oo"


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
    





def generatePrompt(datas, tower):
    # st.write("AI thinking")
    body = {
        "input": f"""
    
      Read all data from this table carefully:
         
        {datas}.
        
        get pecentage of {tower} from this table and give as a json
        Convert the decimal value into a percentage string (e.g., 0.04 → "4%")
        Need only JSON—not explanation, not string literal, no code formatting, and no repetition.
        
        give the json propery it is only one json data understand

        json fromat
        [{{
         'tower_name': 'tower_name',
         'percentage': 'percentage_value'
        }}]

        Note: Return the result strictly as a JSON object—no code, no explanations, and dont add any notes, and steps please only the JSON that contains towername and values.

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




def GetWaveCity(exceldatas):

    try:
        datas = pd.read_excel(exceldatas, sheet_name='MSP Progress',header=1)
        # st.write(datas.head())
        task_keywords = [
            "Wave City Club Start-finish ",
            "Block 1 (B1) Banquet Hall",
            "Block 6 (B6) Toilets",
            "Block 7(B7) Indoor Sports",
            "Block 9 (B9) Spa & Saloon",
            "Block 8 (B8) Squash Court",
            "Block 2 & 3 (B2 & B3) Cafe & Bar",
            "Block 4 (B4) Indoor Swimming Pool Changing Room & Toilets",
            "Block 11 (B11) Guest House",
            "Block 10 (B10) Gym",
            "Block 5 (B5) Admin + Member Lounge+Creche+Av Room + Surveillance Room +Toilets",
            "Fine Dine"
        ]

        def match_task(row):
            for keyword in task_keywords:
                if keyword in str(row):
                    return keyword
            return None
        
        datas['Matched Task'] = datas['Task Name'].apply(match_task)
        filtered_data = datas.dropna(subset=['Matched Task'])

        # Compute average % Complete
        result = (
            filtered_data
            .groupby('Matched Task')['% Complete']
            .mean()
            .reset_index()
        )
        # st.write(result)
        # Multiply to convert to percentage and format with %
        # result['% Complete'] = (result['% Complete'] * 100).round(1).astype(str) + "%"
        result['% Complete'] = np.round(result['% Complete'] * 100).astype(int).astype(str) + "%"

        # Build the JSON format
        json_data = [
            {
                "Project": "Wave City Club",
                "Tower Name": row['Matched Task'],
                "Structure": row['% Complete'],
                "Finishing": "0%"
            }
            for _, row in result.iterrows()
        ]
        # st.dataframe(json_data)

        # st.json(json_data)

        return json_data
        # st.session_state.wavecity_finishing = 
    except Exception as e:
        # st.session_state.tower4_finishing = "Error While Read Excel"

        st.write(f"Error While Read Excel{e}")
        return [{"Project":"Wave City Club","Tower Name":"Wave City","Strucutre":"Error While Generating","Finishing":"Error While Generating"}]
    

  
