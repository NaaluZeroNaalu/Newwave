import streamlit as st
import pandas as pd
import requests
import json
import time





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



def GetTower4Finishing(exceldatas):

    try:
        datas = pd.read_excel(exceldatas, sheet_name='TOWER 4 FINISHING.')
        # Ai_answer = generatePrompt(datas.head()[['Activity Name', '% Complete']], "Tower 4")
        # json_data = json.loads(Ai_answer)
            # st.write(Ai_answer)
        # st.write(datas.head()['% Complete'][1])
        st.session_state.tower4_finishing = str(datas.head()['% Complete'][1]) + "%"
    except:
        st.session_state.tower4_finishing = "Error While Read Excel"
        st.write("Error While Read Excel")
    
def GetTower5Finishing(exceldatas):
    
    try:
        datas = pd.read_excel(exceldatas, sheet_name='TOWER 5 FINISHING.')
        # Ai_answer = generatePrompt(datas.head(2)[['Activity Name', '% Complete']], "Tower 5")
        # st.write(Ai_answer)
        # json_data = json.loads(Ai_answer)
        # st.write(datas.head(2)['% Complete'][1])
        st.session_state.tower5_finishing = str(datas.head(2)['% Complete'][1]) + "%"
    except:
        st.session_state.tower5_finishing = "Error When Generating Prompt"
        st.write("Error When Generating Prompt")
  

def GetTowerGFinishing(exceldatas):

    try:
        datas = pd.read_excel(exceldatas, sheet_name='Tower G Finishing')
        # st.write(datas.head()[['Activity Name', '% Complete']])
        # Ai_answer = generatePrompt(datas.head()[['Activity Name', '% Complete']], "Tower G")
        # st.write(Ai_answer)
        # json_data = json.loads(Ai_answer)
        # st.write(datas.head()[['Activity Name', '% Complete']])
        st.session_state.towerg_finishing = str(datas.head()['% Complete'][1]) + "%"
    except:
        st.session_state.towerg_finishing = "Error When Read Excel"
        st.write("Error When Read Excel")

    
def GetTowerHFinishing(exceldatas):

    try:
        datas = pd.read_excel(exceldatas, sheet_name='Tower H Finishing')
        # st.write(datas.head()[['Activity Name', '% Complete']])
        # Ai_answer = generatePrompt(datas.head(2)[['Activity Name', '% Complete']], "Tower H")
        # st.write(Ai_answer)
        # json_data = json.loads(Ai_answer)
        st.session_state.towerh_finishing = str(datas.head()['% Complete'][1]) + "%"
    except:
        st.session_state.towerh_finishing = "Error When Read Excel"
        st.write("Error When Read Excel")
  

