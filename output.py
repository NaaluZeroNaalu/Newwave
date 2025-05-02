import streamlit as st

pages = {

    "REPORTS":[
        st.Page("dashboard.py",title="Dashboard",icon=":material/dashboard:"),
        st.Page("ncr.py",title="NCR Report(NCR, HouseKeeping & Safety)",icon=":material/dataset:"),
        st.Page("structure_and_finishing_main.py",title="Overall  Project Completion - Structure and Finishing ",icon=":material/view_timeline:"),
        st.Page("shedule_report.py",title="Flatwise Report",icon=":material/apartment:"),
        st.Page("slabreport.py",title="slab report",icon=":material/engineering:"),
        # st.Page("House.py",title="House keeping report",icon=":material/home:"),
        # st.Page("testcos.py",title="test",icon=":material/home:"),


    ]
}


st.navigation(pages).run()
