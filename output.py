import streamlit as st

pages = {

    "REPORTS":[
        st.Page("dashboard.py",title="Dashboard",icon=":material/dashboard:"),
        st.Page("ncr.py",title="NCR Report(NCR, HouseKeeping & Safety)",icon=":material/dataset:"),
        st.Page("overall.py",title="Overall  Project Completion - Structure and Finishing ",icon=":material/view_timeline:"),
        st.Page("shedule_report.py",title="Flatwise Report",icon=":material/apartment:"),
        st.Page("slabreport.py",title="slab report",icon=":material/engineering:"),
        st.Page("timedelay.py",title="Time Delay",icon=":material/home:"),
        # st.Page("test.py",title="test",icon=":material/home:"),
        st.Page("Milestone.py",title="Milestone",icon=":material/home:"),
        st.Page("MilestoneFinishing.py",title="MilestoneFinishing",icon=":material/home:"),
        # st.Page("download.py",title="Download",icon=":material/home:"),

    ]
}


st.navigation(pages).run()
