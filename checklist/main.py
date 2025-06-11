import streamlit as st


pages = {
    "Projects":[
        st.Page("veridia.py",title="VERIDIA"),
        st.Page("eden.py",title="EDEN")
    ]
}


pg = st.navigation(pages)
pg.run()