import psycopg2
import streamlit as st

def get_connection():
    return psycopg2.connect(
        host=st.secrets["db"]["host"],
        port=st.secrets["db"]["port"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        database=st.secrets["db"]["database"],
        sslmode="verify-full",
        sslrootcert=r"C:\Users\Praveen\AppData\Roaming\postgresql\root.crt"
    )
