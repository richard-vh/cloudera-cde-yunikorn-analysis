# code/ui.py
import streamlit as st
import pandas as pd
import datetime
import os
import base64
from pathlib import Path


# ==========================================
# CONFIGURATION - RETRIEVED FROM OS ENV
# ==========================================
required_vars = [
    "IMPALA_CONN_NAME", 
    "TABLE_SCHEMA",
    "TABLE_NAME"
]

# Check if all environment variables exist
for var in required_vars:
    if var not in os.environ:
        raise OSError(f"Environment variable '{var}' does not exist")


IMPALA_CONN_NAME = os.environ["IMPALA_CONN_NAME"]
TARGET_TABLE_SCHEMA = os.environ["TABLE_SCHEMA"]
TARGET_TABLE_NAME = os.environ["TABLE_NAME"]


# ==========================================

    
def display_custom_title():
    """Displays the custom HTML title with an image."""
    img_path = "images/cde.png"
    img_base64 = img_to_bytes(img_path)
    title_html = f"""
    <div style=" display: flex;align-items: center; gap: 15px;">
        <img src="data:image/png;base64,{img_base64}" width="65">
        <h1 style="margin: 0; padding: 0;">CDE Service Utilization Analyzer</h1>
    </div>
    """
    st.markdown(title_html, unsafe_allow_html=True)
    st.markdown("An interactive dashboard to analyze CDE resource utilization by service and node.")
    
   
  
  

def display_sidebar_inputs():
    """Displays the sidebar for data source input only."""
    st.sidebar.header("Data Source")
    
    # Using the variables defined at the top as defaults
    schema_name = st.sidebar.text_input("Schema Name", TARGET_TABLE_SCHEMA)
    table_name = st.sidebar.text_input("Table Name", TARGET_TABLE_NAME)
    
    st.sidebar.markdown("---")
    
    # --- NEW: Node Type Filter ---
    st.sidebar.header("Filters")
    node_type_filter = st.sidebar.radio(
        "Select Node Type:",
        ["All Nodes", "Compute Nodes", "Infra Nodes"]
    )
    
    st.sidebar.markdown("---")
    
    # Return the new filter value alongside the connection details
    return IMPALA_CONN_NAME, schema_name, table_name, node_type_filter

def display_raw_data_expander(df):
    """Displays the raw, filtered data in an expander."""
    with st.expander("View Raw Data"):
        st.dataframe(df, use_container_width=True)
        
def img_to_bytes(img_path):
    """Encodes a local image file to a base64 string."""
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded
