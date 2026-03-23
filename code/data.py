# code/data.py
import streamlit as st
import pandas as pd
import cml.data_v1 as cmldata

@st.cache_data(ttl=600)
def load_data(connection_name, schema, table):
    """
    Connects to the data source, executes a query, 
    and returns the data as a pandas DataFrame.
    """
    conn = None
    try:
        conn = cmldata.get_connection(connection_name)
        # ORDER BY to guarantee the rows always stay in the exact same order
        sql_query = f"SELECT * FROM {schema}.{table} ORDER BY log_time, node_id"
        df = conn.get_pandas_dataframe(sql_query)
        return df
    except Exception as e:
        st.error(f"Failed to connect or query the data source: {e}")
        return pd.DataFrame()  # Return empty dataframe on error
    finally:
        if conn:
            conn.close()
            
@st.cache_data(ttl=600)
def load_allocations_data(connection_name, schema, alloc_table, node_id, log_date):
    """Loads allocation data for a specific node and date from the child table."""
    conn = None
    try:
        conn = cmldata.get_connection(connection_name)
        # We query by node_id and date to keep it fast
        sql_query = f"SELECT * FROM {schema}.{alloc_table} WHERE node_id = '{node_id}' AND log_date = '{log_date}'"
        df = conn.get_pandas_dataframe(sql_query)
        return df
    except Exception as e:
        st.error(f"Failed to query allocations: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

@st.cache_data
def process_data(df):
    """
    Performs initial data processing and feature engineering for the YuniKorn utilization data.
    """
    if df.empty:
        return pd.DataFrame()

    # FIX: Create the timestamp directly from the 'log_time' column, as it contains the full date and time.
    if 'log_time' in df.columns:
        df['timestamp'] = pd.to_datetime(df['log_time'], errors='coerce')
        
        df['display_time'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Define all expected numeric columns from the schema
    numeric_cols = [
        'capacity_cpu', 'capacity_gb', 'available_cpu', 'available_memory',
        'allocated_cpu', 'allocated_memory', 'utilized_cpu_perc', 'utilized_memory_perc'
    ]

    # Loop through and convert columns to numeric, coercing errors to NaN
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df
  
