import os
import requests
import json
import time
import getpass
import pandas as pd
import cml.data_v1 as cmldata
from datetime import datetime
from urllib.parse import urlparse
from impala.dbapi import connect
from IPython.display import clear_output


# ==========================================
# CONFIGURATION - RETRIEVED FROM OS ENV
# ==========================================
required_vars = [
    "YUNIKORN_URL", 
    "GRAFANA_URL", 
    "IMPALA_CONN_NAME", 
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "WORKLOAD_USER",
    "WORKLOAD_PASSWORD"
]

# Check if all environment variables exist
for var in required_vars:
    if var not in os.environ:
        raise OSError(f"Environment variable '{var}' does not exist")

YUNIKORN_URL = os.environ["YUNIKORN_URL"]
GRAFANA_URL = os.environ["GRAFANA_URL"]
IMPALA_CONN_NAME = os.environ["IMPALA_CONN_NAME"]
TARGET_TABLE_SCHEMA = os.environ["TABLE_SCHEMA"]
TARGET_TABLE_NAME = os.environ["TABLE_NAME"]
WORKLOAD_USER = os.environ["WORKLOAD_USER"]
WORKLOAD_PASSWORD = os.environ["WORKLOAD_PASSWORD"]

# ==========================================

def get_cde_token():
    try:
        parsed_url = urlparse(GRAFANA_URL)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        token_endpoint = f"{base_url}/gateway/authtkn/knoxtoken/api/v1/token"
        
        response = requests.get(token_endpoint, auth=(WORKLOAD_USER, WORKLOAD_PASSWORD))
        response.raise_for_status()
        
        token_data = response.json()
        return token_data.get('access_token')
         
    except Exception as e:
        print(f"Error retrieving CDE token: {e}")
        return None

def get_yunikorn_nodes(cde_token):
    headers = {"Authorization": f"Bearer {cde_token}"}
    url = f"{YUNIKORN_URL}/ws/v1/partition/default/nodes"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching YuniKorn nodes: {e}")
        return None

def setup_database_tables():
    print("Checking and initializing database tables...")
    conn = cmldata.get_connection(IMPALA_CONN_NAME)
    cursor = conn.get_cursor()
    
    # 1. Main Node Table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME} (
            log_date DATE,
            log_time TIMESTAMP,
            cde_service_name STRING,
            node_id STRING,
            node_type STRING, 
            capacity_cpu DOUBLE,
            capacity_gb DOUBLE,
            available_cpu DOUBLE,
            available_memory DOUBLE,
            allocated_cpu DOUBLE,
            allocated_memory DOUBLE,
            utilized_cpu_perc DOUBLE,
            utilized_memory_perc DOUBLE
        ) STORED AS PARQUET
    """)

    # 2. Allocations Table
    alloc_table_name = f"{TARGET_TABLE_NAME}_allocations"
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE_SCHEMA}.{alloc_table_name} (
            log_date DATE,
            log_time TIMESTAMP,
            node_id STRING,
            allocation_key STRING,
            application_id STRING,
            request_time BIGINT,
            allocation_time BIGINT,
            allocation_delay BIGINT,
            priority STRING,
            originator BOOLEAN,
            placeholder_used BOOLEAN,
            task_group_name STRING,
            job_run_id STRING,
            pod_name STRING,
            allocated_cpu DOUBLE,
            allocated_memory_gb DOUBLE,
            allocated_pods INT,
            allocation_tags_json STRING
        ) STORED AS PARQUET
    """)
    
    cursor.close()
    conn.close()
    print("Tables verified successfully.")
    

def insert_data_in_db(df, alloc_df):
    conn = None
    try:
        conn = cmldata.get_connection(IMPALA_CONN_NAME)
        cursor = conn.get_cursor()
        
        # 1. Parameterized Bulk Insert for Main Node Table
        # Using %s as placeholders (standard for many Python DB-API drivers)
        insert_sql = f"""
            INSERT INTO {TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME} 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # Convert DataFrame to a list of tuples for executemany
        main_data = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(insert_sql, main_data)

        # 2. Parameterized Bulk Insert for Allocations Table
        alloc_table_name = f"{TARGET_TABLE_NAME}_allocations"
        if not alloc_df.empty:
            insert_alloc_sql = f"""
                INSERT INTO {TARGET_TABLE_SCHEMA}.{alloc_table_name} 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            alloc_data = [tuple(x) for x in alloc_df.to_numpy()]
            cursor.executemany(insert_alloc_sql, alloc_data)
            
    except Exception as e:
        print(f"Database insertion failed: {e}")
    finally:
        # Ensures the connection ALWAYS closes, even if the insert fails
        if conn:
            cursor.close()
            conn.close()
    

def extract_cde_service_name(url):
    try:
        hostname = urlparse(url).netloc
        parts = hostname.split('.')
        return parts[1] if len(parts) >= 2 else None
    except:
        return None

def process_and_upload(cde_token):
    json_data = get_yunikorn_nodes(cde_token)
    if not json_data: return
    
    cde_service_name = extract_cde_service_name(YUNIKORN_URL)
    processed_data = []
    allocations_data = [] # New list for the child table
    current_ts = datetime.now()
    
    for node in json_data:
        node_id = node.get('nodeID')
        allocations = node.get('allocations', [])
        
        # 1. Determine Node Type
        is_compute = any(
            'kubernetes.io/label/dex-job-run-id' in alloc.get('allocationTags', {})
            for alloc in allocations
        )
        node_type = 'compute' if is_compute else 'infra'

        # Resource Math
        gb_factor = 1024 ** 3
        
        # 2. Append to main node table (added 'Node Type')
        processed_data.append({
            'Log Date': current_ts.strftime("%Y-%m-%d"),
            'Log Time': current_ts.strftime("%Y-%m-%d %H:%M:%S"),
            'CDE Service Name': cde_service_name,
            'Node ID': node_id,
            'Node Type': node_type,
            'Capacity CPU': (node.get('capacity', {}).get('vcore', 0) / 1000),
            'Capacity GB': round(node.get('capacity', {}).get('memory', 0) / gb_factor, 2),
            'Available CPU': (node.get('available', {}).get('vcore', 0) / 1000),
            'Available Memory': round(node.get('available', {}).get('memory', 0) / gb_factor, 2),
            'Allocated CPU': (node.get('allocated', {}).get('vcore', 0) / 1000),
            'Allocated Memory': round(node.get('allocated', {}).get('memory', 0) / gb_factor, 2),
            'Utilized CPU': node.get('utilized', {}).get('vcore', 0),
            'Utilized Memory': node.get('utilized', {}).get('memory', 0)
        })

        # 3. If it's a compute node, extract ONLY allocations with a DEX job run ID
        if is_compute:
            for alloc in allocations:
                tags = alloc.get('allocationTags', {})
                
                # ---> NEW: Only process this allocation if it has the required tag <---
                if 'kubernetes.io/label/dex-job-run-id' in tags:
                    resources = alloc.get('resource', {})
                    
                    allocations_data.append({
                        'Log Date': current_ts.strftime("%Y-%m-%d"),
                        'Log Time': current_ts.strftime("%Y-%m-%d %H:%M:%S"),
                        'Node ID': node_id,
                        'Allocation Key': alloc.get('allocationKey', ''),
                        'Application ID': alloc.get('applicationId', ''),
                        'Request Time': alloc.get('requestTime', 0),
                        'Allocation Time': alloc.get('allocationTime', 0),
                        'Allocation Delay': alloc.get('allocationDelay', 0),
                        'Priority': alloc.get('priority', ''),
                        'Originator': alloc.get('originator', False),
                        'Placeholder Used': alloc.get('placeholderUsed', False),
                        'Task Group Name': alloc.get('taskGroupName', ''),
                        'Job Run ID': tags.get('kubernetes.io/label/dex-job-run-id', ''),
                        'Pod Name': tags.get('kubernetes.io/meta/podName', ''),
                        'Allocated CPU': resources.get('vcore', 0) / 1000,
                        'Allocated Memory GB': round(resources.get('memory', 0) / gb_factor, 4),
                        'Allocated Pods': resources.get('pods', 0),
                        'Allocation Tags JSON': json.dumps(tags)
                    })

    df = pd.DataFrame(processed_data)
    alloc_df = pd.DataFrame(allocations_data)
    
    print("--- Node Metrics ---")
    print(df.to_markdown(index=False))
    print("\n--- Allocation Metrics ---")
    if not alloc_df.empty:
        print(alloc_df.to_markdown(index=False))
        
    # Pass both dataframes to the database function
    insert_data_in_db(df, alloc_df)

def main_loop():
    print(f"Running as {WORKLOAD_USER}. Press Ctrl+C to stop.")
    
    # Initialize tables ONCE before the infinite loop starts
    setup_database_tables()

    try:
        while True:
            now = datetime.now()
            time_to_sleep = 60 - now.second
            print(f"Waiting {time_to_sleep}s for next minute...")
            time.sleep(time_to_sleep)

            token = get_cde_token()
            if token:
                try:
                    process_and_upload(token)
                except Exception as e:
                    print(f"Unexpected error during processing: {e}")
            else:
                print("Token refresh failed.")
                
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Shutting down gracefully...")
        

if __name__ == "__main__":
    main_loop()