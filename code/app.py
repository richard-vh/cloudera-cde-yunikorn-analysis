# code/app.py
import streamlit as st
import data
import ui
import plots
import pandas as pd
import datetime

# --- Page Configuration ---
st.set_page_config(
    page_title="Cloudera Data Engineering Service Utilization Analyzer",
    page_icon="⚙️",
    layout="wide"
)

# --- Get the inputs from the UI ---
connection_name, schema_name, table_name, node_type_filter = ui.display_sidebar_inputs()

# --- Data Loading and Processing ---
raw_df = data.load_data(connection_name, schema_name, table_name)
processed_df = data.process_data(raw_df)

if processed_df.empty:
    st.warning(f"The table {schema_name}.{table_name} is empty or doesn't exist.")
    st.stop()

# 1. Save the complete timeline before filtering
all_timestamps = processed_df[['timestamp', 'display_time', 'log_time']].drop_duplicates()

# 2. Apply Node Type Filter
if node_type_filter == "Compute Nodes":
    processed_df = processed_df[processed_df['node_type'] == 'compute']
elif node_type_filter == "Infra Nodes":
    processed_df = processed_df[processed_df['node_type'] == 'infra']

# 3. Re-merge the timeline so we don't lose the timestamps (creates the 'empty bars')
processed_df = pd.merge(all_timestamps, processed_df, on=['timestamp', 'display_time', 'log_time'], how='left')

# 4. Fill numeric gaps with 0 so they plot correctly
numeric_cols = [
    'capacity_cpu', 'capacity_gb', 'available_cpu', 'available_memory',
    'allocated_cpu', 'allocated_memory', 'utilized_cpu_perc', 'utilized_memory_perc'
]
processed_df[numeric_cols] = processed_df[numeric_cols].fillna(0)

# --- Main Page Rendering ---
ui.display_custom_title()

if processed_df.empty:
    st.warning("No data available for the selected filters. Please check the data source settings.")
    st.stop()

# --- Visualizations in Tabs ---
tab1, tab2 = st.tabs(["Utilization Over Time", "Raw Data"])

with tab1:
    st.subheader("Average Resource Allocation")
    
    if not processed_df.empty and 'timestamp' in processed_df.columns:
        min_date = processed_df['timestamp'].min().date()
        max_date = processed_df['timestamp'].max().date()

        selected_date = st.date_input(
            "Select a Date to Display:",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="utilization_date_selector"
        )

        if selected_date:
            start_datetime = pd.to_datetime(selected_date)
            end_datetime = start_datetime + datetime.timedelta(days=1)
            
            date_filtered_df = processed_df[
                (processed_df['timestamp'] >= start_datetime) & (processed_df['timestamp'] < end_datetime)
            ].copy() 
        else:
            date_filtered_df = pd.DataFrame()
    else:
        date_filtered_df = pd.DataFrame()

    # --- SYNCHRONIZED SELECTION LOGIC ---
    if 'active_selection' not in st.session_state:
        st.session_state.active_selection = None
    if 'last_table_sel' not in st.session_state:
        st.session_state.last_table_sel = []

    # Pull current widget states safely
    cpu_state = st.session_state.get('cpu_chart', {}).get('selection', {}).get('points', [])
    mem_state = st.session_state.get('mem_chart', {}).get('selection', {}).get('points', [])
    table_state = st.session_state.get('node_table', {}).get('selection', {}).get('rows', [])

    curr_cpu = cpu_state[0]['x'] if cpu_state else None
    curr_mem = mem_state[0]['x'] if mem_state else None
    
    # Detect if the dataframe was the widget that triggered the rerun
    table_changed = table_state != st.session_state.last_table_sel
    st.session_state.last_table_sel = table_state

    # Logic: If the table was interacted with, preserve the current chart selection.
    # Otherwise, update the selection based on the charts.
    if not table_changed:
        if curr_cpu:
            st.session_state.active_selection = curr_cpu
        elif curr_mem:
            st.session_state.active_selection = curr_mem
        else:
            st.session_state.active_selection = None

    selected_time = st.session_state.active_selection

    # --- CPU CHART WITH SELECTION ---
    # Pass selected_time to the plot generator so it can highlight the bar
    fig_cpu = plots.create_cpu_allocation_chart(date_filtered_df, selected_time)
    st.plotly_chart(
        fig_cpu, 
        use_container_width=True, 
        on_select="rerun", 
        selection_mode="points",
        key="cpu_chart"  
    )

    # --- MEMORY CHART WITH SELECTION ---
    # Pass selected_time to the plot generator so it can highlight the bar
    fig_memory = plots.create_memory_allocation_chart(date_filtered_df, selected_time)
    st.plotly_chart(
        fig_memory, 
        use_container_width=True, 
        on_select="rerun", 
        selection_mode="points",
        key="mem_chart"  
    )
    
    # --- DRILL-DOWN TABLE LOGIC ---
    if selected_time:
        st.divider()
        st.markdown(f"#### 🔍 Detailed Node Breakdown for `{selected_time}`. Select compute nodes for CDE workload details")
        
        drill_down_df = date_filtered_df[
            (date_filtered_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M') == selected_time) &
            (date_filtered_df['node_id'].notna())
        ].copy()

        if drill_down_df.empty:
            st.info(f"No {node_type_filter.lower()} were active at {selected_time}.")
        else:
            drill_down_df.insert(0, 'node_#', range(1, len(drill_down_df) + 1))

            for col in ['utilized_cpu_perc', 'utilized_memory_perc']:
                drill_down_df[col] = pd.to_numeric(
                    drill_down_df[col].astype(str).str.replace('%', ''), errors='coerce'
                ).fillna(0) / 100

            event = st.dataframe(
                drill_down_df,
                column_config={
                    "node_#": st.column_config.NumberColumn("node_#", help="Sequential row number"),
                    "utilized_cpu_perc": st.column_config.ProgressColumn(
                        "CPU Utilization", format="%.2f%%", min_value=0, max_value=1,
                    ),
                    "utilized_memory_perc": st.column_config.ProgressColumn(
                        "Memory Utilization", format="%.2f%%", min_value=0, max_value=1,
                    ),
                    "timestamp": None,
                    "display_time": None,
                    "log_time": None
                },
                use_container_width=True,
                hide_index=True,
                on_select="rerun",           
                selection_mode="single-row",
                key="node_table"
            )
            
            if event and len(event.selection.rows) > 0:
                selected_row_idx = event.selection.rows[0]
                selected_node_id = drill_down_df.iloc[selected_row_idx]['node_id']
                selected_node_type = drill_down_df.iloc[selected_row_idx]['node_type']
                
                if selected_node_type != 'compute':
                    st.info(f"💡 Node `{selected_node_id}` is an Infrastructure node. Pod and Job Run details are only tracked for Compute nodes.")
                else:
                    st.markdown(f"#### 📦 Active Compute Workload Pods on Node: `{selected_node_id}`")
                    
                    selected_date_str = selected_time.split(' ')[0]
                    alloc_table_name = f"{table_name}_allocations"
                    
                    alloc_df = data.load_allocations_data(
                        connection_name, schema_name, alloc_table_name, selected_node_id, selected_date_str
                    )
                    
                    if not alloc_df.empty:
                        alloc_df['timestamp'] = pd.to_datetime(alloc_df['log_time'], errors='coerce')
                        minute_alloc_df = alloc_df[alloc_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M') == selected_time]
                        
                        if not minute_alloc_df.empty:
                            display_cols = ['job_run_id', 'application_id', 'allocated_cpu', 'allocated_memory_gb', 'task_group_name', 'pod_name']
                            st.dataframe(minute_alloc_df[display_cols], use_container_width=True, hide_index=True)
                        else:
                            st.info("No compute allocations active on this node for this exact minute.")
                    else:
                        st.info("No allocations found for this node.")
            
    else:
        st.info("💡 **Tip:** Click on any bar in the charts above to see the specific nodes active at that timestamp.")

with tab2:
    st.subheader("Raw Log Data")
    ui.display_raw_data_expander(processed_df)