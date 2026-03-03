# code/plots.py
import plotly.express as px
import pandas as pd

def create_cpu_allocation_chart(df, selected_time=None):
    if df.empty or 'timestamp' not in df.columns:
        return px.bar(title="No data available for CPU analysis.")

    df_agg = df.groupby('timestamp').agg({
        'allocated_cpu': 'sum',
        'available_cpu': 'sum',
        'capacity_cpu': 'sum',
        'node_id': 'count'
    }).reset_index()
    
    df_agg = df_agg.rename(columns={'node_id': 'node_count'})
    df_agg['total_cores'] = df_agg['capacity_cpu']
    df_agg['timestamp_str'] = df_agg['timestamp'].dt.strftime('%Y-%m-%d %H:%M')

    plot_df = df_agg.melt(
        id_vars=['timestamp_str', 'total_cores', 'node_count'],
        value_vars=['allocated_cpu', 'available_cpu'],
        var_name='Metric',
        value_name='CPU Cores'
    )
    
    plot_df['Metric'] = plot_df['Metric'].map({
        'allocated_cpu': 'Allocated CPU',
        'available_cpu': 'Available CPU'
    })

    fig = px.bar(
        plot_df,
        x='timestamp_str',
        y='CPU Cores',
        color='Metric',
        title="Total CPU Allocation & Availability (Compute Nodes)",
        labels={'timestamp_str': 'Time', 'Metric': 'Resource Type'},
        color_discrete_map={
            'Allocated CPU': '#d62728', 
            'Available CPU': '#2ca02c'
        },
        barmode='stack',
        custom_data=['total_cores', 'node_count']
    )

    # ---> NEW: Programmatically highlight the selected bar <---
    if selected_time and selected_time in plot_df['timestamp_str'].values:
        unique_times = plot_df['timestamp_str'].unique().tolist()
        selected_index = unique_times.index(selected_time)
        fig.update_traces(
            selectedpoints=[selected_index],
            unselected={'marker': {'opacity': 0.3}}, # Dims unselected bars
            selected={'marker': {'opacity': 1.0}}    # Keeps selected bar bright
        )

    fig.update_traces(
        hovertemplate=(
            '<b>%{fullData.name}</b><br>' +
            'Time: %{x}<br>' +
            'Cluster Capacity Cores: %{customdata[0]:.2f}<br>' + 
            '%{fullData.name}: %{y:.2f}<br>' + 
            'Total K8s Nodes: %{customdata[1]}<br>' + 
            '<extra></extra>'
        ),
        marker_line_width=1,
        marker_line_color="white"
    )

    fig.update_layout(
        bargap=0, 
        xaxis=dict(
            type='category',
            title='Time',
            tickangle=-90,
            automargin=True,
            tickmode='array',
            tickvals=plot_df['timestamp_str'].unique(),
            tickformat='%Y-%m-%d %H:%M' 
        ),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig
  
def create_memory_allocation_chart(df, selected_time=None):
    if df.empty or 'timestamp' not in df.columns:
        return px.bar(title="No data available for Memory analysis.")

    df_agg = df.groupby('timestamp').agg({
        'allocated_memory': 'sum',
        'available_memory': 'sum',
        'capacity_gb': 'sum',
        'node_id': 'count'
    }).reset_index()
    
    df_agg = df_agg.rename(columns={'node_id': 'node_count'})
    df_agg['total_memory'] = df_agg['capacity_gb']
    df_agg['timestamp_str'] = df_agg['timestamp'].dt.strftime('%Y-%m-%d %H:%M')

    plot_df = df_agg.melt(
        id_vars=['timestamp_str', 'total_memory', 'node_count'],
        value_vars=['allocated_memory', 'available_memory'],
        var_name='Metric',
        value_name='Memory (GB)'
    )
    
    plot_df['Metric'] = plot_df['Metric'].map({
        'allocated_memory': 'Allocated Memory (GB)',
        'available_memory': 'Available Memory (GB)'
    })

    fig = px.bar(
        plot_df,
        x='timestamp_str',
        y='Memory (GB)',
        color='Metric',
        title="Total Memory Allocation & Availability (Compute Nodes)",
        labels={'timestamp_str': 'Time', 'Metric': 'Resource Type'},
        color_discrete_map={
            'Allocated Memory (GB)': '#d62728',
            'Available Memory (GB)': '#2ca02c'
        },
        barmode='stack',
        custom_data=['total_memory', 'node_count']
    )

    # ---> NEW: Programmatically highlight the selected bar <---
    if selected_time and selected_time in plot_df['timestamp_str'].values:
        unique_times = plot_df['timestamp_str'].unique().tolist()
        selected_index = unique_times.index(selected_time)
        fig.update_traces(
            selectedpoints=[selected_index],
            unselected={'marker': {'opacity': 0.3}}, # Dims unselected bars
            selected={'marker': {'opacity': 1.0}}    # Keeps selected bar bright
        )

    fig.update_traces(
        hovertemplate=(
            '<b>%{fullData.name}</b><br>' +
            'Time: %{x}<br>' +
            'Cluster Capacity (GB): %{customdata[0]:.2f}<br>' +
            '%{fullData.name}: %{y:.2f}<br>' + 
            'Total K8s Nodes: %{customdata[1]}<br>' + 
            '<extra></extra>'
        ),
        marker_line_width=1,
        marker_line_color="white"
    )

    fig.update_layout(
        bargap=0, 
        xaxis=dict(
            type='category',
            title='Time',
            tickangle=-90,
            automargin=True,
            tickmode='array',
            tickvals=plot_df['timestamp_str'].unique(),
            tickformat='%Y-%m-%d %H:%M' 
        ),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig