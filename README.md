# ![](/images/title.png)

Occasionally it might be useful to analyze your CDE Kubernetes Service to observe how many K8s nodes are autoscaling and what the resourse utilization rates of the nodes are. This can give you insights into how effeciently your Spark workloads are using the K8s infrastructure, whether your K8s machine type is a good fit or your Spark application executor core and memory settings, whether you need to tune these settings and so on.

It is not recommended to continually run the extract process. Only use it when you want to get a window view of utilization over a specific time frame.

This repository contains a two-part solution for monitoring Cloudera Data Engineering (CDE) resources:
- **Data Extraction Engine**: A Python script that polls the YuniKorn scheduler API to track node-level CPU and Memory utilization.
- **Interactive Dashboard**: A Streamlit-based web application to visualize cluster trends and perform drill-downs into node performance at each point in time. Filter on Infrastructure or Compute nodes, and drill down further into compute nodes to see exactly what jobs are running on them and what resources they're using.


![alt text](/images/yunikorn1.png)

![alt text](/images/yunikorn2.png)

---

## Project Structure

The project is organized into a modular structure to separate concerns, making it easy to maintain and extend.

```
yunikorn-analyzer-app/
├── 🗂️ code/
│   ├── app.py              # The entry point for the Streamlit dashboard
│   ├── data.py             # Handles data retrieval from Impala
│   ├── plots.py            # Plotly charts for CPU and Memory allocation
│   ├── ui.py               # Dashboard layout, sidebar configuration, and custom HTML
├── 🗂️ yunikorn_analytics_extract/
│   └── yunikorn_extract_analytics.py   # Yunikorn core extraction script
├── 🗂️ images/
│   └── x.png               # Image assets
├── app_deploy.py           # Streamlit deploy file for CAI Application
├── README.md               # This file
└── requirements.txt        # Project dependencies
```

---

🚀 Features
1. Automated Data Collection
- **API Integration**: Connects to the CDE YuniKorn API to get real-time node capacity, availability, and allocation.
- **Time-Series Logging**: Executes on a minute-by-minute loop, ensuring high-resolution data for troubleshooting.
- **Persistent Storage**: Automatically creates and populates a Parquet-backed Impala table (richardvh.cde_yunikorn_tracking_v2).

2. Interactive Visualization
- **Resource Allocation Charts**: Stacked bar charts showing "Allocated" vs. "Available" CPU and Memory across the cluster.
- **Minute-by-Minute Drill-Down**: Users can click on any bar in the chart to see a detailed table of every node active during that specific minute, including progress bars for CPU and Memory utilization.
- **Historical Analysis**: A date selector allows users to browse through past logs.

---

🛠️ Setup and Installation

Follow these steps to get the application running on your local machine.

### Prerequisites

-   Cloudera AI project
-   Python 3.8+
-   Access to the Impala data source where the table metrics are stored.
-   Run the yunikorn_extract_analytics.py process first. The target table that the data is written to by this process is used as the source table by this Streamlit application below. 


### Installation Steps

1.  Create a new project in Cloudera AI. Give your Project a name and and choose Git for Initial Setup. Supply this Git Repo URL and choose a PBJ Workbench of Python 3.8 or greater:

    ![alt text](/images/readme_image1.png)

2.  Start a Python session and install the Python packages in the requirements.txt file:

    ![alt text](/images/readme_image2.png)

3. Set the required project enviornment variables in your CAI project sesstings:

 - **Required**
 
      See below on how to retrieve the YUNIKORN_URL, GRAFANA_URL and IMPALA_CONN_NAME 
 
    - YUNIKORN_URL: e.g. `https://yunikorn.cde-s9abcde.dl01-dem.ylcu-atmi.cloudera.site`
    - GRAFANA_URL: e.g. `https://service.cde-s9abcde.dl01-dem.ylcu-atmi.cloudera.site/grafana/d/0Oq0WmQWk/instance-metrics?orgId=1&refresh=5s&var-virtual_cluster_name=spark-351`
    - IMPALA_CONN_NAME: e.g. `default-impala-aws`
    - TABLE_SCHEMA: e.g. `rvanheerden`
    - TABLE_NAME: e.g. `yunikorn_utilisation`
    - WORKLOAD_USER: e.g. `rvanheerden`
    - WORKLOAD_PASSWORD: e.g. `myworkloadpassword`
    
   
   ![alt text](/images/readme_image3.png)
  
   - **YUNIKORN_URL**
   
   Copy the Resource Scheduler URL from you CDE Service
   
   ![alt text](/images/readme_image4.png)
   
   - **GRAFANA_URL**
   
   Copy the Grafana Charts URL from you CDE Service Virtual Cluster
   
   ![alt text](/images/readme_image5.png)
   
   - **IMPALA_CONN_NAME**
   
   Copy the desired Impala Connection name from your CAI Workbench Connections
   
   ![alt text](/images/readme_image6.png)
   
---

📊 How to Run the Yunikorn Analytics Extract Process

In your CAI Project start a new Python Session and run the `cde-yunikorn-extract.py` file. You can also schedule this as a job to run as a background process.

![alt text](/images/readme_image9.png)

---

📊 How to Run the Streamlit Web Application

In your CAI project select Applications in the menu and Create New Application. Give your application a name, subdomain and select the provided app_deploy.py file. Choose your your PBJ Workbench Python version, select resources required and create the application:

![alt text](/images/readme_image7.png)


Once the application is running click on it to open it in your browser.

![alt text](/images/readme_image8.png)

---



