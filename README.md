                                                     DATA LAKE PROJECT
                                                                                
# Overview:
This project provides a step-by-step guide to create a data pipeline using Azure Data Factory (ADF) for data ingestion, Azure Databricks for data cleaning and transformation.
The pipeline demonstrates handling various data formats and optimizing the data flow between services.


# Prerequisites

Ensure the following resources are ready:

Azure Subscription: An active subscription.

Azure Data Factory: A Data Factory instance.

Azure Databricks: A Databricks workspace.

Azure Synapse Analytics: A Synapse workspace with a dedicated or serverless SQL pool.

Storage Account: For staging data.

Azure CLI or Azure Portal Access: To manage resources.

Required Permissions: Permissions to create and manage resources in Azure.


# Key Features

Ingestion of data from diverse sources like Azure Blob Storage, SQL Server, and Data Lake.

Data cleaning and transformation using Azure Databricks.

Support for multiple data formats: CSV, JSON, Parquet, and Avro.

Loading of transformed data into Azure Synapse Analytics.

Automation with triggers for scheduled and event-based pipeline execution.

Logging, monitoring, and error handling for pipeline operations.



# Architecture

Pipeline Flow

Source Data: Data from sources like SQL Server, Azure Blob Storage, or Azure Data Lake and many more is ingested using Data Factory.

Transformation: Data is cleaned and transformed using Databricks notebooks.

Loading: Transformed data is written into Azure Synapse Analytics.

Automation and Monitoring: Pipelines are automated with triggers and monitored for performance and errors.


# Step-by-Step Implementation
# Step 1: Data Ingestion (Backend Storage to Raw(Bronze) Container):
Data ingestion pipeline is a crucial component of modern data architecture, enabling businesses to efficiently manage and utilize their data. 
It's the process of importing, transferring, loading, and processing data for later use or storage in a database.

# 1. Configure Azure Data Factory (ADF) for Data Copy
# 1.1.Sign in to the Azure Portal:
Navigate to the Azure Portal.

# 1.2.Create an Azure Data Factory Instance:
In the Azure Portal, search for "Data Factory" and click "Create".
Fill in the necessary details, such as:
        Resource Group: Select an existing group or create a new one.
        Name: Provide a unique name for your Data Factory instance.
        Region: Choose the region where you want to deploy.
        Click "Review + Create" and then "Create".
Open the Data Factory Studio to ingest the data.

# 1.3.Set Up Linked Services:
In the Data Factory, go to Manage > Linked Services > New.
Create a Linked Service for Backend Storage:
Choose Azure Blob Storage/Azure Data Lake/SQL Server/ HTTP or any other storge according to the Backend Storage as the data store.
Enter the storage account details of the backend team’s storage account.
Use either Account Key or SAS token for authentication.
Create a Linked Service for Your Data Lake Storage or any other storage as per the requirment:
Repeat the steps above for your own Data Lake Storage account.

# 2.Create a Data Factory Pipeline
Navigate to the Author Tab:
Click "Author" > "+" > "Pipeline".
Add a Copy Data Activity from "Move and transform":
Drag and drop the Copy Data activity onto the canvas.
# 2.1Configure the Source:
        Select Source in the Copy Data activity settings.
        Click New to add a dataset pointing to the backend storage account.
        Choose DelimitedText for CSV files or JSON for JSON file likewise choose appropriate option as per your backend storage file formate.
        Specify the path for each file.

# 2.2Configure the Sink:
        Select Sink in the Copy Data activity settings.
        Click New to add a dataset pointing to your Data Lake Storage Raw (Bronze) container.
        Specify the destination paths (e.g., raw/accounts.csv, raw/customers.csv).

# 2.3Set Up Parameters (Optional for Dynamic Configurations):
        Use parameters to define file paths, making your pipeline flexible and easily configurable.
# 2.4Debug and Publish:
        Click Debug to test the pipeline.
        If successful, click Publish All to save your pipeline.


# Note: If you would like to push N number of files into the pipeline,
  you can achieve it by the option called ”wildcard file path” in the source tab as shown in the below screenshot.

# Step 2: Databricks Activity (Incremental/Delta Processing)
# 2.1. Set Up Databricks
Incremental and delta processing in Databricks allows for the processing of data in a way that is more efficient and cost-effective than repeated batch jobs.
Create an Azure Databricks Workspace:
In the Azure Portal, search for "Azure Databricks" and click "Create".
Provide the required details and click "Review + Create", then "Create".
Once created, launch the Databricks workspace.

# 2.2 Create a Databricks Cluster:
In the Databricks workspace, go to Clusters > Create Cluster.
Configure the cluster settings (e.g., name, node types) and create

# 2.3 Create a Databricks Notebook for Incremental Processing
Set Up a Notebook:
In Databricks, click "Create" > "Notebook".
Name the notebook (e.g., Incremental_Processing).
Choose Language as PySpark.
Read Data from Raw (Bronze) Container

      storage_account_name = "practicestrgacc"
      storage_account_key = "OjW3Dt8+bA9SZR2cFS2jWYJopJRBHmHTo7Rar81b73XKDYs6WY+MW6D69Bxm63AkLUITZ4UnFNqh+AStDgcuxA=="
        
    # Configure Spark to use the storage account key
    spark.conf.set(f"fs.azure.account.key.practicestrgacc.dfs.core.windows.net","OjW3Dt8+bA9SZR2cFS2jWYJopJRBHmHTo7Rar81b73XKDYs6WY+MW6D69Bxm63AkLUITZ4UnFNqh+AStDgcuxA==")

      accounts_df = spark.read.format("csv").option("header", "true").load("abfss://raw@practicestrgacc.dfs.core.windows.net/accounts.csv")
      accounts_df.show(5)

       # 2. Handle Missing Values
       accounts_df = accounts_df.fillna({'balance': 'Unknown'})

    # 3. Remove Duplicate Rows
    accounts_df = accounts_df.dropDuplicates()
    display(accounts_df)
    
    # Remove rows with null values in important columns
      accounts_df = accounts_df.dropna(subset=["account_id", "balance"])
      
      # Remove duplicate rows
      accounts_df = accounts_df.dropDuplicates(["account_id"])
      accounts_df.show(10)

    accounts_df.write.format("csv").mode("overwrite").option("header", "true").save("abfss://processed@practicestrgacc.dfs.core.windows.net/accounts_df.csv")
    accounts_df.show(10)
      

    
# 2.4 Databricks Activity (ETL Processing)
1. Create Another Databricks Notebook for ETL
Set Up a New Notebook:
Create another notebook named ETL_Processing.
Read Data from Curated (Silver) Container:
2. Transformation Logic
Calculate Total Balance for Each Customer:
3. Save Data to Refined (Gold) Container

   



