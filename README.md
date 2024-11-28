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

      https://www.google.com/url?sa=i&url=https%3A%2F%2Flearn.microsoft.com%2Fen-us%2Fazure%2Farchitecture%2Fsolution-ideas%2Farticles%2Fazure-databricks-modern-analytics-            architecture&psig=AOvVaw0PDJViS7nEHL3jqgskCXP9&ust=1732839710633000&source=images&cd=vfe&opi=89978449&ved=0CBEQjRxqFwoTCLi2yI_h_YkDFQAAAAAdAAAAABAE

# Step-by-Step Implementation
# Step 1: Data Ingestion (Backend Storage to Raw(Bronze) Container):
Data ingestion pipeline is a crucial component of modern data architecture, enabling businesses to efficiently manage and utilize their data. 
It's the process of importing, transferring, loading, and processing data for later use or storage in a database.

       https://github.com/user-attachments/assets/c35286b0-2e2f-40f4-8ebf-e9dfd3c85fe5

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


# Note:
   If you would like to push N number of files into the pipeline,
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

# Sample code for cleaning
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
      
# NOTE:
  This part of the ETL contains only the Cleaning part where the data move from Raw(bronze) container to Processed(silver) container.
    
# 2.4 Databricks Activity (ETL Processing)
1. Create Another Databricks Notebook for ETL
Set Up a New Notebook:
Create another notebook named ETL_Processing.
Read Data from Curated (Silver) Container:
2. Transformation Logic
Calculate Total Balance for Each Customer:
3. Save Data to Refined (Gold) Container
# Sample code for Transformations: 

    storage_account_name = "strgacc2831"
    storage_account_key = "nK93TB+8Q47M4LiV45af/QN9W01RMS1NJYTe3ITBfE/QetSi7jp2jAtmC/DsNenMGt5aNqGjDBxR+AStA8o5SA=="
    
    # Configure Spark to use the storage account key
    spark.conf.set(f"fs.azure.account.key.strgacc2831.dfs.core.windows.net","nK93TB+8Q47M4LiV45af/QN9W01RMS1NJYTe3ITBfE/QetSi7jp2jAtmC/DsNenMGt5aNqGjDBxR+AStA8o5SA==")
    
    # Set up configuration for accessing ADLS with a SAS token
    spark.conf.set(
        "fs.azure.sas.silver.strgacc2831.dfs.core.windows.net",
        "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-11-08T09:12:09Z&st=2024-11-08T01:12:09Z&spr=https&sig=93v7nCVWLv3xH6v%2Fx5KvVAloWG83JJNJ5vBX9dcA%2Bjw%3D"
    )


    # Assuming both files are in CSV format
        accounts_df = spark.read.format("csv").option("header", "true").load("abfss://silver@strgacc2831.dfs.core.windows.net/cleaned_accounts.csv/part-00000-tid-1480919646493366281-e08f047d-1733-4059-8af7-      4a1ba5f7a417-42-1-c000.csv")
        customers_df = spark.read.format("csv").option("header", "true").load("abfss://silver@strgacc2831.dfs.core.windows.net/customers.csv")
        
        accounts_df.show(5)
        customers_df.show(5)


      from pyspark.sql import functions as F
      
      # Convert balance column to float if necessary
      accounts_df = accounts_df.withColumn("balance", F.col("balance").cast("float"))
      
      # Join customers and accounts DataFrames on customer_id and select specific columns to avoid ambiguity
      joined_df = customers_df.alias("cust").join(accounts_df.alias("acct"), F.col("cust.customer_id") == F.col("acct.customer_id"), "inner") \
          .select(
              F.col("cust.customer_id").alias("customer_id"),
              "first_name",
              "last_name",
              "address",
              "city",
              "state",
              "zip",
              "account_type",
              "balance"
          )
      
      # Aggregate to calculate the total balance for each customer
      result_df = joined_df.groupBy(
          "customer_id",
          "first_name",
          "last_name",
          "address",
          "city",
          "state",
          "zip",
          "account_type"
      ).agg(
          F.sum("balance").alias("total_balance")
      )
      # Display the result
      result_df.show()


    # Write the sorted DataFrame to Azure Data Lake Storage in CSV format
    result_df.write.format("csv").mode("overwrite").option("header", "true").save("abfss://silver@strgacc2831.dfs.core.windows.net/total_balance_per_customer.csv")


    # Assuming `total_balance_df` is the DataFrame with the aggregated total balances
      gold_container_path = "abfss://gold@strgacc2831.dfs.core.windows.net/total_balance_per_customer.csv"
      
      result_df.write.format("csv").mode("overwrite").option("header", "true").save("abfss://gold@strgacc2831.dfs.core.windows.net/total_balance_per_customer.csv")
      
      result_df.show()

    
   # NOTE:
  This part of the ETL contains Transformations and everything according to the business requirement  where the data move from Processed(silver) container to Meta(gold) container.




# Step 4: Azure Synapse Analytics

# Create External Tables in Synapse:
Connect to Synapse Studio and create a  SQL Database in ‘Data’ tab 
Configure a dedicated SQL pool or use the serverless SQL pool.

    https://github.com/user-attachments/assets/f214ab5a-2326-4549-a153-b1cf52c6871d


Define External Data Sources for Silver container by selecting appropriate database in ‘use database’
 Create External Data Sources for both the containers.
 Create External File format for the containers.

Verify whether the data sources, File formate that are created for both the containers in the  Data>workspace.

Create External Tables in Synapse for both the curated and refined data. Choose the container in Data>linked for which you wanted to create an external table.

   Name the external table name a unique table name followed by schema dbo(default schema name).
   
   This is how we can create an External table with an Automatic code generator. 
   
   This allows data analysts and business intelligence teams to access and query the data directly using tools like Synapse Studio or notebooks.

            ![Screenshot 2024-11-11 175516](https://github.com/user-attachments/assets/9bd029de-accb-456c-95fd-e0ae04b6323a)
    ![Screenshot 2024-11-11 175217](https://github.com/user-attachments/assets/9c469ef8-ea7d-4d16-9113-6dbf5fe0b455)
    ![Screenshot 2024-11-11 174839](https://github.com/user-attachments/assets/af6ada9c-e140-4791-8377-9c0cf26cbb7e)
    ![Screenshot 2024-11-11 174811](https://github.com/user-attachments/assets/a13cac0a-d5dd-423a-9133-aa56ef229466)
    ![Screenshot 2024-11-11 174517](https://github.com/user-attachments/assets/ef3b43af-d485-4734-b098-4db611661d87)
    ![Screenshot 2024-11-11 174342](https://github.com/user-attachments/assets/661ad3bf-0827-43c2-8ac0-1776086fd6e3)
    ![Screenshot 2024-11-11 173947](https://github.com/user-attachments/assets/04031db0-18e0-4ca5-87b5-7af16c46ba77)
    ![Screenshot 2024-11-11 170651](https://github.com/user-attachments/assets/b8081de4-77c3-4bba-b119-a217ac148687)


# Configure Triggers for Automation

Schedule Trigger

Set up a trigger to run the pipeline at a specific time interval.

Define start time, recurrence, and end time.

Event Trigger

Set up an event-based trigger to run the pipeline when new files are uploaded to Blob Storage.

# Validate and Run the Pipeline

Debug the Pipeline

Use the "Debug" option in ADF to test the pipeline with sample data.

Publish and Execute

Publish the pipeline and run it manually or via triggers.

Monitor Pipeline Execution

Use the "Monitor" tab in ADF to track execution progress and logs.

# Monitor and Optimize

Logging and Alerts:

Set up alerts for failures and long-running pipelines.

Implement error tracking and retry mechanisms in ADF and Databricks.

Performance Tuning:

Optimize transformations in Databricks and Synapse.

Use partitioning and parallelism for large datasets.

Troubleshooting;:

Use Azure Databricks logs and Spark UI for debugging.

Review pipeline activity logs in Azure Data Factory Monitor.


# Best Practices:

Use staging tables in Synapse for intermediate data.

Secure linked services with Managed Identity wherever possible.

Monitor and analyze logs regularly.

Optimize Databricks clusters by selecting appropriate VM sizes and autoscaling options.

Use incremental data loads to optimize performance.



=======================================================================



# How to create Azure Key Vault:
			
# Create Azure Key-Vault

Select resource group: “practice2831”
Key vault name: “mykeyvault1”
Region: “Canada Central”
Access Configuration:
Permission model - select : Vault access policy
Access policies - check the box “Name”
Access policies - select “create”
In permissions : under “Secret permissions” check the box “select all”
In principle: Type the name of the Azure Data Factory: “adfpractice”
Applications (No change)
Review + Create - Create
Select : objects - Secrets
+ Generate/Import 
Secret Name: “mysecret”
Secret value: (act as password)
Create
==========================================================

# How to create Service principal:
		
A Service Principal in Azure is an identity used by applications, automated processes, or virtual machines to access Azure resources securely. It's essentially an identity with specific permissions, enabling secure access without requiring a user login. Service principals are commonly used for applications to authenticate and perform operations in Azure without user intervention.

# Key Points about Service Principals:
Role-Based Access Control (RBAC): A service principal can be granted specific permissions through Azure RBAC, allowing it to access only the necessary resources and perform certain actions.
Authentication: Service principals can authenticate using a client secret (a password) or a certificate.
Azure Active Directory (AAD): Service principals are managed within Azure Active Directory.

# Steps to Create a Service Principal
You can create a service principal using the Azure Portal, Azure CLI, or Azure PowerShell. Here’s how to do it with each method:
Method 1: Using Azure Portal
Go to Azure Active Directory:
In the Azure portal, navigate to Azure Active Directory.
Register a New Application:
Under Manage, select App registrations.
Click on New registration.
Enter a name for the application (e.g., MyAppServicePrincipal).
For Supported account types, select the relevant option based on your requirements (usually "Accounts in this organizational directory only").
Click Register.
Create a Client Secret or Certificate:
After the app is created, go to Certificates & secrets.
Under Client secrets, click + New client secret.
Provide a description and select an expiration period for the client secret.
Click Add and copy the Value (client secret), as you’ll need it later. Note that this value won’t be shown again.
Note Down Application (Client) ID and Directory (Tenant) ID:
In the app’s Overview section, copy the Application (client) ID and Directory (tenant) ID. You’ll need these values for authentication.
Assign RBAC Permissions:
Navigate to the resource (e.g., Key Vault, Storage Account) that the service principal needs to access.
Go to Access Control (IAM) and click on Add role assignment.
Select the appropriate role (e.g., Contributor, Reader, or Key Vault Secrets Officer).
Under Assign access to, choose User, group, or service principal.
Search for the name of your newly created app registration (the service principal), select it, and click Save.

# Note: Service principal id and object id are same, you can find it in the overview page.
Role- Storage blob data reader role is created to read the blob data in azure data lake storage gen 2 




      
