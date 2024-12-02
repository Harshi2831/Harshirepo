                                                     DATA LAKEHOUSE PROJECT
                                                                                
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

	https://github.com/user-attachments/assets/538f98e1-fbb4-4f0d-8fa6-0358b297b0e6


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

You can choose HTTP Linked service to work. 
An HTTP linked service in Azure Data Factory is used to connect to external REST APIs or web services for integration into your data pipelines. It is helpful in scenarios where you need to pull data from external APIs, send data to external endpoints, or integrate with third-party services.

# Reasons to Use an HTTP Linked Service:
# External API Data Ingestion:

Retrieve data from REST APIs (e.g., weather data, stock prices, IoT data).
Useful for integrating with public or private web APIs.
Integration with Webhooks:

Trigger workflows or send notifications via webhooks.
Custom API Integration:

Fetch data or push processed data to custom endpoints provided by third-party applications.
Flexible Connectivity:

Supports both HTTP and HTTPS protocols for secure and flexible data exchange.
Parameterization:

Use dynamic parameters to pass API keys, URLs, or query parameters for customizable calls.
How It Works in Pipelines
Source: You can use the HTTP linked service as a source to pull data from an API and load it into a data lake or database.
Sink: You can use it to send processed data or results from your pipeline to an external service.
Steps to Create an HTTP Linked Service in ADF
Go to Manage in Data Factory:

Open the Manage section in Azure Data Factory UI.
Create a New Linked Service:

Select New Linked Service and choose HTTP.
Configure the HTTP Linked Service:

Base URL: Enter the base URL of the API endpoint (e.g., https://api.example.com).
Authentication: Choose the appropriate authentication type:
Anonymous: For publicly available APIs.
Basic: Requires a username and password.
Managed Identity: Securely authenticate using Azure AD Managed Identity.
API Key: Pass the API key in the header or query parameters.
Test the Connection:

Test the linked service to ensure successful connectivity.
Use in a Pipeline:

Add a Copy Data activity and set the HTTP linked service as the source.
Specify additional parameters, such as request headers or query strings.
Example Configuration
Here’s an example JSON for an HTTP linked service:

		json code
		
		{
		  "name": "HttpLinkedService",
		  "type": "Microsoft.DataFactory/factories/linkedservices",
		  "properties": {
		    "type": "Http",
		    "typeProperties": {
		      "url": "https://api.example.com/data",
		      "authenticationType": "Basic",
		      "userName": "your-username",
		      "password": {
		        "type": "SecureString",
		        "value": "your-password"
		      }
		    }
		  }
		}
# Use Cases in Your Pipeline:
Fetching Metadata: Query metadata APIs to dynamically populate pipeline parameters.
Data Extraction: Pull data from an external service, process it in Databricks, and load it into Synapse.
Custom Notifications: Send custom notifications or trigger external workflows via HTTP POST requests.


# 2.Create a Data Factory Pipeline
Navigate to the Author Tab:
Click "Author" > "+" > "Pipeline".
Add a Copy Data Activity from "Move and transform":
Drag and drop the Copy Data activity onto the canvas.
# 2.1 Configure the Source:
Select Source in the Copy Data activity settings.
Click New to add a dataset pointing to the backend storage account.
Choose DelimitedText for CSV files or JSON for JSON file likewise choose appropriate option as per your backend storage file formate.
Specify the path for each file.

# 2.2 Configure the Sink:
Select Sink in the Copy Data activity settings.
Click New to add a dataset pointing to your Data Lake Storage Raw (Bronze) container.
Specify the destination paths (e.g., raw/accounts.csv, raw/customers.csv).

  
	


# 2.3 Set Up Parameters (Optional for Dynamic Configurations):
Use parameters to define file paths, making your pipeline flexible and easily configurable.
After adding source and sink linked services,add parameters for both of them as shown in the screenshot below.


Go to “Additional Headers” and choose Dynamic content to add @pipeline().parameters.source by choosing source parameter and test the connection. Repeat the same process in sink for sink parameters 



Debug and Publish:
Click Debug to test the pipeline.
If successful, click Publish All to save your pipeline.

Now add Trigger for the pipeline.



# 2.4 Debug and Publish:
Click Debug to test the pipeline.
If successful, click Publish All to save your pipeline.
	

# Note:
   If you would like to push N number of files into the pipeline,
  you can achieve it by the option called ”wildcard file path” in the source tab as shown in the below screenshot.
  
   	        ![Screenshot 2024-11-27 224150](https://github.com/user-attachments/assets/c75a83fa-ee83-4fd6-b961-205576d3272d)
		![Screenshot 2024-11-27 224123](https://github.com/user-attachments/assets/b6e5d3a6-63ba-4740-902a-5b1a7b7b294e)
		![Screenshot 2024-11-27 224009](https://github.com/user-attachments/assets/4ce5f306-b992-4b8a-b495-e3e6f60b4d63)
		![Screenshot 2024-11-27 223918](https://github.com/user-attachments/assets/d1eec42a-ad60-45e8-8af4-a529cfd2e159)

  

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
  
	![Screenshot 2024-11-27 224704](https://github.com/user-attachments/assets/c8f94a12-98a4-44ef-aacd-122341fafb33)

    
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

    accounts_df.write.format("parquet").mode("append").save("abfss://processed@practicestrgacc.dfs.core.windows.net/accounts_df")
    accounts_df.show(10)

screenshots:
      
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


    # Write the sorted DataFrame to Azure Data Lake Storage 
    result_df.write.format("parquet").mode("overwrite").save("abfss://silver@strgacc2831.dfs.core.windows.net/total_balance_per_customer")


    # Assuming `total_balance_df` is the DataFrame with the aggregated total balances
      gold_container_path = "abfss://gold@strgacc2831.dfs.core.windows.net/total_balance_per_customer"
      
      result_df.write.format("parquet").mode("overwrite").save("abfss://gold@strgacc2831.dfs.core.windows.net/total_balance_per_customer")
      
      result_df.show()

 	
		![Screenshot 2024-11-11 165438](https://github.com/user-attachments/assets/10ab686d-deb0-4394-90ce-dc94d0be9979)
		![Screenshot 2024-11-11 165130](https://github.com/user-attachments/assets/f202a476-5635-4a07-862f-1b3e1f906222)
		![Screenshot 2024-11-11 165035](https://github.com/user-attachments/assets/4e7ccbe3-b108-44bb-a033-921d6d8d5971)
		![Screenshot 2024-11-11 164954](https://github.com/user-attachments/assets/fcc93547-5f39-46e7-a4f7-671cdeddc72c)

    
   # NOTE:
  This part of the ETL contains Transformations and everything according to the business requirement  where the data move from Processed(silver) container to Meta(gold) container.




# Step 4: Azure Synapse Analytics:
 Azure Synapse Analytics is a cloud-based analytics service from Microsoft that combines data integration, big data analytics, and enterprise data warehousing

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


    ==============================================================================================


# Configure Triggers for Automation

Schedule Trigger:

Set up a trigger to run the pipeline at a specific time interval.

Define start time, recurrence, and end time.

Event Trigger:

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

Troubleshooting:

Use Azure Databricks logs and Spark UI for debugging.

Review pipeline activity logs in Azure Data Factory Monitor.


# Best Practices:

Use staging tables in Synapse for intermediate data.

Secure linked services with Managed Identity wherever possible.

Monitor and analyze logs regularly.

Optimize Databricks clusters by selecting appropriate VM sizes and autoscaling options.

Use incremental data loads to optimize performance.

# Common Troubleshooting Issues:

# Authentication Failures

Issue: Failing to connect to external services due to authentication issues.

Solution: Verify linked service configurations, especially credentials (e.g., Azure Key Vault secrets, Managed Identity permissions).

# Data Movement Errors

Issue: Data fails to load between stages (e.g., Blob to Databricks or Databricks to Synapse).

Solution: Ensure proper dataset configurations, file paths, and sufficient permissions on storage accounts.

# Pipeline Failures

Issue: Pipeline fails during execution.

Solution: Check the "Monitor" tab for error messages and debug using activity logs.

# Slow Performance

Issue: Pipelines take longer than expected to complete.

Solution: Optimize data partitions, use parallel processing, and adjust Databricks cluster configurations.

# Trigger Misconfigurations

Issue: Scheduled or event-based triggers not firing.

Solution: Verify trigger configurations and ensure "Publish All" is completed.

# Schema Mismatches

Issue: Data format or schema does not match between source and sink.

Solution: Validate schemas during data transformations and ensure compatibility between source and destination.

# Databricks Cluster Issues

Issue: Databricks notebook activity fails due to cluster unavailability.

Solution: Ensure the cluster is running and properly configured with sufficient resources.

# Permission Denied Errors

Issue: Access denied errors when interacting with storage or databases.

Solution: Verify permissions on Azure Storage, Synapse, and other connected resources.

# File Format Errors

Issue: Incorrect file format or delimiter errors.

Solution: Validate the file format and configure the correct file format settings in linked services or external tables.





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
  
			![Screenshot 2024-11-29 122025](https://github.com/user-attachments/assets/07874e08-a7d9-4dc6-a8d1-796c350f9fbd)
		
		  	![Screenshot 2024-11-29 115752](https://github.com/user-attachments/assets/0392e8ac-7ab6-4baf-a8c1-699ebdf163bc)

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

		![Screenshot 2024-11-14 143326](https://github.com/user-attachments/assets/e712eaf0-46e8-4fb7-a436-306fd527935b)


# Note:
Service principal id and object id are same, you can find it in the overview page of it.



==================================================================================
  
 # Creating Triggers for a Pipline in Data Factory.
To create a trigger for your pipeline in Azure Data Factory, follow these steps:

# Steps to Create Triggers
# 1. Create a Schedule Trigger
This type of trigger runs the pipeline at a specific interval.

Go to the Azure Data Factory UI:

Open the Author section.
Add a New Trigger:

Click on the Add Trigger button in the toolbar and select New/Edit.
Configure the Schedule:

In the trigger settings, choose New Trigger.
Enter a name for the trigger (e.g., DailyTrigger).
Set the Start Time (UTC) and specify the recurrence frequency:
Every X Minutes, Hours, or Days.
Associate the Trigger with a Pipeline:

Select the pipeline to attach the trigger to and specify any required parameters.
Publish the Trigger:

Click OK, and then Publish All to activate the trigger.

# 2. Create an Event-Based Trigger
This type of trigger responds to events, such as new files being uploaded to Blob Storage.

Go to the Azure Data Factory UI:

Open the Manage section.
Create an Event Trigger:

Click on Triggers and select New.
Choose Event Trigger.
Configure the Trigger:

Select the Data Source (e.g., Azure Blob Storage).
Specify the Blob Path Begins With and Blob Path Ends With patterns to define the files the trigger will monitor.
Set Trigger Actions:

Attach the pipeline to be triggered.
Provide any parameters required for the pipeline.

# Publish the Trigger:
Save and Publish All to enable the trigger.

# Dynamic Parameters Using Trigger in Linked Services
Step 1: Define Pipeline Parameters

Go to the pipeline in ADF and create pipeline parameters by clicking on the "Parameters" tab.
Example:
filePath for dynamic file paths.
tableName for dynamic database table names.
Step 2: Pass Parameters to Activities

Use the @pipeline().parameters.parameterName syntax in activity settings (e.g., in a Copy Data activity source or sink).
Example:
json
Copy code
"source": {
  "type": "AzureBlobStorage",
  "filePath": "@{pipeline().parameters.filePath}"
}
Step 3: Configure Trigger Parameters

Go to the "Triggers" section and create or edit a trigger.
Add parameters in the trigger definition and map them to the pipeline parameters.
Example:
json

				{
				  "name": "trigger1",
				  "properties": {
				    "annotations": [],
				    "runtimeState": "Stopped",
				    "pipelines": [
					{
					"pipelineReference": {
				          "referenceName": "pipeline1",
				          "type": "PipelineReference"
				        }
				      }	
				    ],
				    "type": "ScheduleTrigger",
				    "typeProperties": {
				      "recurrence": {
				        "frequency": "Day",
				        "interval": 15,
				        "startTime": "2024-11-23T07:13:00",
				        "timeZone": "Eastern Standard Time"
				      }
				    }
				  }
				}
Step 4: Use Parameters in Linked Services

Configure linked services (e.g., Azure Blob Storage, Azure SQL) to accept dynamic inputs.
Use the @{linkedService().parameterName} syntax to pass values dynamically.
Example for a Blob Storage linked service:
json

	{
	  "type": "AzureBlobStorage",
	  "typeProperties": {
	    "connectionString": "@{linkedService().connectionString}",
	    "filePath": "@{pipeline().parameters.filePath}"
	  }
	}
Step 5: Publish and Test

Publish the pipeline and trigger.
Test by running the trigger and verify that dynamic parameters are passed correctly to activities and linked services.


# Best Practices for Triggers
Use Descriptive Names: Name triggers clearly to identify their purpose.
Error Handling: Ensure the pipeline has retry policies and logging enabled for failures.
Monitor Triggered Pipelines: Use the Monitor section in ADF to check trigger execution and troubleshoot issues.
Parameterize Pipelines: Use parameters to handle dynamic data paths, table names, or other configurations.

==================================================================================


# Dynamic Parameters:
 Configure parameters in ADF for flexibility, such as file paths and table names.

#Dynamic Parameters Using Trigger in Linked Services:

## Step 1: Define Pipeline Parameters

Go to the pipeline in ADF and create pipeline parameters by clicking on the "Parameters" tab.

Example:

   filePath for dynamic file paths.

   tableName for dynamic database table names.

## Step 2: Pass Parameters to Activities

Use the @pipeline().parameters.parameterName syntax in activity settings (e.g., in a Copy Data activity source or sink).
		
		Example:
		
		"source": {
		  "type": "AzureBlobStorage",
		  "filePath": "@{pipeline().parameters.filePath}"
		}

## Step 3: Configure Trigger Parameters

Go to the "Triggers" section and create or edit a trigger.

Add parameters in the trigger definition and map them to the pipeline parameters.

		Example:
		
		"type": "Trigger",
		"pipeline": {
		  "parameters": {
		    "filePath": "path/to/new/file.csv",
		    "tableName": "NewTable"
		  }
		}
		
## Step 4: Use Parameters in Linked Services
		
Configure linked services (e.g., Azure Blob Storage, Azure SQL) to accept dynamic inputs.
		
Use the @{linkedService().parameterName} syntax to pass values dynamically.
		
		Example for a Blob Storage linked service:
		
		{
		  "type": "AzureBlobStorage",
		  "typeProperties": {
		    "connectionString": "@{linkedService().connectionString}",
		    "filePath": "@{pipeline().parameters.filePath}"
		  }
		}

## Step 5: Publish and Test

Publish the pipeline and trigger.

Test by running the trigger and verify that dynamic parameters are passed correctly to activities and linked services.

============================================================================================
# GitHub Integration

Integrating GitHub with Azure enhances your development workflow by enabling seamless code management, continuous integration, and deployment. Here are several approaches to achieve this integration:

# 1. GitHub Actions for Azure

GitHub Actions allows you to automate your software workflows directly from your GitHub repository. By utilizing GitHub Actions for Azure, you can build, test, package, release, and deploy applications to Azure services. This integration facilitates a streamlined code-to-cloud automation process. 
MICROSOFT LEARN

# 2. Azure Pipelines Integration

Azure Pipelines, a component of Azure DevOps, provides robust continuous integration and continuous deployment (CI/CD) capabilities. By connecting your GitHub repository to Azure Pipelines, you can automate builds and deployments, ensuring that code changes are consistently tested and delivered. This setup supports various project types and platforms, offering flexibility in deployment targets. 
MICROSOFT LEARN

# 3. Azure Boards and GitHub Integration

Azure Boards offers tools for planning, tracking, and discussing work across teams. Integrating Azure Boards with GitHub repositories enables linking of GitHub commits, pull requests, branches, and issues to work items. This connection provides end-to-end traceability and enhances project management capabilities. 
MICROSOFT LEARN

# 4. Direct Deployment from GitHub to Azure

Azure services like App Service and Azure Functions support direct deployment from GitHub repositories. By configuring continuous deployment, any changes pushed to your GitHub repository automatically deploy to the specified Azure service, facilitating rapid and consistent updates to your applications. 
MICROSOFT LEARN

# 5. Azure DevOps and GitHub Integration

Azure DevOps provides a suite of development tools that integrate with GitHub. By connecting Azure DevOps with your GitHub account, you can utilize Azure Pipelines for CI/CD, Azure Boards for work tracking, and other Azure DevOps services in conjunction with your GitHub repositories. This integration enhances collaboration and streamlines the development process. 
MICROSOFT DEVELOPER BLOGS

Each of these integration methods offers unique benefits, allowing you to choose the approach that best fits your project's requirements and workflow preferences.
		
		![Screenshot 2024-11-29 123249](https://github.com/user-attachments/assets/286a103d-36f2-4bcb-8f93-43b6a7c9cecb)
		![Screenshot 2024-11-29 123230](https://github.com/user-attachments/assets/55aad6ad-2ce8-409b-8edb-a5b78711b515)
		![Screenshot 2024-11-29 123149](https://github.com/user-attachments/assets/2edf3085-7dae-46d5-b0b6-92f13c2a6cff)
		![Screenshot 2024-11-29 123108](https://github.com/user-attachments/assets/a6c183b6-5577-4e88-97ad-5f115761229d)
		![Screenshot 2024-11-29 122918](https://github.com/user-attachments/assets/be6e4343-b434-4103-b5af-2012c9017fdd)
		![Screenshot 2024-11-29 122902](https://github.com/user-attachments/assets/6f464997-5c65-4207-9e6c-4805ac16226d)
		![Screenshot 2024-11-29 122721](https://github.com/user-attachments/assets/cac2cb74-4db3-49ff-aed6-7966db29cb01)
		![Screenshot 2024-11-29 122654](https://github.com/user-attachments/assets/96426499-4fb9-4975-a77a-21f8410830f8)
		![Screenshot 2024-11-29 122640](https://github.com/user-attachments/assets/2667292c-45a5-42ad-97bb-e2d96774c6fa)
  
==============================================================================================

# Range Partioning technique in Azure SQl Database

# Step 1: Create a Partition Function
The partition function defines the range boundaries for partitioning.


		CREATE PARTITION FUNCTION sales_partition_function (DATE)
		AS RANGE RIGHT FOR VALUES ('2021-01-01', '2022-01-01', '2023-01-01');
RANGE RIGHT means each range includes the boundary value.
For example:

Partition 1: All rows with sale_date < '2021-01-01'
Partition 2: All rows with sale_date >= '2021-01-01' and < '2022-01-01'
Partition 3: All rows with sale_date >= '2022-01-01' and < '2023-01-01'
  
# Step 2: Create a Partition Scheme
The partition scheme maps the partition function to specific filegroups. For simplicity, we can map all partitions to the PRIMARY filegroup.


		CREATE PARTITION SCHEME sales_partition_scheme
		AS PARTITION sales_partition_function ALL TO ([PRIMARY]);
  
# Step 3: Create the Partitioned Table
Now, create the table using the partition scheme and define which column (sale_date) should be partitioned.


		CREATE TABLE sales (
		    id INT PRIMARY KEY,
		    amount DECIMAL(10, 2),
		    sale_date DATE
		)
		ON sales_partition_scheme (sale_date);
# Step 4: Insert Sample Data
Insert data into the table as usual. The database will automatically place the rows into the appropriate partitions based on the sale_date.


		INSERT INTO sales (id, amount, sale_date) 
		VALUES 
		    (1, 100.50, '2020-01-15'),
		    (2, 250.00, '2020-06-20'),
		    (3, 175.75, '2020-12-05'),
		    (4, 300.20, '2021-02-14'),
		    (5, 450.10, '2021-07-21'),
		    (6, 500.00, '2021-11-30'),
		    (7, 325.35, '2022-03-25'),
		    (8, 700.80, '2022-08-15'),
		    (9, 150.45, '2023-04-10'),
		    (10, 800.25, '2023-09-05');
# Step 5: Query the Partitioned Table
When querying the data, SQL Server will automatically prune irrelevant partitions, improving query performance.
-- Retrieve data for 2021

		SELECT * FROM sales WHERE sale_date BETWEEN '2021-01-01' AND '2021-12-31';


	SELECT YEAR(sale_date) AS year, SUM(amount) AS total_sales
		FROM sales
		GROUP BY YEAR(sale_date)
		ORDER BY year;
![Screenshot 2024-11-29 193130](https://github.com/user-attachments/assets/c2dc8a8c-d35f-4d7b-8742-b8f75f9cb80e)


# Azure Synapse Analytics: 
If you're working with Azure Synapse Analytics instead of Azure SQL Database, you can use DISTRIBUTION and PARTITION strategies natively, which are different from this approach.
Partition Management: You can update or modify the partition function and scheme to handle new ranges as your data grows (e.g., adding new years).

========================================================================================================================


