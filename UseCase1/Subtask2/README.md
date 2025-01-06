# Task 1.2 Databricks YouFlix DB Silver

## Prerequisites:

1. Create Azure AAD application and service principal `app-reg-di-mentoring-xx` (`xx` – your initials).

   • Supported account types – Accounts in this organizational directory only (EPAM only - Single tenant).
   ![](./screenshots/app-registration.png)

2. Create new client secret for your `app-reg-di-mentoring-xx`. Security requirement:

   • Store service principal’s Application ID, Tenant ID and Client Secret value as a separate Azure Key Vault
   secrets.
   ![](./screenshots/client-secret-creation.png)
   ![](./screenshots/secrents-in-akv.png)

3. Grant your service principal `app-reg-di-mentoring-xx` access to your data lake by assigning Storage Blob
   Data Contributor role.
   ![](./screenshots/role-blob-contributor-assignment.png)

4. Create Azure Key Vault-backed secret scope in Azure Databricks workspace.
   ![](./screenshots/secret-scope-creation.png)

5. In Azure Databricks workspace, create new cluster with the following settings:

   • Cluster type – Single node.
   • Access mode – No isolation shared.
   • Databricks runtime version – 13.0.
   • Terminate after – 15 minutes.
   ![](./screenshots/cluster-creation.png)

6. Download the notebook `uc1_load_bronze_to_silver.ipynb` by the link and import it to Databricks
   workspace.
   IMPORTANT: in the current task, you do not need to create Unity Catalog.
   ![](./screenshots/notebook-import.png)

7. In `Cmd 1` block, fill in your values for parameters in `<>` brackets.
   ![](./screenshots/complete-cmd-1.png)

8. In `Cmd 2` block, write your code to create YouFlix Database (Schema) using SQL.
9. In `Cmd 2` block, add SQL code to create unmanaged Delta Lake tables in YouFlix Database (Schema)
   according to the schema:

   • YouFlix.youflix_user_delta.
   • YouFlix.youflix_device_delta.
   • YouFlix.youflix_subscription_delta.
   • YouFlix.youflix_user_subscription_device_delta.

   In the step, use Delta data source and your mounted data lake storage.
   ![](./screenshots/complete-cmd-2.png)

10. In `Cmd 3` block, complete TODO block in `#MERGE BRONZE TO SILVER` part of code to load files from
    bronze directory to Delta Lakes tables. Tables should be merged by their keys.
    Refer to the link to learn about upsert into a Delta Lake table using merge. This link will help to understand
    how to delete, update and merge Delta tables.
    ```python
    (
        silver_table.alias('silver') \
                    .merge(
                        bronzeDF_cln.alias('updates'),
                        f'silver.{entity[1]} = updates.{entity[1]}'
                    ) \
                    .whenMatchedUpdate(
                        set =
                        {
                           col: f"updates.{col}" for col in bronzeDF.columns
                        }
                    ) \
                    .whenNotMatchedInsert(values =
                        {
                            col: f"updates.{col}" for col in bronzeDF.columns
                        }
                    ) \
                    .execute()
    )
    ```
11. In `Cmd 3` block, complete TODO block in `#MOVE TO PROCESSED DIRECTORY` part of code to move files
    from bronze layer to processed directory after successful load. The following structure should be used:
    ```commandline
    data
    ├─bronze
    │ └─youflix
    │ └─processed
    │ ├─youflix_user
    │ │ └─yyyy
    │ │ └─mm
    │ │ └─dd
    │ │ └─youflix_user_yyyyMMddHHmmss.csv
    │ ├─youflix_device
    │ │ └─yyyy
    │ │ └─mm
    │ │ └─dd
    │ │ └─youflix_device_yyyyMMddHHmmss.csv
    │ ├─ youflix_subscription
    ...
    ```
    Refer to the link to learn about Databricks Utilities (dbutils) and its `mv` and `rm` commands.
    ```python
    dbutils.fs.mv(file_info.path, processedPath + "/" + file_date[0] + "/" + file_date[1] + "/" + file_date[2] + "/" + file_info.name)
    ```
    ![](./screenshots/processed-folder.png)
    ![](./screenshots/processed-device-path-creation.png)
12. In `Cmd 3` block, complete TODO block `#REMOVE Success.csv` in part to remove `Success.csv` file after
    successful load.
    ```python
    dbutils.fs.rm(f"/mnt/data/bronze/youflix/Success_{entity[0]}.txt")
    ```
    ![](./screenshots/success-deleted.png)


## Full Notebook
```python
# Authenticate Databricks to access Data Lake
# <scope name> value must be updated with Azure Key Vault-backed secret scope name
# <app-id-azure-key-vault-key> must be updated with Azure Key Vault value for application id
# <tenant-id-azure-key-vault-key> must be updated with Azure Key Vault value for tenant id
# <client-secret-azure-key-vault-key> must be updated with Azure Key Vault value for client secret

AppID = dbutils.secrets.get(scope="secret-scope-ed", key="applicationID>")
TenantID = dbutils.secrets.get(scope="secret-scope-ed", key="tenantID")
ClientSecret = dbutils.secrets.get(scope="secret-scope-ed", key="clientSecret")

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": AppID,
          "fs.azure.account.oauth2.client.secret": ClientSecret,
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{tenant}/oauth2/token".format(tenant=TenantID)}
          

# Mounting data in an Azure storage account using an Azure Active Directory (Azure AD) application service principal for authentication
# dbutils.fs.unmount("/mnt/data") #use to unmount data if needed
# <data-lake-name> must be replaced with your Azure Data Lake Storage Gen2 name

try:
    dbutils.fs.mount(
      source = "abfss://data@stdimentoringdatalakeed.dfs.core.windows.net/",
      mount_point = "/mnt/data",
      extra_configs = configs)
except Exception as e:
    if "Directory already mounted" in str(e):
        pass # Ignore error if already mounted.
    else:
        raise e
```


```python
dbutils.fs.mounts()
```




    [MountInfo(mountPoint='/databricks-datasets', source='databricks-datasets', encryptionType=''),
     MountInfo(mountPoint='/Volumes', source='UnityCatalogVolumes', encryptionType=''),
     MountInfo(mountPoint='/databricks/mlflow-tracking', source='databricks/mlflow-tracking', encryptionType=''),
     MountInfo(mountPoint='/databricks-results', source='databricks-results', encryptionType=''),
     MountInfo(mountPoint='/databricks/mlflow-registry', source='databricks/mlflow-registry', encryptionType=''),
     MountInfo(mountPoint='/Volume', source='DbfsReserved', encryptionType=''),
     MountInfo(mountPoint='/volumes', source='DbfsReserved', encryptionType=''),
     MountInfo(mountPoint='/mnt/data', source='wasbs://data@stdimentoringdatalakeed.blob.core.windows.net/', encryptionType=''),
     MountInfo(mountPoint='/', source='DatabricksRoot', encryptionType=''),
     MountInfo(mountPoint='/volume', source='DbfsReserved', encryptionType='')]




```python
%sql
-- Creating database and delta tables if not exist

CREATE DATABASE IF NOT EXISTS YouFlix;
CREATE TABLE IF NOT EXISTS YouFlix.youflix_user_delta (
    user_id BIGINT,
    user_name STRING,
    user_email STRING,
    first_name STRING,
    last_name STRING,
    user_date_of_birth DATE,
    user_address STRING,
    user_phone STRING,
    created_timestamp TIMESTAMP,
    expiration_timestamp TIMESTAMP,
    modified_timestamp TIMESTAMP
)
USING DELTA
LOCATION '/mnt/data/silver/youflix/youflix_user';

CREATE TABLE IF NOT EXISTS YouFlix.youflix_device_delta (
    device_id BIGINT,
    device_name STRING,
    device_type STRING,
    device_os STRING,
    created_timestamp TIMESTAMP
)
USING DELTA 
LOCATION '/mnt/data/silver/youflix/youflix_device';

CREATE TABLE IF NOT EXISTS YouFlix.youflix_subscription_delta (
    subscription_id BIGINT,
    subscription_name STRING,
    subscription_type STRING,
    subscription_video_quality STRING,
    subscription_max_devices INT,
    created_timestamp TIMESTAMP,
    expiration_timestamp TIMESTAMP
)
USING DELTA
LOCATION '/mnt/data/silver/youflix/youflix_subscription';

CREATE TABLE IF NOT EXISTS YouFlix.youflix_user_subscription_device_delta (
    user_subscription_device_id BIGINT,
    user_id BIGINT,
    device_id BIGINT,
    created_timestamp TIMESTAMP
)
USING DELTA
LOCATION '/mnt/data/silver/youflix/youflix_user_subscription_device';

```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr></tr></thead><tbody></tbody></table></div>


```python
import re
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import Window

#dictionary "entities" store the name of the entities and it's BK 
entities = {"device":"device_id", 
          "subscription":"subscription_id", 
          "user":"user_id", 
          "user_subscription_device":"user_subscription_device_id"}

try:
    #looping through every entity
    for entity in entities.items() :   

        bronzePath = "/mnt/data/bronze/youflix/youflix_{entity_name}".format(entity_name=entity[0])
        silverPath = "/mnt/data/silver/youflix/youflix_{entity_name}".format(entity_name=entity[0])
        processedPath = "/mnt/data/bronze/youflix/processed/youflix_{entity_name}".format(entity_name=entity[0])

        #files to load from bronze to silver
        filePaths = dbutils.fs.ls(bronzePath)

        if filePaths:
            
            #MERGE BRONZE TO SILVER
                       
            #TODO
            #use pyspark spark.read.load method to create dataframe based on csv files in the bronzePath
            #do not forget that file has header

            bronzeDF = spark.read.option("header", "true").option("inferSchema", "true").csv(bronzePath)
                       
                       
            partition=Window.partitionBy(entity[1]).orderBy(col("filedate").desc())
           
            bronzeDF_cln = (bronzeDF.withColumn("filedate", to_timestamp(regexp_extract(input_file_name(),'([\d]{14})',0), 'yyyyMMddhhmmss'))
                                    .withColumn("rn",row_number().over(partition))
                                    .filter("rn == 1")
                            )

            #get delta table at the silver path
            silver_table = DeltaTable.forPath(spark, silverPath)

        
            #TODO
            #add your code into brackets below
            #use pyspark merge method to merge bronzeDF_cln dataframe into silver_table by BK. BK for table can be accessible by entity[1]
            #refer to the https://learn.microsoft.com/en-us/azure/databricks/delta/merge to learn about upsert into a Delta Lake table using merge. 
            #this article https://docs.delta.io/latest/delta-update.html#table-deletes-updates-and-merges&language-python will help to understand how to delete, update and merge Delta tables
            (
                silver_table.alias('silver') \
                                .merge(
                                    bronzeDF_cln.alias('updates'),
                                    f'silver.{entity[1]} = updates.{entity[1]}'
                                ) \
                                .whenMatchedUpdate(
                                    set =
                                    {
                                       col: f"updates.{col}" for col in bronzeDF.columns
                                    }
                                ) \
                                .whenNotMatchedInsert(values =
                                    {
                                        col: f"updates.{col}" for col in bronzeDF.columns
                                    }
                                ) \
                                .execute()
            )

            #MOVE TO PROCESSED DIRECTORY 

            #looping through every file in directory
            for file_info in filePaths:

                #creating tuple to store (year, month, day) of the file
                file_date = (re.split("_", file_info.name)[-1][0:4], re.split("_", file_info.name)[-1][4:6], re.split("_", file_info.name)[-1][6:8])
                
                #TODO 
                #complete mv command to move files from bronzePath to processedPath according to the structure in 1.2.7.
                #use file_date tuple to get year, month and day of the file, use file_info.name to get name of file
                dbutils.fs.mv(file_info.path, processedPath + "/" + file_date[0] + "/" + file_date[1] + "/" + file_date[2] + "/" + file_info.name)

            #REMOVE Success.csv

            #TODO 
            dbutils.fs.rm(f"/mnt/data/bronze/youflix/Success_{entity[0]}.txt")
    
        else:
            print("Entity \"{entity}\" - No files for load".format(entity=entity[0]))
    
except Exception as e:
    print(e)
    
```
