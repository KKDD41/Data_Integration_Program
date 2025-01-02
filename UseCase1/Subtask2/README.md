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

7. In `Cmd 1` block, fill in your values for parameters in <> brackets.
8. In `Cmd 2` block, write your code to create YouFlix Database (Schema) using SQL.
9. In `Cmd 2` block, add SQL code to create unmanaged Delta Lake tables in YouFlix Database (Schema)
   according to the schema:

   • YouFlix.youflix_user_delta.
   • YouFlix.youflix_device_delta.
   • YouFlix.youflix_subscription_delta.
   • YouFlix.youflix_user_subscription_device_delta.

   In the step, use Delta data source and your mounted data lake storage.
10. In `Cmd 3` block, complete TODO block in #MERGE BRONZE TO SILVER part of code to load files from
    bronze directory to Delta Lakes tables. Tables should be merged by their keys.
    Refer to the link to learn about upsert into a Delta Lake table using merge. This link will help to understand
    how to delete, update and merge Delta tables
11. In `Cmd 3` block, complete TODO block in #MOVE TO PROCESSED DIRECTORY part of code to move files
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
    Refer to the link to learn about Databricks Utilities (dbutils) and its mv and rm commands.
12. In “Cmd 3” block, complete TODO block #REMOVE Success.csv in part to remove Success.csv file after
    successful load.
