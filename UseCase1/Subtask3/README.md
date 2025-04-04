# Task 1.3 Orchestration with Storage Event Trigger

## Run Scenario:

1. Before starting the run scenario, clear `“bronze/youflix”` and `“silver/youflix”` directories.
2. Delete and re-create YouFlixDB database from scratch using `DeploymentScript.sql` script. It is necessary to
   get rid of the results of your previous execution and testing activities.
3. Drop `YouFlix` database from your Databricks workspace.
4. Go to data lake `stdimentoringdatalakexx` and proceed to Storage browser, then click on Tables and edit
   each of entity by setting watermark value to `2000-01-01T00:00:00.00Z`.
5. Take screenshot(s) of created trigger.
   ![](./screenshots/linked-trigger-to-pipeline.png)
   ![](./screenshots/trigger-matching-files.png)

6. Execute pipeline from task 1.1 manually.
7. When it’s completed and pipeline from task 1.2 is triggered, take screenshot(s) from Monitor tab with
   `“Triggered by”` column is visible for the triggered pipeline.
   ![](./screenshots/databricks-pipeline.png)
   ![](./screenshots/pipeline-triggered.png)

8. In Synapse Workspace, navigate to Data section, find in Linked tab your container, open `“silver/youflix”` and
   check number of rows for each delta table using SQL query.
   - `youflix_device` – 30
   - `youflix_subscription` – 3
   - `youflix_user` – 10000
   - `youflix_user_subscription_device` – 26746
   
9. Take screenshot(s) of SQL queries with count values.
   ![](./screenshots/subscription-count-silver.png)
   ![](./screenshots/device-count-silver.png)
   ![](./screenshots/user-subscription-device-sount-silver.png)
   ![](./screenshots/user-count-silver.png) 

10. Connect to MS SQL Server YouFlix database and run the following command:
    ```sql
    EXEC youflix_internal.sp_youflix_tables_insert_update 200000, 0;
    ```
    ![](./screenshots/table-update.png)

11. Execute pipeline from task 1.1 manually and wait until triggered pipeline from 1.2 is finished.
12. In Synapse Workspace, navigate to Data section, find in Linked tab your container, open `silver/youflix` and
    check number of rows for each delta table using SQL query.
    - `youflix_device` – 30
    - `youflix_subscription` – 3
    - `youflix_user` – 200000
    - `youflix_user_subscription_device` – 531173

13. Take screenshot(s) of SQL queries with count values.
    ![](./screenshots/user-subscription-device-sount-upd.png)
    ![](./screenshots/device-count-upd.png)
    ![](./screenshots/subscription-count-upd.png)
    ![](./screenshots/user-count-upd.png)