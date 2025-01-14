# Task 1.1 Synapse Pipeline with Copy Data Activity

## Run Scenario:

1. Before starting the run scenario, clear `bronze/youflix` folder and re-create `YouFlixDB` database from
   scratch using `DeploymentScript.sql` script. It is necessary to get rid of the results of your previous execution
   and testing activities.
2. Go to data lake `stdimentoringdatalakexx` and proceed to Storage browser, then click on Tables and edit
   each of entity by setting watermark value to `2000-01-01T00:00:00.00Z`.
   ![](./screenshots/watermark_table_entities.png)

3. Take screenshot(s) of your pipeline.
   ![](./screenshots/for-each-pipeline.png)
   ![](./screenshots/lookup-old-watermark-foreach.png)
   ![](./screenshots/lookup-new-watermark-foreach.png)
   ![](./screenshots/copy-data-foreach-source.png)
   Source query:
   ```sql
   select 
       * 
   from youflix.@{item().TABLE_NAME}
   where 
       created_timestamp > cast('@{activity('LookupOldWatermark').output.firstRow.Watermark}' as datetime2) 
       and created_timestamp <= cast('@{activity('LookupNewWatermark').output.firstRow.NewWatermarkvalue}' as datetime2)
   ```
   ![](./screenshots/copy-data-foreach-sink.png)
   ![](./screenshots/update-watermark-source.png)
   ![](./screenshots/update-watermark-sink-foreach.png)
   ![](./screenshots/update-watermark-mapping-foreach.png)
   ![](./screenshots/generate-success-foreach-sink.png)
   ![](./screenshots/generate-success-foreach-source.png)

4. Go to your created pipeline and execute it manually.
   ![](./screenshots/foreach-success.png)
5. Once the job succeeds, check `bronze/youflix` folder, it should contain 4 folders and `Success.csv` file.
6. Take screenshot(s) of the target folder.
   ![](./screenshots/success-foreach-files.png)

7. Go inside each of 4 folders and take screenshot(s) of files generated.
   ![](./screenshots/device-result.png)
   ![](./screenshots/subscription-result.png)
   ![](./screenshots/user-subscription-device-result.png)

8. In Synapse Workspace, navigate to Data section, find in Linked tab your container, open `bronze/youflix`
   and select top 10 rows for each file using SQL queries.
9. Take screenshot(s) of SQL queries and result output.
   ![](./screenshots/device-query.png)
   ![](./screenshots/subscription-query.png)
   ![](./screenshots/user-subscription-device-query.png)

10. In Synapse Workspace, navigate to Data section, find in Linked tab your container, open `bronze/youflix`
    and check number of rows for each file using SQL query.
11. Take screenshot(s) of SQL queries with count values.
    ![](./screenshots/subscription-count.png)
    ![](./screenshots/user-subscription-device-count.png)
    ![](./screenshots/device-count.png)

12. Go to your Azure Table and take a screenshot of new watermark values.
    ![](./screenshots/updated-watermark-table.png)

13. Without changing source data, execute your pipeline one more time.
14. In Synapse Workspace, navigate to Data section, find in Linked tab your container, open `bronze/youflix`
    and check number of rows for each newly loaded file using SQL query.
15. Take screenshot(s) of SQL queries with count values.
    ![](./screenshots/user-subscription-device-empty.png)
    ![](./screenshots/device-empty.png)
    ![](./screenshots/subscription-empty.png)
   
16. Connect to MS SQL Server `YouFlixDB` database and run the following command:
    ```sql
    EXEC youflix_internal.sp_youflix_tables_insert_update 10000, 15;
    ```
    ![](./screenshots/updating-onperm.png)
    
17. Execute pipeline manually again.
18. In Synapse Workspace, navigate to Data section, find in Linked tab your container, open `bronze/youflix`
    and check number of rows for each newly loaded file using SQL query.
19. Take screenshot(s) of SQL queries with count values.
    ![](./screenshots/user-subscription-device-count-after-rerun.png)

