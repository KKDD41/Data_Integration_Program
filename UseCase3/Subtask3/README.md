# Task 3.1 Orchestration with Custom Event Trigger

## Run Scenario:
1. Take a screenshot of your `TRG_UC3` trigger.

   ![](./screenshots/trigger-uc3.png)

2. Take a screenshot of your pipeline. Pipeline parameters should be visible in the picture.
   ![](./screenshots/pipeline-params.png)
   ![](./screenshots/trigger-run-parameters.png)

3. Take a screenshot of your Notebook activity settings with expanded Base parameters list.
   ![](./screenshots/base-params.png)
   ![](./screenshots/params-in-notebook.png)

4. Clear “bronze/tmdb” and “silver/tmdb” folders.
5. Run the function from Azure portal manually.
6. Make sure that files are uploaded to the data lake “bronze/tmdb” folder.
   ![](./screenshots/azure-function-run.png)

7. Make sure that `uc3_load_bronze_to_silver.ipynb` notebook finishes successfully, and new data uploaded
   to the data lake “silver/tmdb” folder.
   ![](./screenshots/data-in-silver.png)

8. Take a screenshot of pipeline runs Monitor with successful finished pipeline.
   ![](./screenshots/pipeline-success.png)