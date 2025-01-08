# Task 1.3 Orchestration with Storage Event Trigger

## Steps to complete the task:

1. In Azure Synapse Studio, create storage event trigger `TRG_UC1` with the following settings:
   • Blob path begins with – full path to `Success.csv`.
   • Event – Blob created.
   • Ignore empty blobs – Yes.
   ![](./screenshots/trigger-creation.png)

2. In Azure Synapse Studio, create Azure Databricks linked service. Security requirements:
   • Select cluster – Existing interactive cluster.
   • Authentication type – Managed service identity (refer to the link).
   • Existing cluster ID – your cluster ID.
   ![](./screenshots/databricks-linked-service.png)

3. Create a pipeline and add Notebook activity to execute Azure Databricks runbook created in previous task.
   ![](./screenshots/databricks-pipeline.png)

4. Link your new pipeline with `TRG_UC1` trigger.
   ![](./screenshots/linked-trigger-to-pipeline.png)
   ![](./screenshots/trigger-matching-files.png)
