# Task 1.1 Synapse Pipeline with Copy Data Activity

## Prerequisites:

1. Creation and configuration a self-hosted Integration Runtime in Synapse Analytics via UI to connect to MS SQL
   Server.
   ![](./screenshots/ir-local-setup.png)
   ![](./screenshots/ir-gui-on-azure.png)
2. In SSMS, `DeploymentScript.sql` script was run on master database with data from `youflix.user.csv`,
   `youflix.user_subscription_device.csv`.
   ![](./screenshots/ssms-database-setup.png)

3. In data lake `stdimentoringdatalakexx` and created “youflix” subfolder in “bronze” and "silver" directories.
   ![](./screenshots/bronze-folder.png)
   ![](./screenshots/silver-folder.png)

4. Creation of Synapse Studio linked services to AKV, Azure Table `YouFlixWatermark`, MS SQL Server tables and data
   lake.
   ![](./screenshots/linked-services-created.png)

5. Creation entity for every source table in `YouFlixWatermark` Azure Table.
   ![](./screenshots/watermark_table_entities.png)

6. Creation of sink intergation datasets to data lake folders.
   ![](./screenshots/integration-datasets.png)
   File format:
   ![](./screenshots/sink-file-format.png)

## Incremental Copy Pipeline for one table `youflix.device`:

1. `LookupOldWatermark` activity:
   ![](./screenshots/lookup-old-watermark.png)
2. `LookupNewWatermark` activity:
   ![](./screenshots/lookup-new-watermark.png)
3. `IncrementalCopyActivity` source and sink:
   ![](./screenshots/copy-source.png)
   Source query:
   ```sql
   select 
       * 
   from youflix.device 
   where 
       created_timestamp > '@{activity('LookupOldWatermark').output.firstRow.Watermark}' 
       and created_timestamp <= '@{activity('LookupNewWatermark').output.firstRow.NewWatermarkvalue}'
   ```
4. `UpdateWatermark` copy activity:
   ![](./screenshots/update-watermark-sink.png)
   ![](./screenshots/update-watermark-source.png)
   Columns mapping:
   ![](./screenshots/update-watermark-mapping.png)
5. `GenerateSuccess` activity:
6. Pipeline with loading of `youflix.device` table run:
   ![](./screenshots/pipeline-run.png)
   ![](./screenshots/watermark-device-updated.png)
   ![](./screenshots/data-added-to-device.png)

## Parameterized Incremental Copy Pipeline for one table `youflix.device`:

1. Pipeline parameters:
   `json
   [
       {
           "TABLE_NAME": "device"
       },
       {
           "TABLE_NAME": "user"
       },
       {
           "TABLE_NAME": "subscription"
       },
       {
           "TABLE_NAME": "user_subscription_device"
       }
   ]`
2. `LookupOldWatermark` activity:
   ![](./screenshots/lookup-old-watermark-foreach.png)
3. `LookupNewWatermark` activity:
   ![](./screenshots/lookup-new-watermark-foreach.png)
4. `IncrementalCopyActivity` source and sink:
   ![](./screenshots/copy-data-foreach-source.png)
   ![](./screenshots/copy-data-foreach-sink.png)
   Source query:
```sql
select 
    * 
from youflix.@{item().TABLE_NAME
where 
    created_timestamp > '@{activity('LookupOldWatermark').output.firstRow.Watermark}' 
    and created_timestamp <= '@{activity('LookupNewWatermark').output.firstRow.NewWatermarkvalue}'
```

5. `UpdateWatermark` copy activity:
   ![](./screenshots/update-watermark-source.png)
   ![](./screenshots/update-watermark-sink-foreach.png)
   Columns mapping:
   ![](./screenshots/update-watermark-mapping-foreach.png)
6. `GenerateSuccess` activity:
   ![](./screenshots/generate-success-foreach-sink.png)
   ![](./screenshots/generate-success-foreach-source.png)
7. Pipeline with loading of all `youflix` tables run:
   ![](./screenshots/foreach-success.png)
   ![](./screenshots/success-foreach-files.png)
