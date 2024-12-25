-- REPLACE $(FullScriptDir) with your path to csv files

PRINT N'Creating Schema [youflix]...';
GO

USE [master];
GO

CREATE DATABASE [YouflixDB]
GO

USE [YouflixDB];
GO

CREATE SCHEMA [youflix];

GO
PRINT N'Creating Schema [youflix_internal]...';

GO
CREATE SCHEMA [youflix_internal];

GO
PRINT N'Creating Table [youflix].[device]...';

GO
CREATE TABLE [youflix].[device] (
    [device_id]         BIGINT        IDENTITY (1, 1) NOT NULL,
    [device_name]       VARCHAR (256) NOT NULL,
    [device_type]       VARCHAR (256) NULL,
    [device_os]         VARCHAR (256) NULL,
    [created_timestamp] DATETIME      NULL,
    CONSTRAINT [PK_device] PRIMARY KEY CLUSTERED ([device_id] ASC)
);

GO
PRINT N'Creating Table [youflix].[subscription]...';

GO
CREATE TABLE [youflix].[subscription] (
    [subscription_id]            BIGINT        IDENTITY (1, 1) NOT NULL,
    [subscription_name]          VARCHAR (256) NOT NULL,
    [subscription_type]          VARCHAR (256) NULL,
    [subscription_video_quality] VARCHAR (256) NULL,
    [subscription_max_devices]   INT           NULL,
    [created_timestamp]          DATETIME      NOT NULL,
    [expiration_timestamp]       DATETIME      NOT NULL,
    CONSTRAINT [PK_subscription] PRIMARY KEY CLUSTERED ([subscription_id] ASC)
);

GO
PRINT N'Creating Table [youflix].[user]...';

GO
CREATE TABLE [youflix].[user] (
    [user_id]                   BIGINT         IDENTITY (1, 1) NOT NULL,
    [user_name]                 VARCHAR (100)  NOT NULL,
    [user_email]                VARCHAR (256)  NULL,
    [first_name]                VARCHAR (256)  NULL,
    [last_name]                 VARCHAR (256)  NULL,
    [user_date_of_birth]        DATE           NULL,
    [user_address]              VARCHAR (1024) NULL,
    [user_phone]                VARCHAR (50)   NULL,
    [created_timestamp]         DATETIME       NOT NULL,
    [expiration_timestamp]      DATETIME       NOT NULL,
    [modified_timestamp]        DATETIME       NOT NULL,
    CONSTRAINT [PK_user] PRIMARY KEY CLUSTERED ([user_id] ASC)
);

GO
PRINT N'Creating Table [youflix].[user_subscription_device]...';

GO
CREATE TABLE [youflix].[user_subscription_device] (
    [user_subscription_device_id] BIGINT   IDENTITY (1, 1) NOT NULL,
    [user_id]                     BIGINT   NOT NULL,
    [subscription_id]             BIGINT   NOT NULL,
    [device_id]                   BIGINT   NOT NULL,
    [created_timestamp]           DATETIME NOT NULL,
    CONSTRAINT [PK_user_subscription_device] PRIMARY KEY CLUSTERED ([user_subscription_device_id] ASC)
);

GO
PRINT N'Creating Table [youflix_internal].[user]...';

GO
CREATE TABLE [youflix_internal].[user] (
    [user_id]                BIGINT         IDENTITY (1, 1) NOT NULL,
    [user_name]              VARCHAR (100)  NOT NULL,
    [user_email]             VARCHAR (256)  NULL,
    [first_name]             VARCHAR (256)  NULL,
    [last_name]              VARCHAR (256)  NULL,
    [user_date_of_birth]     DATE           NULL,
    [user_address]           VARCHAR (1024) NULL,
    [user_phone]             VARCHAR (50)   NULL,
    [created_timestamp]      DATETIME       NOT NULL
);

GO
PRINT N'Creating Table [youflix_internal].[user_subscription_device]...';

GO
CREATE TABLE [youflix_internal].[user_subscription_device] (
    [user_subscription_device_id] BIGINT IDENTITY (1, 1) NOT NULL,
    [user_id]                     BIGINT NOT NULL,
    [subscription_id]             BIGINT NOT NULL,
    [device_id]                   BIGINT NOT NULL
);

GO
PRINT N'Creating Foreign Key [youflix].[FK_user_subscription_device_user]...';

GO
ALTER TABLE [youflix].[user_subscription_device] WITH NOCHECK
    ADD CONSTRAINT [FK_user_subscription_device_user] FOREIGN KEY ([user_id]) REFERENCES [youflix].[user] ([user_id]);

GO
PRINT N'Creating Foreign Key [youflix].[FK_user_subscription_device_subscription]...';


GO
ALTER TABLE [youflix].[user_subscription_device] WITH NOCHECK
    ADD CONSTRAINT [FK_user_subscription_device_subscription] FOREIGN KEY ([subscription_id]) REFERENCES [youflix].[subscription] ([subscription_id]);

GO
PRINT N'Creating Foreign Key [youflix].[FK_user_subscription_device_device]...';

GO
ALTER TABLE [youflix].[user_subscription_device] WITH NOCHECK
    ADD CONSTRAINT [FK_user_subscription_device_device] FOREIGN KEY ([device_id]) REFERENCES [youflix].[device] ([device_id]);


GO
PRINT N'Creating Procedure [youflix].[sp_youflix_tables_insert_update]...';

GO
CREATE PROCEDURE [youflix_internal].[sp_youflix_tables_insert_update] 
(
    @delta_users BIGINT,
    @updated_users BIGINT = 0
)
AS
    SET NOCOUNT, XACT_ABORT ON;
    /*
        The youflix_internal.sp_youflix_tables_insert_update(delta_users) procedure emulates delta by moving 
        data from "youflix_internal" schema to the main "youflix" schema for two tables: [user]
        and [user_subscription_device].
        Parameters:
            * @delta_users bigint - maximum number of users to move on procedure call
            * @updated_users bigint - maximum number of users to update on procedure call
    */

DECLARE 
    @count_youflix_internal_users BIGINT = 0,
    @count_youflix_update_users BIGINT = 0;

BEGIN

    -- Update some users in users table
    DROP TABLE IF EXISTS #upd_youflix_users ;
    SELECT TOP (@updated_users)
        [user_id] 
    INTO #upd_youflix_users
    FROM youflix.[user]
    ORDER BY NEWID();

    -- Check if any data available for updating.
    SET @count_youflix_update_users = (SELECT COUNT(*) FROM #upd_youflix_users);

    IF @count_youflix_update_users = 0 
        PRINT 'Nothing to update in YOUFLIX.user table.';
    ELSE
      BEGIN
        PRINT CAST(@count_youflix_update_users AS VARCHAR (255)) + ': updating users in YOUFLIX.user table.';

        UPDATE youflix.[user] 
        SET modified_timestamp = GETUTCDATE(), 
            expiration_timestamp = DATEADD(MONTH, 3, GETUTCDATE())
        WHERE [user_id] IN 
            (SELECT [user_id] FROM #upd_youflix_users);
      END

    -- Check if any data available for moving. Exit if there are no data in youflix_internal schema.
    SET @count_youflix_internal_users = (SELECT COUNT(*) FROM youflix_internal.[user]);

    IF @count_youflix_internal_users = 0 
        THROW 60000, 'Nothing to move to the main YOUFLIX schema.', 1;
    ELSE
        PRINT 'Moving users to the main YOUFLIX schema.';

    -- Move data from youflix_internal.user to youflix.user
    DROP TABLE IF EXISTS #ins_youflix_users ;

    SELECT TOP (@delta_users)
         [user_id] 
        ,[user_name]
        ,user_email
        ,first_name
        ,last_name 
        ,user_date_of_birth
        ,user_address 
        ,user_phone 
        ,created_timestamp
    INTO #ins_youflix_users
    FROM youflix_internal.[user]
    ORDER BY [user_id] ASC;

    SET IDENTITY_INSERT youflix.[user] ON;

    INSERT INTO youflix.[user](
         [user_id] 
        ,[user_name] 
        ,user_email
        ,first_name
        ,last_name 
        ,user_date_of_birth
        ,user_address 
        ,user_phone 
        ,created_timestamp
        ,expiration_timestamp
        ,modified_timestamp
        )
      SELECT 
           [user_id] 
          ,[user_name] 
          ,user_email
          ,first_name
          ,last_name 
          ,user_date_of_birth
          ,user_address 
          ,user_phone 
          ,created_timestamp
          ,DATEADD(MONTH, 3, GETUTCDATE())
          ,GETUTCDATE()
      FROM #ins_youflix_users
      ORDER BY [user_id] ASC ;

    SET IDENTITY_INSERT youflix.[user] OFF;

    DELETE FROM youflix_internal.[user]
    WHERE youflix_internal.[user].[user_id] IN 
        (SELECT [user_id] FROM #ins_youflix_users);

    -- Move data from youflix_internal.user_subscription_device to youflix.user_subscription_device
    DROP TABLE IF EXISTS #ins_youflix_user_subscription_devices;

    SELECT 
         s.user_subscription_device_id 
        ,s.[user_id]
        ,s.subscription_id 
        ,s.device_id 
    INTO #ins_youflix_user_subscription_devices
    FROM #ins_youflix_users u 
    JOIN youflix_internal.user_subscription_device s
        ON u.[user_id] = s.[user_id]
    ORDER BY user_subscription_device_id ASC ;

    SET IDENTITY_INSERT  youflix.user_subscription_device ON;

    INSERT INTO youflix.user_subscription_device(
         user_subscription_device_id 
        ,[user_id] 
        ,subscription_id 
        ,device_id
        ,created_timestamp)
        SELECT
            user_subscription_device_id 
            ,[user_id] 
            ,subscription_id 
            ,device_id
            ,GETUTCDATE()
        FROM #ins_youflix_user_subscription_devices
        ORDER BY user_subscription_device_id ASC ;

    SET IDENTITY_INSERT  youflix.user_subscription_device OFF;

    DELETE FROM youflix_internal.user_subscription_device 
    WHERE youflix_internal.user_subscription_device.user_subscription_device_id IN 
        (SELECT user_subscription_device_id FROM #ins_youflix_user_subscription_devices);   

    -- Display results
    DECLARE @count_rows_users VARCHAR(255) = (SELECT CAST(COUNT(*) AS VARCHAR(10)) FROM #ins_youflix_users) ;
    PRINT @count_rows_users + ' users moved to the main YOUFLIX schema.';

    DECLARE @count_rows_user_subscription_devices VARCHAR(255) = (SELECT CAST(COUNT(*) AS VARCHAR(10)) FROM #ins_youflix_user_subscription_devices);
    PRINT @count_rows_user_subscription_devices + ' devices moved to the main YOUFLIX schema.';
    
    DROP TABLE IF EXISTS #ins_youflix_users ;
    DROP TABLE IF EXISTS #ins_youflix_user_subscription_devices ;


-- Run the youflix_internal.sp_youflix_tables_insert_update procedure:
-- EXEC youflix_internal.sp_youflix_tables_insert_update(10000, 10); 

-- Check statistics in main and internal schemes
/*
SELECT     'youflix.user' as tbl_name, (SELECT count(*) FROM youflix."user") as cnt, 
        'youflix_internal.user' as int_tbl_name, (SELECT count(*) FROM youflix_internal."user") as int_cnt
UNION ALL
SELECT     'youflix.user_subscription_device' as tbl_name, (SELECT count(*) FROM youflix.user_subscription_device) as cnt,
        'youflix_internal.user_subscription_device' as int_tbl_name, (SELECT count(*) FROM youflix_internal.user_subscription_device) as int_cnt; */

END
GO

/*
Post-Deployment Script
*/

PRINT 'Start Post Deployment';

--=========================================
-- Populate [youflix_internal].[user]
--=========================================

BULK INSERT [youflix_internal].[user]
FROM 'C:\Users\Ekaterina_Dul\PycharmProjects\Data_Integration_Program\UseCase1\Subtask1\data\youflix.user.csv'
WITH
( 
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FORMAT='CSV'
)
GO
--=========================================
-- Populate [youflix_internal].[user_subscription_device]
--=========================================

BULK INSERT [youflix_internal].[user_subscription_device]
FROM 'C:\Users\Ekaterina_Dul\PycharmProjects\Data_Integration_Program\UseCase1\Subtask1\data\youflix.user_subscription_device.csv'
WITH 
(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FORMAT='CSV'
)
GO
SET NOCOUNT ON
SET IDENTITY_INSERT youflix.subscription ON;
GO

MERGE youflix.subscription  AS trg
USING (
    SELECT 
      subscription_id, 
      subscription_name,
      subscription_type,
      subscription_video_quality,
      subscription_max_devices,
      created_timestamp,
      expiration_timestamp 
    FROM (
       VALUES
        (1,   'Basic subscripion with two allowed devices and HD video quality.',      'BASIC',     'HD',       2,  '2021-01-01',   '9999-01-01'),
        (2,   'Standard subscripion with four allowed devices and 4k video quality.',  'STANDARD',  'FullHD',   4,  '2021-01-01',   '9999-01-01'),	
        (3,   'Premium subscripion with seven allowed devices and HD video quality.',  'PREMIUM',   '4k',       7,  '2021-01-01',   '9999-01-01')
      ) AS subscr
        (subscription_id, subscription_name, subscription_type, subscription_video_quality, subscription_max_devices, created_timestamp, expiration_timestamp ) 
    ) AS src
ON trg.subscription_id = src.subscription_id
WHEN MATCHED 
    AND (trg.subscription_name          != src.subscription_name
      OR trg.subscription_type          != src.subscription_type
      OR trg.subscription_video_quality != src.subscription_video_quality
      OR trg.subscription_max_devices   != src.subscription_max_devices
      OR trg.created_timestamp          != src.created_timestamp
      OR trg.expiration_timestamp       != src.expiration_timestamp
      )
THEN UPDATE 
    SET
      trg.subscription_name           = src.subscription_name,
      trg.subscription_type           = src.subscription_type,
      trg.subscription_video_quality  = src.subscription_video_quality,
      trg.subscription_max_devices    = src.subscription_max_devices,
      trg.created_timestamp           = src.created_timestamp,
      trg.expiration_timestamp        = src.expiration_timestamp
WHEN NOT MATCHED BY TARGET 
THEN INSERT (
      subscription_id, 
      subscription_name,
      subscription_type,
      subscription_video_quality,
      subscription_max_devices,
      created_timestamp,
      expiration_timestamp  )
VALUES (
      src.subscription_id, 
      src.subscription_name,
      src.subscription_type,
      src.subscription_video_quality,
      src.subscription_max_devices,
      src.created_timestamp,
      src.expiration_timestamp 
);

SET IDENTITY_INSERT youflix.subscription OFF;
GO
SET NOCOUNT ON
SET IDENTITY_INSERT youflix.device ON;
GO

MERGE youflix.device  AS trg
USING (
    SELECT 
      device_id,
      device_name,
      device_type,
      device_os,
      created_timestamp
     FROM (
       VALUES
        (1,         'HP',                 'PC',         'Windows'  ,  '2021-01-01'       ),
        (2,         'Lenovo',             'PC',         'Linux'    ,  '2021-01-01'       ),
        (3,         'iMac',               'PC',         'macOS'    ,  '2021-01-01'       ),
        (4,         'Raspberry Pi',       'PC',         'Linux'    ,  '2021-01-01'       ),
        (5,         'MSI',                'PC',         'Windows'  ,  '2021-01-01'       ),
        (6,         'Dell',               'PC',         'Windows'  ,  '2021-01-01'       ),
        (7,         'HP',                 'Laptop',     'Windows'  ,  '2021-01-01'       ),
        (8,         'Dell',               'Laptop',     'Windows'  ,  '2021-01-01'       ),
        (9,         'Lenovo',             'Laptop',     'Windows'  ,  '2021-01-01'       ),
        (10,        'ASUS',               'Laptop',     'Linux'    ,  '2021-01-01'       ),
        (11,        'Apple MacBook Air',  'Laptop',     'macOS'    ,  '2021-01-01'       ),
        (12,        'Apple MacBook',      'Laptop',     'macOS'    ,  '2021-01-01'       ),
        (13,        'Apple MacBook Pro',  'Laptop',     'macOS'    ,  '2021-01-01'       ),
        (14,        'iPhone 8',           'Mobile',     'iOS'      ,  '2021-01-01'       ),
        (15,        'iPhone 11',          'Mobile',     'iOS'      ,  '2021-01-01'       ),
        (16,        'iPhone 12',          'Mobile',     'iOS'      ,  '2021-01-01'       ),
        (17,        'iPhone 13',          'Mobile',     'iOS'      ,  '2021-01-01'       ),
        (18,        'Samsung S10',        'Mobile',     'Android'  ,  '2021-01-01'       ),
        (19,        'Samsung Note',       'Mobile',     'Android'  ,  '2021-01-01'       ),
        (20,        'Samsung S20',        'Mobile',     'Android'  ,  '2021-01-01'       ),
        (21,        'Xiaomi Civi Pro',    'Mobile',     'Android'  ,  '2021-01-01'       ),
        (22,        'Sony Xperia Pro-I',  'Mobile',     'Android'  ,  '2021-01-01'       ),
        (23,        'Meizu 18',           'Mobile',     'Android'  ,  '2021-01-01'       ),
        (24,        'Huawei Mate 40',     'Mobile',     'Android'  ,  '2021-01-01'       ),
        (25,        'Apple TV',           'SmartTV',    'tvOS'     ,  '2021-01-01'       ),
        (26,        'Samsung',            'SmartTV',    'Android'  ,  '2021-01-01'       ),
        (27,        'VOX',                'SmartTV',    'Android'  ,  '2021-01-01'       ),
        (28,        'Sony',               'SmartTV',    'Linux'    ,  '2021-01-01'       ),
        (29,        'LG Signature',       'SmartTV',    'WebOS'    ,  '2021-01-01'       ),
        (30,        'LG OLED',            'SmartTV',    'WebOS'    ,  '2021-01-01'       )) AS device 
        (device_id, device_name,          device_type,  device_os,   created_timestamp) 
    ) AS src
ON trg.device_id = src.device_id
WHEN MATCHED 
    AND (trg.device_name       != src.device_name
      OR trg.device_type       != src.device_type
      OR trg.device_os         != src.device_os 
      OR trg.created_timestamp != src.created_timestamp)
THEN UPDATE 
    SET
      trg.device_name       = src.device_name,
      trg.device_type       = src.device_type,
      trg.device_os         = src.device_os,
      trg.created_timestamp = src.created_timestamp
WHEN NOT MATCHED BY TARGET 
THEN INSERT (
    device_id,
    device_name,
    device_type,
    device_os,
    created_timestamp )
VALUES (
    src.device_id,
    src.device_name,
    src.device_type,
    src.device_os,
    src.created_timestamp
);
   
GO

SET IDENTITY_INSERT youflix.device OFF;
GO


EXEC youflix_internal.sp_youflix_tables_insert_update 10000; 
GO 

PRINT 'Post Deployment process is completed';
GO
