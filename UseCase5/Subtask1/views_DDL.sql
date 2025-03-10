CREATE DATABASE [LDW];

USE [LDW];
GO

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'XXXXXX';

CREATE DATABASE SCOPED CREDENTIAL [AzureStorageCredential]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'XXXXXX';
GO

CREATE EXTERNAL DATA SOURCE [AzureDataLakeSource]
WITH (
    LOCATION = 'https://stdimentoringdatalakeed.dfs.core.windows.net/data',
    CREDENTIAL = [AzureStorageCredential]
);
GO

CREATE OR ALTER VIEW [dbo].[vwEpisode] AS
SELECT
    title_id AS TitleId,
    parent_tv_show_title_id AS ParentTitleId,
    season_number AS SeasonNum,
    episode_number AS EpisodeNum
FROM
    OPENROWSET(
        BULK 'silver/imdb/episode_belongs_to/**',
        DATA_SOURCE = [AzureDataLakeSource],
        FORMAT = 'PARQUET'
    ) AS [source];


CREATE OR ALTER VIEW [dbo].[vwEvent] AS
SELECT
    event_timestamp AS EventTimestamp,
    CAST(DATETIMEFROMPARTS(
        [source].filepath(1),
        [source].filepath(2),
        [source].filepath(3),
        0,
        0,
        0,
        0
    ) AS DATE) AS FilePathDate,
    event_uid AS EventUID,
    event_id AS EventId,
    event_type AS EventType,
    movie_id AS TitleId,
    user_subscription_device_id AS UserSubDevId
FROM
    OPENROWSET(
        BULK 'silver/events/year=*/month=*/day=*/*.parquet',
        DATA_SOURCE = [AzureDataLakeSource],
        FORMAT = 'PARQUET'
    ) AS [source];


CREATE OR ALTER VIEW [dbo].[vwMovie] AS
SELECT
    title_id AS TitleId,
    title_type AS TitleType,
    primary_title AS PrimaryTitle,
    is_adult AS IsAdult,
    start_year AS StartYear,
    end_year AS EndYear,
    runtime_minutes AS RuntimeMinutes
FROM
    OPENROWSET(
        BULK 'silver/imdb/titles/**',
        DATA_SOURCE = [AzureDataLakeSource],
        FORMAT = 'PARQUET'
    ) AS [source];


CREATE OR ALTER VIEW [dbo].[vwUser] AS
SELECT
    user_id AS UserId,
    user_name AS UserName,
    first_name AS FirstName,
    last_name AS LastName,
    user_date_of_birth AS BirthDate
FROM
    OPENROWSET(
        BULK 'silver/youflix/youflix_user/**',
        DATA_SOURCE = [AzureDataLakeSource],
        FORMAT = 'PARQUET'
    ) AS [source];


