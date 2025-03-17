CREATE OR ALTER PROCEDURE [dbo].[usp_GenerateTop10ShowsDataset]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @datetimeStamp NVARCHAR(14) = FORMAT(GETDATE(), 'yyyyMMddHHmmss');
    DECLARE @folderPath NVARCHAR(MAX) = CONCAT('gold/top10shows/', @datetimeStamp);
    DECLARE @externalTableName NVARCHAR(128) = '[dbo].[extTop10Shows]';
    DECLARE @viewName NVARCHAR(128) = '[dbo].[vwTop10ShowsCurrent]';

    IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'extTop10Shows')
    BEGIN
        DROP EXTERNAL TABLE [dbo].[extTop10Shows];
    END

    DECLARE @cetasSQL NVARCHAR(MAX) = CONCAT(
        'CREATE EXTERNAL TABLE ', @externalTableName, ' ',
        'WITH (',
        '    LOCATION = ''', @folderPath, ''', ',
        '    DATA_SOURCE = [AzureDataLakeSource], ',
        '    FILE_FORMAT = [file-parquet] ',
        ') ',
        'AS ',
        'SELECT EventMonth, ShowTitleId, ShowTitle, EpisodesCount, [Rank] ',
        'FROM [dbo].[vwTop10Shows];'
    );

    EXEC sp_executesql @cetasSQL;

    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vwTop10ShowsCurrent')
    BEGIN
        DROP VIEW [dbo].[vwTop10ShowsCurrent];
    END

    DECLARE @viewSQL NVARCHAR(MAX) = CONCAT(
        'CREATE VIEW ', @viewName, ' AS ',
        'SELECT TOP 10 * FROM OPENROWSET(',
        '    BULK "', @folderPath, '/**", ',
        '    DATA_SOURCE = [AzureDataLakeSource], ',
        '    FORMAT = PARQUET',
        ') AS [result] ',
        'ORDER BY [result].[Rank];'
    );

    EXEC sp_executesql @viewSQL;

    PRINT 'Stored procedure executed successfully.';
END;
