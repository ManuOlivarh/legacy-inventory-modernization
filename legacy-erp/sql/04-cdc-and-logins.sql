USE ERPDB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'ERPDB' AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO

IF DATABASE_PRINCIPAL_ID('cdc_reader') IS NULL
    CREATE ROLE cdc_reader;
GO

DECLARE @tables TABLE (name SYSNAME);
INSERT INTO @tables VALUES ('INV_ART'), ('INV_STK'), ('INV_MOV');

DECLARE @t SYSNAME;
DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @tables;
OPEN c;
FETCH NEXT FROM c INTO @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = @t AND is_tracked_by_cdc = 1)
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name   = @t,
            @role_name     = N'cdc_reader',
            @supports_net_changes = 0;
    FETCH NEXT FROM c INTO @t;
END;
CLOSE c;
DEALLOCATE c;
GO

USE master;
GO
IF SUSER_ID('debezium') IS NULL
    CREATE LOGIN debezium WITH PASSWORD = '$(DEBEZIUM_PASSWORD)';
GO
GRANT VIEW SERVER STATE TO debezium;
GO
USE ERPDB;
GO
IF DATABASE_PRINCIPAL_ID('debezium') IS NULL
    CREATE USER debezium FOR LOGIN debezium;
GO
ALTER ROLE db_datareader ADD MEMBER debezium;
ALTER ROLE cdc_reader ADD MEMBER debezium;
GRANT SELECT ON SCHEMA::cdc TO debezium;
GRANT VIEW DATABASE STATE TO debezium;
GO

USE master;
GO
IF SUSER_ID('erp_app') IS NULL
    CREATE LOGIN erp_app WITH PASSWORD = '$(ERP_APP_PASSWORD)';
GO
USE ERPDB;
GO
IF DATABASE_PRINCIPAL_ID('erp_app') IS NULL
    CREATE USER erp_app FOR LOGIN erp_app;
GO
GRANT EXECUTE ON dbo.SP_INV_MOVIMIENTO TO erp_app;
GRANT SELECT ON dbo.INV_ART TO erp_app;
GRANT SELECT ON dbo.INV_BOD TO erp_app;
GO
