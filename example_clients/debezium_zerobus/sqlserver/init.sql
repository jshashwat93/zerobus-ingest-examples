-- Prepares the demo source database: two tables, change data capture on both,
-- and the login Debezium connects with.
--
-- Runs once from the db-init container and is safe to re-run, because every
-- step tests for its own result first.
--
-- sqlcmd variables: DB, DBZ_USER, DBZ_PASSWORD

SET NOCOUNT ON;
GO

-- CREATE DATABASE cannot sit inside a conditional block, hence the EXEC.
IF DB_ID('$(DB)') IS NULL
    EXEC ('CREATE DATABASE [$(DB)]');
GO

-- Logins are server-scoped, so this one lives outside the database.
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '$(DBZ_USER)')
    CREATE LOGIN [$(DBZ_USER)] WITH PASSWORD = '$(DBZ_PASSWORD)', CHECK_POLICY = OFF;
GO

USE [$(DB)];
GO

IF OBJECT_ID('dbo.customers', 'U') IS NULL
    CREATE TABLE dbo.customers (
        customer_id INT          IDENTITY(1,1) NOT NULL PRIMARY KEY,
        full_name   VARCHAR(120) NOT NULL,
        email       VARCHAR(160) NOT NULL,
        city        VARCHAR(80)  NOT NULL,
        -- DATETIME2(6) on purpose: at scale 6 Debezium emits epoch microseconds,
        -- which is the unit Zerobus expects for a Delta TIMESTAMP. A DATETIME2(3)
        -- column emits milliseconds and lands a thousand times too small.
        updated_at  DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()
    );
GO

IF OBJECT_ID('dbo.orders', 'U') IS NULL
    CREATE TABLE dbo.orders (
        order_id     INT           IDENTITY(1,1) NOT NULL PRIMARY KEY,
        customer_id  INT           NOT NULL,
        order_status VARCHAR(20)   NOT NULL,
        amount       DECIMAL(10,2) NOT NULL,
        order_date   DATE          NOT NULL,
        updated_at   DATETIME2(6)  NOT NULL DEFAULT SYSUTCDATETIME()
    );
GO

-- Debezium reads the cdc schema's change tables. db_owner is broader than it
-- strictly needs; narrow this for anything past a demo.
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '$(DBZ_USER)')
    CREATE USER [$(DBZ_USER)] FOR LOGIN [$(DBZ_USER)];
GO

IF IS_ROLEMEMBER('db_owner', '$(DBZ_USER)') = 0
    ALTER ROLE db_owner ADD MEMBER [$(DBZ_USER)];
GO

-- CDC goes on at the database level first, then per table.
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = '$(DB)') = 0
    EXEC sys.sp_cdc_enable_db;
GO

IF NOT EXISTS (SELECT 1
               FROM cdc.change_tables ct
               JOIN sys.tables t ON t.object_id = ct.source_object_id
               WHERE t.name = 'customers')
    EXEC sys.sp_cdc_enable_table
         @source_schema = N'dbo', @source_name = N'customers',
         @role_name = NULL, @supports_net_changes = 1;
GO

IF NOT EXISTS (SELECT 1
               FROM cdc.change_tables ct
               JOIN sys.tables t ON t.object_id = ct.source_object_id
               WHERE t.name = 'orders')
    EXEC sys.sp_cdc_enable_table
         @source_schema = N'dbo', @source_name = N'orders',
         @role_name = NULL, @supports_net_changes = 1;
GO

-- Seed rows. Debezium reports these as snapshot reads, operation "r" rather
-- than "c", which is worth seeing next to the live changes the load generator
-- produces.
IF NOT EXISTS (SELECT 1 FROM dbo.customers)
    INSERT INTO dbo.customers (full_name, email, city) VALUES
        ('Dana Whitfield',    'dana@example.com',   'Chicago'),
        ('Marcus Oyelaran',   'marcus@example.com', 'Lagos'),
        ('Priya Raghunathan', 'priya@example.com',  'Bengaluru');
GO

IF NOT EXISTS (SELECT 1 FROM dbo.orders)
    INSERT INTO dbo.orders (customer_id, order_status, amount, order_date) VALUES
        (1, 'NEW',        249.99, CAST(SYSUTCDATETIME() AS DATE)),
        (2, 'SHIPPED',   1830.00, CAST(SYSUTCDATETIME() AS DATE)),
        (3, 'DELIVERED',   76.45, CAST(SYSUTCDATETIME() AS DATE));
GO

-- Capture only starts once Agent has created the capture job, so report what
-- actually exists rather than assuming.
SELECT ct.capture_instance, t.name AS source_table
FROM cdc.change_tables ct
JOIN sys.tables t ON t.object_id = ct.source_object_id;
GO

PRINT 'source database ready';
GO
