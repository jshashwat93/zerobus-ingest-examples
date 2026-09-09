-- Target tables for the Debezium Zerobus example.
--
-- One Delta column per source column, so these tables mirror dbo.orders and
-- dbo.customers in the source database.
--
-- Before running:
--   1. Replace <catalog> and <schema> with your values.
--   2. Replace <service-principal-id> with the service principal's Application ID.
--   3. Run in Databricks SQL, or let setup.sh do all of it for you.
--
-- Zerobus requirements the table has to satisfy:
--   - Unity Catalog managed Delta table, in the same region as the workspace.
--   - Change Data Feed and column mapping disabled.
--   - The tables must exist before Debezium starts. Zerobus never creates them.

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.orders (
  order_id     INT           COMMENT 'Source primary key',
  customer_id  INT,
  order_status STRING,
  amount       DECIMAL(10,2),
  order_date   DATE,
  updated_at   TIMESTAMP,
  -- Deletes arrive as the STRING "true" here, not a BOOLEAN.
  __deleted    STRING        COMMENT '"true" when the source row was deleted'
) USING DELTA
COMMENT 'SQL Server dbo.orders, replicated by Debezium Server via Zerobus Ingest'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'false',
  'delta.enableRowTracking'    = 'false'
);

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.customers (
  customer_id INT       COMMENT 'Source primary key',
  full_name   STRING,
  email       STRING,
  city        STRING,
  updated_at  TIMESTAMP,
  __deleted   STRING    COMMENT '"true" when the source row was deleted'
) USING DELTA
COMMENT 'SQL Server dbo.customers, replicated by Debezium Server via Zerobus Ingest'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'false',
  'delta.enableRowTracking'    = 'false'
);

-- Grant the service principal access.
-- Important: GRANT ALL PRIVILEGES is not sufficient, and neither is inheriting
-- from the schema. Zerobus needs SELECT and MODIFY granted on the table itself.
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-id>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.orders TO `<service-principal-id>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.customers TO `<service-principal-id>`;
