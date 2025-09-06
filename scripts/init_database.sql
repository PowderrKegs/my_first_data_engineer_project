/*

========================================================================
Create Database and Schemas
========================================================================
Script Purpose:
  This script creates a new database named 'data_warehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additional, the script sets up three schemas within the database: 'bronze', 'silver', 'gold'.

Warning:
  Running this script will drop the entire 'data_warehouse' database if it exists.
  All data in the database will be permanently deleted. Process with caution and ensure you have proper backups before running this script.

*/


-- Drop and recreate the 'data_warehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'data_warehouse')
BEGIN
  ALTER DATABASE data_warehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE data_warehouse;
END

-- Create the 'data_warehouse' database
CREATE DATABASE data_warehouse;

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
