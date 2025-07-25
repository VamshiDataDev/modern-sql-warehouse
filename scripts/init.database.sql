/*
=============================================================
Create Database and Schema
=============================================================
Script Purpose:
	Drops and recreates the `DataWarehouse` database if it exists,  
	then creates the layered schemas: `bronze`, `silver`, and `gold`.

Warning: 
	This script performs a destructive overwrite. 
	Ensure backups are taken before executing.
*/

-- Ensure you're in master before dropping the database
USE MASTER;
GO

--Drop and recreate the DataWarehouse database
IF EXISTS (SELECT * FROM SYS.DATABASES WHERE NAME  = 'DataWarehouse')
	DROP DATABASE AMAZON
GO


-- Create the database
CREATE DATABASE DataWarehouse;
GO

-- Switch to new DB context
USE DataWarehouse;
GO

-- Drop schemas if they already exist (precautionary cleanup)
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
	DROP SCHEMA bronze;
GO

IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
	DROP SCHEMA silver;
GO

IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
	DROP SCHEMA gold;
GO

-- Create layered schemas for ELT/ETL
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
