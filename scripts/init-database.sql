/*
========================================
Create Database and Schemas
========================================
Script Purpose
	This script creates a new database named 'Datawarewhouse' after checking if it already exists.
	If the database exists , it is dropped and recreated. Additionally, the script sets up three schemas 
	withini the databse: 'bronze','silver','gold'.

Warning:
	Running this script will drop the entire 'DataWarehouse' Database if it exits.
	All data in the data base will be permanently deleted.Proceed with caution
	and ensure you have proper backups before running this script.
	*/

USE master;
GO
  IF EXISTS(Select 1 from sys.database where name='Datawarehouse')
  Begin
  Alter Database Datawarehouse Set single_user with Rollback immediate;
  drop database datawarehouse;
End;
GO

/* Create Database*/
CREATE DATABASE Datawarehouse;
GO
  
USE Datawarehouse;
GO
/* Create Schemas*/
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
