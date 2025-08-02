/*
===============================================================================
Procedure: bronze_ingestion_proc
Purpose  : Load raw CSV data into Bronze tables (Source → Bronze Layer)
===============================================================================

Description:
    - Truncates existing data in 'bronze' schema tables
    - Loads data from CSV files using BULK INSERT

Parameters : None  

Usage:
    EXEC bronze.load_bronze;

Notes:

===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -- Declare all timing variables
    DECLARE @proc_start_time    DATETIME;  -- Entire procedure start
    DECLARE @load_start_time    DATETIME;  -- Overall data load start
    DECLARE @start_time   DATETIME;  -- Per-table load start
    DECLARE @end_time     DATETIME;  -- Per-table load end
    DECLARE @proc_end_time      DATETIME;  -- Entire procedure end

    BEGIN TRY
        -- Procedure start
        SET @proc_start_time = GETDATE();
        PRINT '=================================================';
        PRINT '[PROCEDURE STARTED] bronze.load_bronze_ch1';
		    PRINT '=================================================';

        -- Bronze Layer Load start
        SET @load_start_time = GETDATE();
		    PRINT '---------------------------';
        PRINT '[START] Bronze Layer Load';
		    PRINT '---------------------------';


        --------------------------------------
        -- CRM: Customer Info
        --------------------------------------
        SET @start_time = GETDATE();

        PRINT '>> Loading: bronze.crm_cust_info';

		    PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

		    PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\user\Downloads\datasets\source_crm\cust_info.csv'
        WITH (
      				FIRSTROW = 2,
      				FIELDTERMINATOR = ',',
      				TABLOCK
			      );

        SET @end_time = GETDATE();

		    PRINT '>> Loaded: bronze.crm_cust_info';

	    	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	    	PRINT '--------------------------------------------------------------';

        --------------------------------------
        -- CRM: Product Info
        --------------------------------------
        SET @start_time = GETDATE();

        PRINT '>> Loading: bronze.crm_prd_info';

		    PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

		    PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\user\Downloads\datasets\source_crm\prd_info.csv'
        WITH (
        				FIRSTROW = 2,
        				FIELDTERMINATOR = ',',
        				TABLOCK
            );

        SET @end_time = GETDATE();
    		PRINT '>> Loaded: bronze.crm_prd_info';
    		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------';

        --------------------------------------
        -- CRM: Sales Details
        --------------------------------------
        SET @start_time = GETDATE();

        PRINT '>> Loading: bronze.crm_sales_details';

		    PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

		    PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\user\Downloads\datasets\source_crm\sales_details.csv'
        WITH (
        				FIRSTROW = 2, 
        				FIELDTERMINATOR = ',',
        				TABLOCK
	    		);
        SET @end_time = GETDATE();
		    PRINT '>> Loading: bronze.crm_sales_details';
		    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------';

        --------------------------------------
        -- ERP: Location Info
        --------------------------------------
        SET @start_time = GETDATE();

        PRINT '>> Loading: bronze.erp_loc_a101';

		    PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

		    PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\user\Downloads\datasets\source_erp\LOC_A101.csv'
        WITH (
        				FIRSTROW = 2,
        				FIELDTERMINATOR = ',',
        				TABLOCK
    			);
        SET @end_time = GETDATE();
		    PRINT '>> Loaded: bronze.erp_loc_a101';
		    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------';

        --------------------------------------
        -- ERP: Customer Info
        --------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Loading: bronze.erp_cust_az12';

		    PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

		    PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\user\Downloads\datasets\source_erp\CUST_AZ12.csv'
        WITH (
      				FIRSTROW = 2,
      				FIELDTERMINATOR = ',',
      				TABLOCK  
	    		);

        SET @end_time = GETDATE();
	    	PRINT '>> Loaded: bronze.erp_cust_az12';
	    	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------';

        --------------------------------------
        -- ERP: Product Category
        --------------------------------------
        SET @start_time = GETDATE();

        PRINT '>> Loading: bronze.erp_px_cat_g1v2';

		    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		    PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\user\Downloads\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (	
      				FIRSTROW = 2,
      				FIELDTERMINATOR = ',',
      				TABLOCK
			  );

        SET @end_time = GETDATE();
    		PRINT '>> Loaded: bronze.erp_px_cat_g1v2';
    		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';  
    		PRINT '--------------------------------------------------------------';

        --------------------------------------
        -- Final Summary
        --------------------------------------
        SET @proc_end_time = GETDATE();
        PRINT '================= [SUCCESS] Bronze Load Completed =================';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @load_start_time, @proc_end_time) AS NVARCHAR) + ' seconds';
        PRINT 'Procedure Duration : ' + CAST(DATEDIFF(SECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==============================================================';
    END TRY

    BEGIN CATCH
        PRINT '================= [ERROR] Bronze Load Failed =================';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==============================================================';
    END CATCH
END;


