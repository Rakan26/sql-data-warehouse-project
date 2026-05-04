EXEC bronze.load_bronze
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY 
        SET @batch_start_time = GETDATE(); 

        PRINT '==================================================';
        PRINT '              Bronze Layer Loading                ';
        PRINT '==================================================';


        PRINT '=============================';
        PRINT '>> Loading: crm_cust_info...';
        PRINT '=============================';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Admin\\datasets\source_crm\cust_info.csv'
        WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '  Loaded in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        print''

        PRINT '=============================';
        PRINT '>> Loading: crm_prd_info...';
        PRINT '=============================';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Admin\\datasets\source_crm\prd_info.csv'
        WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '  Loaded in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
                print''

        PRINT '=============================';
        PRINT '>> Loading: crm_sales_details...';
        PRINT '=============================';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Admin\\datasets\source_crm\sales_details.csv'
        WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '  Loaded in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
                print''

        PRINT '=============================';
        PRINT '>> Loading: erp_CUST_AZ12...';
        PRINT '=============================';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_CUST_AZ12;
        BULK INSERT bronze.erp_CUST_AZ12
        FROM 'C:\Users\Admin\\datasets\source_crm\CUST_AZ12.csv'
        WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '  Loaded in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
         print''

        PRINT '=============================';
        PRINT '>> Loading: erp_LOC_A101...';
        PRINT '=============================';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_LOC_A101;
        BULK INSERT bronze.erp_LOC_A101
        FROM 'C:\Users\Admin\\datasets\source_crm\LOC_A101.csv'
        WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '  Loaded in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
                print''

        PRINT '=============================';
        PRINT '>> Loading: erp_PX_CAT_G1V2...';
        PRINT '=============================';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM 'C:\Users\Admin\\datasets\source_crm\PX_CAT_G1V2.csv'
        WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '  Loaded in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
                print''

        SET @batch_end_time = GETDATE(); 

        PRINT '==================================================';
        PRINT '        Bronze Layer Loading Completed            ';
        PRINT '==================================================';
        PRINT ' Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
        

       

    END TRY   
    BEGIN CATCH
        PRINT '==================================================';
        PRINT '   ERROR OCCURRED DURING LOADING BRONZE LAYER    ';
        PRINT '==================================================';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
   
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        
    END CATCH
END;
