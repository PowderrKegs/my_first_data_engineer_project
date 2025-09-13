/*
=========================================================================================================
Function: bronze.load_bronze()
=========================================================================================================
Purpose:
    This function loads raw data into the 'bronze' schema.
    It performs the following actions:
    - Truncates tables in the bronze schema to ensure a clean state.
    - Uses the PostgreSQL COPY command to load data from CSV files into bronze tables.
    - Logs progress and execution times for each table and the overall process using RAISE NOTICE.
    - Includes error handling to capture and report issues during execution.

Usage:
    Execute with: SELECT bronze.load_bronze();
    Ensure CSV files are accessible at the specified paths and the PostgreSQL server has read permissions.
    Check logs in the VS Code PostgreSQL extension Output panel (set client_min_messages TO NOTICE) or psql.
*/

CREATE OR REPLACE FUNCTION bronze.load_bronze()
RETURNS void AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    total_start_time TIMESTAMP;
    total_end_time TIMESTAMP;
BEGIN
    -- Capture total start time
    total_start_time := CURRENT_TIMESTAMP;

    -- Logging start of the procedure
    RAISE NOTICE '=====================================================================================';
    RAISE NOTICE 'Loading Bronze Layer';

    RAISE NOTICE '-------------------------------------------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '-------------------------------------------------------------------------------------';

    -- Truncate and load crm_cust_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM 'D:/Data Engineer/First DE Project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (DELIMITER ',', FORMAT CSV, HEADER);
    end_time :=CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration for crm_cust_info: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '------------------------------------------';

    -- Truncate and load crm_prd_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM 'D:/Data Engineer/First DE Project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (DELIMITER ',', FORMAT CSV, HEADER);
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration for crm_prd_info: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '------------------------------------------';

    -- Truncate and load crm_sales_details
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM 'D:/Data Engineer/First DE Project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (DELIMITER ',', FORMAT CSV, HEADER);
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration for crm_sales_details: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '------------------------------------------';

    -- Truncate and load erp_cust_az12
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM 'D:/Data Engineer/First DE Project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (DELIMITER ',', FORMAT CSV, HEADER);
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration for erp_cust_az12: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '------------------------------------------';

    -- Truncate and load erp_loc_a101
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM 'D:/Data Engineer/First DE Project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (DELIMITER ',', FORMAT CSV, HEADER);
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration for erp_loc_a101: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '------------------------------------------';

    -- Truncate and load erp_px_cat_g1v2
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM 'D:/Data Engineer/First DE Project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (DELIMITER ',', FORMAT CSV, HEADER);
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration for erp_px_cat_g1v2: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '------------------------------------------';

    -- Log total execution time
    total_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Function Completed at: %', total_end_time;
    RAISE NOTICE 'Total Execution Time: % seconds', EXTRACT(EPOCH FROM (total_end_time - total_start_time));
    RAISE NOTICE '=====================================================================================';

EXCEPTION
    WHEN OTHERS THEN
        -- Log error details
        RAISE NOTICE 'Error occurred: %', SQLERRM;
        RAISE EXCEPTION 'Function failed: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;
