CREATE OR REPLACE PROCEDURE bronze.load_bronze () language plpgsql AS $$
DECLARE 
   start_proc_timestamp TIMESTAMP;
   end_proc_timestamp TIMESTAMP;
   start_timestamp TIMESTAMP;
   end_timestamp TIMESTAMP;
   tables_counter INT := 0;
   row_count INT;
BEGIN
    start_proc_timestamp := NOW();
    RAISE NOTICE '=============================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=============================================================';

    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE 'Loading Kaggle Tables';
    RAISE NOTICE '-------------------------------------------------------------';

    RAISE NOTICE '>> Truncatinc Table: bronze.brent_oil_prices';
    TRUNCATE TABLE bronze.brent_oil_prices;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.brent_oil_prices';
    start_timestamp := NOW();
    COPY bronze.brent_oil_prices
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_brent_oil_prices.csv' delimiter ',' csv header;
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.brent_oil_prices;

    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';



    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.canada_crude_production';
    TRUNCATE TABLE bronze.canada_crude_production;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.canada_crude_production';
    start_timestamp := NOW();
    COPY bronze.canada_crude_production
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_canada_crude_production_2016-2024_monthly.csv' delimiter ',' csv header;
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.canada_crude_production;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';


    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.canada_gas_prices';
    TRUNCATE TABLE bronze.canada_gas_prices;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.canada_gas_prices';
    start_timestamp := NOW();
    COPY bronze.canada_gas_prices
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_canada_gas_prices_1979_2023.csv' delimiter ',' csv header;
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.canada_gas_prices;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';
    

    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.crude_oil_wti_price_monthly';
    TRUNCATE TABLE bronze.crude_oil_wti_price_monthly;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.crude_oil_wti_price_monthly';
    start_timestamp := NOW();
    COPY bronze.crude_oil_wti_price_monthly
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_crude_oil_wti_price_monthly.csv' delimiter ',' csv header;
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.crude_oil_wti_price_monthly;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';


    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.origin_destination_info';
    TRUNCATE TABLE bronze.origin_destination_info;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.origin_destination_info';
    start_timestamp := NOW();
    COPY bronze.origin_destination_info
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_origin_destination_info.csv' delimiter ',' csv header;
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.origin_destination_info;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';


    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.accidents';
    TRUNCATE TABLE bronze.accidents;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.accidents';
    start_timestamp := NOW();
    COPY bronze.accidents
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_accidents.csv' delimiter ',' csv header;
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.accidents;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';


    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.all_info_1932_2014';
    TRUNCATE TABLE bronze.all_info_1932_2014;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.all_info_1932_2014';
    start_timestamp := NOW();
    COPY bronze.all_info_1932_2014
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_all_info_1932_2014.csv' delimiter ',' csv header;
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.all_info_1932_2014;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';


    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.oil_gas_prices';
    TRUNCATE TABLE bronze.oil_gas_prices;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.oil_gas_prices';
    start_timestamp := NOW();
    COPY bronze.oil_gas_prices
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_oil_gas_prices.csv' delimiter ',' csv header;
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.oil_gas_prices;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';


    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.oil_consumption_by_country';
    TRUNCATE TABLE bronze.oil_consumption_by_country;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.oil_consumption_by_country';
    start_timestamp := NOW();
    COPY bronze.oil_consumption_by_country
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_oil_consumption_by_country_1965_2023.csv' delimiter ',' csv header NULL 'NA';
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.oil_consumption_by_country;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';
    

    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.world_crude_oil_reserves_1995_2021';
    TRUNCATE TABLE bronze.world_crude_oil_reserves_1995_2021;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.world_crude_oil_reserves_1995_2021';
    start_timestamp := NOW();
    COPY bronze.world_crude_oil_reserves_1995_2021
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_world_crude_oil_reserves_1995_2021.csv' delimiter ',' csv header;
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.world_crude_oil_reserves_1995_2021;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';


    RAISE NOTICE '-------------------------------------------------------------';
    RAISE NOTICE '>> Truncatinc Table: bronze.global_inflation';
    TRUNCATE TABLE bronze.global_inflation;
    RAISE NOTICE '>> Inserting Data Into Table: bronze.global_inflation';
    start_timestamp := NOW();
    COPY bronze.global_inflation
    FROM
        'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_global_inflation.csv' 
     WITH
    (
        format csv,
        header TRUE,
        delimiter ',',
        encoding 'WIN1252'
    );
    end_timestamp := NOW();
    SELECT COUNT(*) INTO row_count FROM bronze.global_inflation;
    RAISE NOTICE '>> Inserting Finished Succesfully. Loading Time: % seconds. Inserted % rows', 
        round(Extract(second from end_timestamp-start_timestamp), 0),
        row_count;
    tables_counter = tables_counter + 1;
    RAISE NOTICE '-------------------------------------------------------------';
    

    end_proc_timestamp := NOW();
    RAISE NOTICE '=============================================================';
    RAISE NOTICE 'Loading Bronze Layer Finished Succesfully. Loading Time: % seconds. Processed % tables', 
        round(Extract(second from end_proc_timestamp-start_proc_timestamp), 0),
        tables_counter;
    RAISE NOTICE '=============================================================';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '----->>>>>Error loading file #%', tables_counter+1;
            RETURN;
END;
$$;

CALL bronze.load_bronze ()