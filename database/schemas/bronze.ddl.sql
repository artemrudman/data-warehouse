/*
===============================================================================
PSQL Script: Create database and reconnect
===============================================================================
DROP DATABASE IF EXISTS oilgasproject;
CREATE DATABASE oilgasproject;
\connect oilgasproject
*/;

/*
===============================================================================
Create Layers (Schemas)
===============================================================================
*/;

CREATE SCHEMA if NOT EXISTS bronze;

CREATE SCHEMA if NOT EXISTS silver;

CREATE SCHEMA if NOT EXISTS gold;

/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
This script creates tables in the 'bronze' schema, dropping existing tables 
if they already exist.
Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/;

DROP TABLE IF EXISTS bronze.brent_oil_prices;

CREATE TABLE bronze.brent_oil_prices (
    related_date date,
    price float
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.canada_crude_production;

CREATE TABLE bronze.canada_crude_production (
    prod_date date,
    crude_m3 text,
    crude_bbl text,
    wcs_price float,
    wti_price float
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.canada_gas_prices;

CREATE TABLE bronze.canada_gas_prices (
    date text,
    city text,
    province text,
    fuel text,
    service text,
    octane text,
    cents_per_litre float
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.crude_oil_wti_price_monthly;

CREATE TABLE bronze.crude_oil_wti_price_monthly (
    cop_date date,
    price float,
    percent_change float,
    change float
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.origin_destination_info;

CREATE TABLE bronze.origin_destination_info (
    "year" int,
    "month" int,
    origin_name text,
    origin_name_type text,
    destination_name text,
    destination_name_type text,
    grade_name text,
    quantity int
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.accidents;

CREATE TABLE bronze.accidents (
    report_number bigint,
    supplemental_number int,
    accident_year int,
    accident_date_time date,
    operatot_id text,
    operator_name text,
    pipeline_facility_name text,
    pipeline_location text,
    pipeline_type text,
    liquid_type text,
    liquid_subtype text,
    liquid_name text,
    accident_city text,
    accident_county text,
    accident_state text,
    accident_latitude float,
    accident_longitude float,
    cause_category text,
    cause_subcategory text,
    unintentional_release_in_barrels float,
    intentional_release_in_barrels float,
    liquid_recovery_in_barrels float,
    net_loss_in_barrels float,
    liquid_ignition boolean,
    liquid_explosion boolean,
    pipeline_shutdown boolean,
    shutdown_date_time date,
    restart_date_time date,
    public_evacuations_in_people int,
    property_damage_costs int,
    lost_commodity_costs int,
    private_property_damage_costs int,
    emergency_response_costs int,
    environmental_remediation_costs int,
    other_costs int,
    all_costs int
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.all_info_1932_2014;

CREATE TABLE bronze.all_info_1932_2014 (
    cty_name text,
    iso3numeric int,
    id text,
    YEAR int,
    eiacty text,
    oil_prod32_14 float,
    oil_price_2000 float,
    oil_price_nom float,
    oil_value_nom float,
    oil_value_2000 float,
    oil_value_2014 float,
    gas_prod55_14 float,
    gas_price_2000_mboe float,
    gas_price_2000 float,
    gas_price_nom float,
    gas_value_nom float,
    gas_value_2000 float,
    gas_value_2014 float,
    oil_gas_value_nom float,
    oil_gas_value_2000 float,
    oil_gas_value_2014 float,
    oil_gas_valuepop_nom float,
    oil_gas_valuepop_2000 float,
    oil_gas_valuepop_2014 float,
    oil_exports float,
    net_oil_exports float,
    net_oil_exports_mt float,
    net_oil_exports_value float,
    net_oil_exports_valuepop float,
    gas_exports float,
    net_gas_exports_bcf float,
    net_gas_exports_mboe float,
    net_gas_exports_value float,
    net_gas_exports_valuepop float,
    net_oil_gas_exports_valuepop float,
    population float,
    pop_maddison float,
    sovereign boolean,
    mult_nom_2000 float,
    mult_nom_2014 float,
    mult_2000_2014 float
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.oil_gas_prices;

CREATE TABLE bronze.oil_gas_prices (
    symbol text,
    "date" date,
    "open" float,
    high float,
    low float,
    "close" float,
    volume float, --contracts_volume_1000barrels
    currency text
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.oil_consumption_by_country;

CREATE TABLE bronze.oil_consumption_by_country (
    entity text NOT NULL,
    "1965" numeric,
    "1966" numeric,
    "1967" numeric,
    "1968" numeric,
    "1969" numeric,
    "1970" numeric,
    "1971" numeric,
    "1972" numeric,
    "1973" numeric,
    "1974" numeric,
    "1975" numeric,
    "1976" numeric,
    "1977" numeric,
    "1978" numeric,
    "1979" numeric,
    "1980" numeric,
    "1981" numeric,
    "1982" numeric,
    "1983" numeric,
    "1984" numeric,
    "1985" numeric,
    "1986" numeric,
    "1987" numeric,
    "1988" numeric,
    "1989" numeric,
    "1990" numeric,
    "1991" numeric,
    "1992" numeric,
    "1993" numeric,
    "1994" numeric,
    "1995" numeric,
    "1996" numeric,
    "1997" numeric,
    "1998" numeric,
    "1999" numeric,
    "2000" numeric,
    "2001" numeric,
    "2002" numeric,
    "2003" numeric,
    "2004" numeric,
    "2005" numeric,
    "2006" numeric,
    "2007" numeric,
    "2008" numeric,
    "2009" numeric,
    "2010" numeric,
    "2011" numeric,
    "2012" numeric,
    "2013" numeric,
    "2014" numeric,
    "2015" numeric,
    "2016" numeric,
    "2017" numeric,
    "2018" numeric,
    "2019" numeric,
    "2020" numeric,
    "2021" numeric,
    "2022" numeric,
    "2023" numeric
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.world_crude_oil_reserves_1995_2021;

CREATE TABLE bronze.world_crude_oil_reserves_1995_2021 (
    entity text NOT NULL,
    "1995" numeric,
    "1996" numeric,
    "1997" numeric,
    "1998" numeric,
    "1999" numeric,
    "2000" numeric,
    "2001" numeric,
    "2002" numeric,
    "2003" numeric,
    "2004" numeric,
    "2005" numeric,
    "2006" numeric,
    "2007" numeric,
    "2008" numeric,
    "2009" numeric,
    "2010" numeric,
    "2011" numeric,
    "2012" numeric,
    "2013" numeric,
    "2014" numeric,
    "2015" numeric,
    "2016" numeric,
    "2017" numeric,
    "2018" numeric,
    "2019" numeric,
    "2020" numeric,
    "2021" numeric
);

-------------------------------------------------------
DROP TABLE IF EXISTS bronze.global_inflation;

CREATE TABLE bronze.global_inflation (
    country_code text,
    imf_country_code numeric,
    country text,
    indicator_type text,
    series_name text,
    "1970" numeric,
    "1971" numeric,
    "1972" numeric,
    "1973" numeric,
    "1974" numeric,
    "1975" numeric,
    "1976" numeric,
    "1977" numeric,
    "1978" numeric,
    "1979" numeric,
    "1980" numeric,
    "1981" numeric,
    "1982" numeric,
    "1983" numeric,
    "1984" numeric,
    "1985" numeric,
    "1986" numeric,
    "1987" numeric,
    "1988" numeric,
    "1989" numeric,
    "1990" numeric,
    "1991" numeric,
    "1992" numeric,
    "1993" numeric,
    "1994" numeric,
    "1995" numeric,
    "1996" numeric,
    "1997" numeric,
    "1998" numeric,
    "1999" numeric,
    "2000" numeric,
    "2001" numeric,
    "2002" numeric,
    "2003" numeric,
    "2004" numeric,
    "2005" numeric,
    "2006" numeric,
    "2007" numeric,
    "2008" numeric,
    "2009" numeric,
    "2010" numeric,
    "2011" numeric,
    "2012" numeric,
    "2013" numeric,
    "2014" numeric,
    "2015" numeric,
    "2016" numeric,
    "2017" numeric,
    "2018" numeric,
    "2019" numeric,
    "2020" numeric,
    "2021" numeric,
    "2022" numeric,
    note text
);

-------------------------------------------------------
;

-------------------------------------------------------
TRUNCATE TABLE bronze.brent_oil_prices;

COPY bronze.brent_oil_prices
FROM
    'D:\oil_gas_project\src\csv\2025-11-10_kaggle_bi_brent_oil_prices.csv' delimiter ',' csv header;

SELECT
    COUNT(*)
FROM
    bronze.brent_oil_prices;

COPY inflation_info
FROM
    'D:\Download\Oil&Gas_Project\Global Dataset of Inflation.csv'
WITH
    (
        format csv,
        header TRUE,
        delimiter ',',
        encoding 'WIN1252' -- or 'LATIN1' if that’s what it is
    );

SELECT
    *
FROM
    inflation_info;