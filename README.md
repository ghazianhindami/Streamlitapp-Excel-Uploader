CREATE DATABASE excel_uploader_db;

CREATE TABLE excel_report_oleic (
    section VARCHAR(12),
    product VARCHAR(30),
    activity VARCHAR(12) UNIQUE,
    production_dates DATE,
    shift_value FLOAT8,
    shift_category VARCHAR(12),
    downtime_value float8,
    tangki_value VARCHAR(12),
    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
);

ALTER TABLE excel_report_oleic
DROP CONSTRAINT excel_report_oleic_activity_key;

CREATE TABLE excel_report_fattyacid (
    section VARCHAR(12),
    product VARCHAR(30),
    activity VARCHAR(12) UNIQUE,
    production_dates DATE,
    shift_value FLOAT8,
    shift_category VARCHAR(12),
    downtime_value float8,
    tangki_value VARCHAR(12),
    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
);

ALTER TABLE excel_report_fattyacid
DROP CONSTRAINT excel_report_fattyacid_activity_key;

silahkan membuat docker container dengan cara 

"docker build -t excel-uploader-streamlit ."dp

