{"cells":[{"cell_type":"markdown","source":["# Bronze To Silver Transformations\n","\n","This notebook perfoms transformations on data from the Bronze Lakehouse and saves the transformed data to the Silver Lakehouse."],"metadata":{"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"586a3bf4-b1d0-45e7-98ac-31d58d90f810"},{"cell_type":"code","source":["%%sql\n","-- Create a temporary view of the wind_power_production table\n","CREATE OR REPLACE TEMPORARY VIEW bronze_wind_power_production AS\n","SELECT *\n","FROM LH_Bronze.wind_power_production"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":15,"statement_ids":[15],"state":"finished","livy_statement_state":"available","session_id":"c7c0a73a-a155-4d32-bbce-50b71e64eac4","normalized_state":"finished","queued_time":"2024-08-22T14:23:33.5505527Z","session_start_time":null,"execution_start_time":"2024-08-22T14:23:34.2201764Z","execution_finish_time":"2024-08-22T14:23:35.18679Z","parent_msg_id":"cae3cfd6-0318-47cb-855e-6e00de393666"},"text/plain":"StatementMeta(, c7c0a73a-a155-4d32-bbce-50b71e64eac4, 15, Finished, Available, Finished)"},"metadata":{}},{"output_type":"execute_result","execution_count":14,"data":{"application/vnd.synapse.sparksql-result+json":{"schema":{"type":"struct","fields":[]},"data":[]},"text/plain":"<Spark SQL result set with 0 rows and 0 fields>"},"metadata":{}}],"execution_count":14,"metadata":{"microsoft":{"language":"sparksql","language_group":"synapse_pyspark"},"collapsed":false},"id":"343fd45e-749b-4d9a-b60d-b8c266101ea6"},{"cell_type":"code","source":["%%sql\n","-- Clean and enrich data\n","CREATE OR REPLACE TEMPORARY VIEW transformed_wind_power_production AS\n","SELECT\n","    production_id,\n","    date,\n","    turbine_name,\n","    capacity,\n","    location_name,\n","    latitude,\n","    longitude,\n","    region,\n","    status,\n","    responsible_department,\n","    wind_direction,\n","    ROUND(wind_speed, 2) AS wind_speed,\n","    ROUND(energy_produced, 2) AS energy_produced,\n","    DAY(date) AS day,\n","    MONTH(date) AS month,\n","    QUARTER(date) AS quarter,\n","    YEAR(date) as year,\n","    REGEXP_REPLACE(time, '-', ':') AS time,\n","    CAST(SUBSTRING(time, 1, 2) AS INT) AS hour_of_day,\n","    CAST(SUBSTRING(time, 4, 2) AS INT) AS minute_of_hour,\n","    CAST(SUBSTRING(time, 7, 2) AS INT) AS second_of_minute,\n","    CASE\n","        WHEN CAST(SUBSTRING(time, 1, 2) AS INT) BETWEEN 5 AND 11 THEN 'Morning'\n","        WHEN CAST(SUBSTRING(time, 1, 2) AS INT) BETWEEN 12 AND 16 THEN 'Afternoon'\n","        WHEN CAST(SUBSTRING(time, 1, 2) AS INT) BETWEEN 17 AND 20 THEN 'Evening'\n","        ELSE 'Night'\n","    END AS time_period\n","FROM bronze_wind_power_production;"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":26,"statement_ids":[26],"state":"finished","livy_statement_state":"available","session_id":"c7c0a73a-a155-4d32-bbce-50b71e64eac4","normalized_state":"finished","queued_time":"2024-08-22T14:42:24.4955545Z","session_start_time":null,"execution_start_time":"2024-08-22T14:42:25.0113188Z","execution_finish_time":"2024-08-22T14:42:26.0157175Z","parent_msg_id":"cecaaab0-08d7-4791-b345-e7552a0d6da3"},"text/plain":"StatementMeta(, c7c0a73a-a155-4d32-bbce-50b71e64eac4, 26, Finished, Available, Finished)"},"metadata":{}},{"output_type":"execute_result","execution_count":25,"data":{"application/vnd.synapse.sparksql-result+json":{"schema":{"type":"struct","fields":[]},"data":[]},"text/plain":"<Spark SQL result set with 0 rows and 0 fields>"},"metadata":{}}],"execution_count":25,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"sparksql","language_group":"synapse_pyspark"},"collapsed":false},"id":"5a4607c8-2834-43f6-92ba-eaef603aae1e"},{"cell_type":"code","source":["%%sql\n","-- Drop the wind_power_production table in Silver Lakehouse if it exists\n","DROP TABLE IF EXISTS LH_Silver.wind_power_production;"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":31,"statement_ids":[31],"state":"finished","livy_statement_state":"available","session_id":"c7c0a73a-a155-4d32-bbce-50b71e64eac4","normalized_state":"finished","queued_time":"2024-08-22T14:53:15.6101745Z","session_start_time":null,"execution_start_time":"2024-08-22T14:53:16.1299999Z","execution_finish_time":"2024-08-22T14:53:18.693663Z","parent_msg_id":"0b10ff63-73f7-4bd2-bd70-5a04eb38472b"},"text/plain":"StatementMeta(, c7c0a73a-a155-4d32-bbce-50b71e64eac4, 31, Finished, Available, Finished)"},"metadata":{}},{"output_type":"execute_result","execution_count":30,"data":{"application/vnd.synapse.sparksql-result+json":{"schema":{"type":"struct","fields":[]},"data":[]},"text/plain":"<Spark SQL result set with 0 rows and 0 fields>"},"metadata":{}}],"execution_count":30,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"sparksql","language_group":"synapse_pyspark"},"collapsed":false},"id":"41b1ead2-e539-484d-922b-3868dc8b69e9"},{"cell_type":"code","source":["%%sql\n","-- Create the new wind_power_production table in Silver Lakehouse\n","CREATE TABLE LH_Silver.wind_power_production\n","USING delta\n","AS\n","SELECT * FROM transformed_wind_power_production"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":32,"statement_ids":[32],"state":"finished","livy_statement_state":"available","session_id":"c7c0a73a-a155-4d32-bbce-50b71e64eac4","normalized_state":"finished","queued_time":"2024-08-22T14:53:45.856604Z","session_start_time":null,"execution_start_time":"2024-08-22T14:53:46.3144883Z","execution_finish_time":"2024-08-22T14:53:50.0562915Z","parent_msg_id":"7e3abd97-6dcd-43ca-a743-632975e70967"},"text/plain":"StatementMeta(, c7c0a73a-a155-4d32-bbce-50b71e64eac4, 32, Finished, Available, Finished)"},"metadata":{}},{"output_type":"execute_result","execution_count":31,"data":{"application/vnd.synapse.sparksql-result+json":{"schema":{"type":"struct","fields":[]},"data":[]},"text/plain":"<Spark SQL result set with 0 rows and 0 fields>"},"metadata":{}}],"execution_count":31,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"sparksql","language_group":"synapse_pyspark"},"collapsed":false},"id":"5311154b-40f8-49b9-8b59-659b0e3fa963"}],"metadata":{"kernel_info":{"name":"synapse_pyspark"},"kernelspec":{"name":"synapse_pyspark","language":"Python","display_name":"Synapse PySpark"},"language_info":{"name":"python"},"microsoft":{"language":"python","language_group":"synapse_pyspark","ms_spell_check":{"ms_spell_check_language":"en"}},"widgets":{},"nteract":{"version":"nteract-front-end@1.0.0"},"spark_compute":{"compute_id":"/trident/default"},"dependencies":{"lakehouse":{"known_lakehouses":[{"id":"fbc6646f-5e35-4dc8-9524-e05d51e7f330"},{"id":"1367568c-68c3-4c83-a404-7d9da94a2412"}],"default_lakehouse":"fbc6646f-5e35-4dc8-9524-e05d51e7f330","default_lakehouse_name":"LH_Silver","default_lakehouse_workspace_id":"bfcb8e41-2b70-4eaf-8e07-1ea42aafff4f"}}},"nbformat":4,"nbformat_minor":5}

%%sql
-- Create a tempory view of the wind_power_production table
CREATE OR REPLACE TEMPORARY VIEW bronze_wind_power_production AS
SELECT *
FROM LH_Bronze.wind_power_production

-------------------------
%%sql
-- Clean and enrich data
CREATE OR REPLACE TEMPORARY VIEW transformed_wind_powed_production AS
SELECT
    production_id,
    date,
    turbine_name,
    capacity,
    location_name,
    latitude,
    longitude,
    region,
    status,
    responsible_department,
    wind_direction,
    ROUND(wind_speed, 2) AS wind_speed,
    ROUND(energy_produced, 2) AS energy_produced,
    DAY(date) AS day,
    MONTH(date) AS month,
    QUARTER(date) AS quarter,
    YEAR(date) AS year,
    REGEXP_REPLACE(time, '-', ':') AS time,
    CAST(SUBSTRING(time, 1, 2) AS INT) AS hour_of_day,
    CAST(SUBSTRING(time, 4, 2) AS INT) AS minute_of_hour,
    CAST(SUBSTRING(time, 7, 2) AS INT) AS second_of_minute,
    CASE
        WHEN CAST(SUBSTRING(time, 1, 2) AS INT) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN CAST(SUBSTRING(time, 1, 2) AS INT) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN CAST(SUBSTRING(time, 1, 2) AS INT) BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END AS time_period

 FROM bronze_wind_power_production; 


--------------------------

%%sql
-- drop the wind_power_production table in Silver Lakehouse if it exists
DROP TABLE IF EXISTS LH_Silver.wind_power_production;
------------------------------

%%sql
-- Create a new wind_power_production table Silver Lakehouse
CREATE TABLE LH_Silver.wind_power_production
USING delta
AS
SELECT * FROM transformed_wind_powed_production
-------------------------

--------------------------

------------------------------

