{"cells":[{"cell_type":"markdown","source":["# Bronze To Silver Transformations\n","\n","This notebook perfoms transformations on data from the Bronze Lakehouse and saves the transformed data to the Silver Lakehouse."],"metadata":{"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"13e12a2a-5748-44ba-b274-b616862613ec"},{"cell_type":"code","source":["from pyspark.sql.functions import round, col, dayofmonth, month, year, to_date, quarter, substring, when, regexp_replace"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":3,"statement_ids":[3],"state":"finished","livy_statement_state":"available","session_id":"7928a370-332b-4b55-ba91-1a50c128d337","normalized_state":"finished","queued_time":"2024-08-23T15:56:25.9370458Z","session_start_time":"2024-08-23T15:56:26.2962985Z","execution_start_time":"2024-08-23T15:56:36.5860624Z","execution_finish_time":"2024-08-23T15:56:39.6366061Z","parent_msg_id":"310999a8-2cf6-4785-a51d-d77a9e6b6b53"},"text/plain":"StatementMeta(, 7928a370-332b-4b55-ba91-1a50c128d337, 3, Finished, Available, Finished)"},"metadata":{}}],"execution_count":1,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"e0b87928-1229-48e9-b1b6-992a59c70fa4"},{"cell_type":"code","source":["# Define the path to the wind_power_production table in the Bronze Lakehouse\n","# You may need to change the path to match your workspace\n","bronze_table_path = \"abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/wind_power_production\"\n","\n","# Load the wind_power_production table into a DataFrame\n","df = spark.read.format(\"delta\").load(bronze_table_path)"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":4,"statement_ids":[4],"state":"finished","livy_statement_state":"available","session_id":"7928a370-332b-4b55-ba91-1a50c128d337","normalized_state":"finished","queued_time":"2024-08-23T15:56:25.9381578Z","session_start_time":null,"execution_start_time":"2024-08-23T15:56:40.166102Z","execution_finish_time":"2024-08-23T15:56:42.0173197Z","parent_msg_id":"a154ba27-6f63-44e0-9022-027c064c0eee"},"text/plain":"StatementMeta(, 7928a370-332b-4b55-ba91-1a50c128d337, 4, Finished, Available, Finished)"},"metadata":{}}],"execution_count":2,"metadata":{"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"28939e7f-3e92-40fc-bc79-9f479dcf8b2b"},{"cell_type":"code","source":["# Clean and enrich data\n","df_transformed = (df\n","    .withColumn(\"wind_speed\", round(col(\"wind_speed\"), 2))\n","    .withColumn(\"energy_produced\", round(col(\"energy_produced\"), 2))\n","    .withColumn(\"day\", dayofmonth(col(\"date\")))\n","    .withColumn(\"month\", month(col(\"date\")))\n","    .withColumn(\"quarter\", quarter(col(\"date\")))\n","    .withColumn(\"year\", year(col(\"date\")))\n","    .withColumn(\"time\", regexp_replace(col(\"time\"), \"-\", \":\"))\n","    .withColumn(\"hour_of_day\", substring(col(\"time\"), 1, 2).cast(\"int\"))\n","    .withColumn(\"minute_of_hour\", substring(col(\"time\"), 4, 2).cast(\"int\"))\n","    .withColumn(\"second_of_minute\", substring(col(\"time\"), 7, 2).cast(\"int\"))\n","    .withColumn(\"time_period\", when((col(\"hour_of_day\") >= 5 ) & (col(\"hour_of_day\") < 12), \"Morning\")\n","                                .when((col(\"hour_of_day\") >= 12 ) & (col(\"hour_of_day\") < 17), \"Afternoon\")\n","                                .when((col(\"hour_of_day\") >= 17 ) & (col(\"hour_of_day\") < 21), \"Evening\")\n","                                .otherwise(\"Night\")\n","    )\n",")"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":5,"statement_ids":[5],"state":"finished","livy_statement_state":"available","session_id":"7928a370-332b-4b55-ba91-1a50c128d337","normalized_state":"finished","queued_time":"2024-08-23T15:56:25.939633Z","session_start_time":null,"execution_start_time":"2024-08-23T15:56:42.5249956Z","execution_finish_time":"2024-08-23T15:56:42.8885472Z","parent_msg_id":"7080790c-242f-491f-8b4a-d296be6aa282"},"text/plain":"StatementMeta(, 7928a370-332b-4b55-ba91-1a50c128d337, 5, Finished, Available, Finished)"},"metadata":{}}],"execution_count":3,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"642db9c1-92b0-459c-99db-152c2d11ba1f"},{"cell_type":"code","source":["# Define the path to the wind_power_production table in the Silver Lakehouse\n","# You may need to change the path to match your workspace\n","silver_table_path = \"abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables/wind_power_production\"\n","\n","# Save the transformed wind_power_production table in the Silver Lakehouse\n","df_transformed.write.format(\"delta\").mode(\"overwrite\").save(silver_table_path)"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":6,"statement_ids":[6],"state":"finished","livy_statement_state":"available","session_id":"7928a370-332b-4b55-ba91-1a50c128d337","normalized_state":"finished","queued_time":"2024-08-23T15:56:25.9406465Z","session_start_time":null,"execution_start_time":"2024-08-23T15:56:43.3881352Z","execution_finish_time":"2024-08-23T15:56:48.9418374Z","parent_msg_id":"b85caf8b-f57d-4bef-b39e-31fc47db7ea9"},"text/plain":"StatementMeta(, 7928a370-332b-4b55-ba91-1a50c128d337, 6, Finished, Available, Finished)"},"metadata":{}}],"execution_count":4,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"508cc457-9ae5-4e80-8c9c-5e6a21e9f731"}],"metadata":{"kernel_info":{"name":"synapse_pyspark"},"kernelspec":{"name":"synapse_pyspark","language":"Python","display_name":"Synapse PySpark"},"language_info":{"name":"python"},"microsoft":{"language":"python","language_group":"synapse_pyspark","ms_spell_check":{"ms_spell_check_language":"en"}},"widgets":{},"nteract":{"version":"nteract-front-end@1.0.0"},"synapse_widget":{"version":"0.1","state":{}},"spark_compute":{"compute_id":"/trident/default"},"dependencies":{"lakehouse":{"default_lakehouse":"1367568c-68c3-4c83-a404-7d9da94a2412","default_lakehouse_name":"LH_Bronze","default_lakehouse_workspace_id":"bfcb8e41-2b70-4eaf-8e07-1ea42aafff4f","known_lakehouses":[{"id":"1367568c-68c3-4c83-a404-7d9da94a2412"},{"id":"fbc6646f-5e35-4dc8-9524-e05d51e7f330"}]}}},"nbformat":4,"nbformat_minor":5}


from pyspark.sql.functions import round, col, dayofmonth, month, year, to_date, quarter, substring, when, regexp_replace

#----------------------------------
# Define the path to thetable wind_power_production bronze Lakehouse
bronze_table_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/wind_power_production"

#Load the wind_power_production table into a Dataframe
df = spark.read.format("delta").load(bronze_table_path)
#----------------------------------

# Clean and enrich data
df_transformed = (df
    .withColumn("wind_speed", round(col("wind_speed"), 2))
    .withColumn("energy_produced", round(col("energy_produced"), 2))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("quarter", quarter(col("date")))
    .withColumn("year", year(col("date")))
    .withColumn("time", regexp_replace(col("time"), "-", ":"))
    .withColumn("hour_of_day", substring(col("time"), 1, 2).cast("int"))
    .withColumn("minute_of_hour", substring(col("time"), 4, 2).cast("int"))
    .withColumn("seconde_of_minutes", substring(col("time"), 7, 2).cast("int"))
    .withColumn("time_period", when((col("hour_of_day") >= 5) & (col("hour_of_day") < 12), "Morning")
                                .when((col("hour_of_day") >= 12) & (col("hour_of_day") < 17), "Afternoon")
                                .when((col("hour_of_day") >= 17) & (col("hour_of_day") < 21), "Evening")
                                .otherwise("Nigth")
    )
)

#----------------------------------

# Path to the wind_power_production table in the Silver Lakehouse
silver_table_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables/wind_power_production"

# Save the transformed table in the Silver Lakehouse
df_transformed.write.format("delta").mode("overwrite").save(silver_table_path)
#----------------------------------


#----------------------------------

#----------------------------------

#----------------------------------

#----------------------------------