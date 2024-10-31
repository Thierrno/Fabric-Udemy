

#########################################      NB_Bronze_To_Silver_Transformations_Python                ######################################################################################################################
# Bronze To Siler Transformations
#This notebook performs transformations on data from Bronze and saves the transformed data to the Silver Lakehouse

from pyspark.sql.functions import round, col, dayofmonth, month, year, to_date, quarter, substring, when, regexp_replace



# Define the path to thetable wind_power_production bronze Lakehouse
bronze_table_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/wind_power_production"

#Load the wind_power_production table into a Dataframe
df = spark.read.format("delta").load(bronze_table_path)



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



# Path to the wind_power_production table in the Silver Lakehouse
silver_table_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables/wind_power_production"

# Save the transformed table in the Silver Lakehouse
df_transformed.write.format("delta").mode("overwrite").save(silver_table_path)



#########################################      NB_Silver_To_Gold_Transformations_Python                ######################################################################################################################
# Siler To Gold Transformations

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

########################

# Define the path to the wind_power_production table in the Silver Lakehouse
silver_table_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables/wind_power_production"

# Load the wind_power_production table
df = spark.read.format("delta").load(silver_table_path)

#######################################################################################################################################################################################
# Create the Date Dimension table
date_dim = df.select("date", "day", "month", "quarter", "year").distinct() \
             .withColumnRenamed("date", "date_id")

# Create the Time Dimension table
time_dim = df.select("time", "hour_of_day", "minute_of_hour", "seconde_of_minutes", "time_period").distinct() \
             .withColumnRenamed("time", "time_id")

# Create the Turbine Dimension table
turbine_dim = df.select("turbine_name", "capacity", "location_name", "latitude", "longitude", "region").distinct() \
                .withColumn("turbine_id", row_number().over(Window.orderBy("turbine_name", "capacity", "location_name", "latitude", "longitude", "region")))

# Create the Operational Status Dimension table
operational_status_dim = df.select("status", "responsible_department").distinct() \
                .withColumn("status_id", row_number().over(Window.orderBy("status", "responsible_department")))
                
#######################################################################################################################################################################################         
# Join the dimension tables to the original DataFrame
df = df.join(turbine_dim, ["turbine_name", "capacity", "location_name", "latitude", "longitude", "region"], "left") \
        .join(operational_status_dim, ["status", "responsible_department"], "left")
        
#######################################################################################################################################################################################
# Create the Fact table
fact_table = df.select("production_id", "date", "time", "turbine_id", "status_id", "wind_speed", "wind_direction", "energy_produced") \
                .withColumnRenamed("date", "date_id").withColumnRenamed("time", "time_id")
                
                
#######################################################################################################################################################################################
# Define the paths to the Gold tables
# You may need to change the paths to match your workspace
gold_date_dim_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Gold.Lakehouse/Tables/dim_date"
gold_time_dim_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Gold.Lakehouse/Tables/dim_time"
gold_turbine_dim_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Gold.Lakehouse/Tables/dim_turbine"
gold_operational_status_dim_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Gold.Lakehouse/Tables/dim_operational_status"
gold_fact_table_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Gold.Lakehouse/Tables/fact_wind_power_production"

# Save the tables in the Gold Lakehouse
date_dim.write.format("delta").mode("overwrite").save(gold_date_dim_path)
time_dim.write.format("delta").mode("overwrite").save(gold_time_dim_path)
turbine_dim.write.format("delta").mode("overwrite").save(gold_turbine_dim_path)
operational_status_dim.write.format("delta").mode("overwrite").save(gold_operational_status_dim_path)
fact_table.write.format("delta").mode("overwrite").save(gold_fact_table_path) 


#########################################      NB_Get_Daily_Data_Python                  ######################################################################################################################


