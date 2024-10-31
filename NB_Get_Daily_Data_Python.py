

{"cells":[{"cell_type":"markdown","source":["# Get Daily Data From The GitHub Repo\n","\n","This notebook retrieves daily data from the GitHub repository, starting from the last recorded date in the Bronze Lakehouse to fetch data for the subsequent day. The retrieved data is then saved back into the Bronze Lakehouse."],"metadata":{"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"d5d8c88e-8f4b-4325-939c-e433c481ebf8"},{"cell_type":"code","source":["import requests\n","import io\n","import pandas as pd\n","from datetime import timedelta"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":3,"statement_ids":[3],"state":"finished","livy_statement_state":"available","session_id":"4bc607d1-9a14-47cf-840c-e432ade02ef1","normalized_state":"finished","queued_time":"2024-09-06T08:21:33.085746Z","session_start_time":"2024-09-06T08:21:33.4543469Z","execution_start_time":"2024-09-06T08:21:45.3247358Z","execution_finish_time":"2024-09-06T08:21:48.1588661Z","parent_msg_id":"30d9acfc-2146-40d1-b1e9-f6aea9322fe1"},"text/plain":"StatementMeta(, 4bc607d1-9a14-47cf-840c-e432ade02ef1, 3, Finished, Available, Finished)"},"metadata":{}}],"execution_count":1,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"39fe395e-cd02-4e25-a593-bb1bfd9e1f1e"},{"cell_type":"code","source":["# Base URL for GitHub raw CSV files\n","base_url = \"https://raw.githubusercontent.com/mikailaltundas/datasets-for-training/main/wind-power-dataset/\"\n","\n","# Path to the wind_power_production table in the Bronze Lakehouse\n","# You may need to change the path to match your workspace\n","bronze_table_path = \"abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/wind_power_production\""],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":4,"statement_ids":[4],"state":"finished","livy_statement_state":"available","session_id":"4bc607d1-9a14-47cf-840c-e432ade02ef1","normalized_state":"finished","queued_time":"2024-09-06T08:21:33.086988Z","session_start_time":null,"execution_start_time":"2024-09-06T08:21:48.6443277Z","execution_finish_time":"2024-09-06T08:21:48.9730371Z","parent_msg_id":"4486d17f-08cb-4986-8a6c-c8169428dd1c"},"text/plain":"StatementMeta(, 4bc607d1-9a14-47cf-840c-e432ade02ef1, 4, Finished, Available, Finished)"},"metadata":{}}],"execution_count":2,"metadata":{"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"f6e5846b-e0d3-433e-832f-93f2de5067ae"},{"cell_type":"code","source":["# Load the existing wind_power_production table into a Pandas DataFrame\n","df_spark = spark.read.format(\"delta\").load(bronze_table_path)\n","df_pandas = df_spark.toPandas()"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":5,"statement_ids":[5],"state":"finished","livy_statement_state":"available","session_id":"4bc607d1-9a14-47cf-840c-e432ade02ef1","normalized_state":"finished","queued_time":"2024-09-06T08:21:33.0881296Z","session_start_time":null,"execution_start_time":"2024-09-06T08:21:49.4784725Z","execution_finish_time":"2024-09-06T08:21:55.1360401Z","parent_msg_id":"28b372a1-8c15-4c73-902f-db84cf0a0791"},"text/plain":"StatementMeta(, 4bc607d1-9a14-47cf-840c-e432ade02ef1, 5, Finished, Available, Finished)"},"metadata":{}}],"execution_count":3,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"e2afa079-63a0-40c5-852d-e6319042bcf4"},{"cell_type":"code","source":["# Get the most recent date and the next day\n","most_recent_date = pd.to_datetime(df_pandas['date'], format = '%Y%m%d').max()\n","next_date = (most_recent_date + timedelta(days = 1)).strftime('%Y%m%d')\n","\n","# Form the URL and download the next day's CSV file\n","next_file_url = f\"{base_url}{next_date}_wind_power_production.csv\"\n","file_content = requests.get(next_file_url).content\n","\n","# Load the CSV file into a Pandas Dataframe\n","df_pandas_new = pd.read_csv(io.StringIO(file_content.decode('utf-8')))\n","\n","# Convert the date column to datetime in the Pandas DataFrame\n","df_pandas_new['date'] = pd.to_datetime(df_pandas_new['date'])\n","\n","# Convert the Pandas DataFrame to a Spark DataFrame\n","df_spark_new = spark.createDataFrame(df_pandas_new, schema = df_spark.schema)"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":6,"statement_ids":[6],"state":"finished","livy_statement_state":"available","session_id":"4bc607d1-9a14-47cf-840c-e432ade02ef1","normalized_state":"finished","queued_time":"2024-09-06T08:21:33.0892526Z","session_start_time":null,"execution_start_time":"2024-09-06T08:21:55.6503186Z","execution_finish_time":"2024-09-06T08:21:56.6737231Z","parent_msg_id":"709d1199-b09f-4e88-a37e-da648bc02b54"},"text/plain":"StatementMeta(, 4bc607d1-9a14-47cf-840c-e432ade02ef1, 6, Finished, Available, Finished)"},"metadata":{}}],"execution_count":4,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"294ddef7-b3cf-4c52-bbe3-1637fb746b21"},{"cell_type":"code","source":["# Append the new data to the existing wind_power_production table in Bronze Lakehouse\n","df_spark_new.write.format(\"delta\").mode(\"append\").save(bronze_table_path)"],"outputs":[{"output_type":"display_data","data":{"application/vnd.livy.statement-meta+json":{"spark_pool":null,"statement_id":7,"statement_ids":[7],"state":"finished","livy_statement_state":"available","session_id":"5fc10eab-fa1e-4311-b1a4-fcfaf0cd6455","normalized_state":"finished","queued_time":"2024-09-02T16:05:55.4589899Z","session_start_time":null,"execution_start_time":"2024-09-02T16:06:21.3339791Z","execution_finish_time":"2024-09-02T16:06:23.0100694Z","parent_msg_id":"fb38f76c-e73c-4295-8c73-0ada2298d33d"},"text/plain":"StatementMeta(, 5fc10eab-fa1e-4311-b1a4-fcfaf0cd6455, 7, Finished, Available, Finished)"},"metadata":{}}],"execution_count":5,"metadata":{"jupyter":{"source_hidden":false,"outputs_hidden":false},"nteract":{"transient":{"deleting":false}},"microsoft":{"language":"python","language_group":"synapse_pyspark"}},"id":"2a606cf2-e507-406e-abc2-c423dbadca61"}],"metadata":{"kernel_info":{"name":"synapse_pyspark"},"kernelspec":{"name":"synapse_pyspark","language":"Python","display_name":"Synapse PySpark"},"language_info":{"name":"python"},"microsoft":{"language":"python","language_group":"synapse_pyspark","ms_spell_check":{"ms_spell_check_language":"en"}},"widgets":{},"nteract":{"version":"nteract-front-end@1.0.0"},"synapse_widget":{"version":"0.1","state":{}},"spark_compute":{"compute_id":"/trident/default"},"dependencies":{"lakehouse":{"default_lakehouse":"1367568c-68c3-4c83-a404-7d9da94a2412","default_lakehouse_name":"LH_Bronze","default_lakehouse_workspace_id":"bfcb8e41-2b70-4eaf-8e07-1ea42aafff4f"}}},"nbformat":4,"nbformat_minor":5}

#-------------------------------------
import requests
import io
import pandas as pd
from datetime import timedelta
#-------------------------------------

# Base URL for GitHub raw CSV files
base_url = "https://raw.githubusercontent.com/mikailaltundas/datasets-for-training/main/wind-power-dataset/"

# Path to the wind_power_production table in the Bronze Lakehouse
# You may need to change the path to match your workspace
bronze_table_path = "abfss://WindPowerGeneration@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/wind_power_production"


#-------------------------------------

# Load the existing wind_power_production table into a Pandas DataFrame
df_spark = spark.read.format("delta").load(bronze_table_path)

# Convert data table Spark to Pandas
df_pandas = df_spark.toPandas()

#-------------------------------------

# Get the most recent date and the next day
most_recent_date = pd.to_datetime(df_pandas['date'], format = '%Y%m%d').max()
next_date = (most_recent_date + timedelta(days = 1)).strftime('%Y%m%d')

# Form the URL and download the next day's CSV file
next_file_url =f"{base_url}{next_date}_wind_power_production.csv"
file_content = requests.get(next_file_url).content


# Load the CSV file into a Pandas Dataframe
df_pandas_new = pd.read_csv(io.StringIO(file_content.decode('utf-8')))

# Convert the date column to datetime in the Pandas DataFrame
df_pandas_new['date'] = pd.to_datetime(df_pandas_new['date'])

# Convert the Pandas DataFrame to a Spark DataFrame
df_spark_new = spark.createDataFrame(df_pandas_new, schema = df_spark.schema)
#-------------------------------------

# Append the new data to the existing wind_power_production table in Bronze Lakehouse
df_spark_new.write.format("delta").mode("append").save(bronze_table_path)
#-------------------------------------

