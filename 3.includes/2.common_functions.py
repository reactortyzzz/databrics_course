# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)

    output_df = input_df.select(column_list)
    return output_df 

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
   
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    #for this method partition column should be the last one in the table so we need to change the order in step above
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    #this part is used for incremental part
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
   #this part is used for cut-over initial data part
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}") 
