# Databricks notebook source
# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/2.common_functions"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name","driver_name") \
    .withColumnRenamed("nationality","driver_nationality").withColumnRenamed("number","driver_number") 

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time", "race_time").withColumnRenamed("driverId", "driver_id")


# COMMAND ----------

race_circuits_df = races_df.join( circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(circuits_df.circuit_location, races_df.race_year, races_df.race_name, races_df.race_id, races_df.race_date)

# COMMAND ----------

joined_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id, "inner" ) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner" ) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner" ) 


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = joined_df.select(race_circuits_df.race_year, race_circuits_df.race_name ,  race_circuits_df.race_date , race_circuits_df.circuit_location \
        , drivers_df.driver_name, drivers_df.driver_number, drivers_df.driver_nationality \
        , constructors_df.team \
        , results_df.grid, results_df.fastest_lap, results_df.race_time, results_df.points.cast("float"),results_df.position ) \
        .withColumn('created_date', current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
