# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "20614907-a4f8-4b17-af2b-e87cc87867f3",
# META       "default_lakehouse_name": "dp700_dataeng_lh",
# META       "default_lakehouse_workspace_id": "0f5b1bcf-30c5-4967-9c17-b8388ad75799",
# META       "known_lakehouses": [
# META         {
# META           "id": "20614907-a4f8-4b17-af2b-e87cc87867f3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cellule "bootstrap"
spark.sql("USE dp700_dataeng_lh.dbo")

# Vérification
spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","false").load("Files/bronze/*.csv")
# df now is a Spark DataFrame containing CSV data from "Files/bronze/2019.csv".
display(df.head(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bootstrap de session (à exécuter en 1er à chaque ouverture)
spark.sql("USE dp700_dataeng_lh.dbo")
spark.sql("SELECT current_catalog(), current_schema()").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
    
# Create the schema for the table
orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
    ])
    
# Import all files from bronze folder of lakehouse
df = spark.read.format("csv").option("header", "false").schema(orderSchema).load("Files/bronze/*.csv")
    
# Display the first 10 rows of the dataframe to preview your data
display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name
    
# Add columns IsFlagged, CreatedTS and ModifiedTS
df = df.withColumn("FileName", input_file_name()) \
    .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01',True).otherwise(False)) \
    .withColumn("CreatedTS", current_timestamp()).withColumn("ModifiedTS", current_timestamp())
    
# Update CustomerName to "Unknown" if CustomerName null or empty
df = df.withColumn("CustomerName", when((col("CustomerName").isNull() | (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))
display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
