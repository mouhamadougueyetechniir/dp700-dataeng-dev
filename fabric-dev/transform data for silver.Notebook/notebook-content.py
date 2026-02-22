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

# MARKDOWN ********************

# # Create a medallion architecture in a Microsoft Fabric lakehouse
# 


# MARKDOWN ********************

# ## Create a lakehouse and upload data to bronze layer
# 


# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/bronze/*.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.pandas import *
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

df = spark.read.format("csv").option("header","true").schema(orderSchema).load("Files/bronze/*.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Explore datasets before transformation
# Compute summary statistics for numerical columns

# CELL ********************

display(df.describe(df.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get number of rows and columns
row =  df.count()

col =  len(df.columns)
print(f'Dimension of the Dataframe is: {(row,col)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f'Show data types of all columns: ', df.dtypes)
print(f'Show Dataframe columns: ', df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform data and load to silver Delta table
# 
# Creating  columns for data validation and cleanup, using a PySpark dataframe to add columns and update the values of some of the existing columns

# CELL ********************

from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name

# Add columns IsFlagged, CreatedTS and ModifiedTS
df = df.withColumn("FileName", input_file_name()) \
     .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01', True).otherwise(False)) \
     .withColumn("CreatedTS", current_timestamp()) \
     .withColumn("ModifiedTS", current_timestamp())

# update CustormerName to "Unknown" if CustormerName is null or empty
df = df.withColumn("CustomerName", when((col("CustomerName").isNull() | (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 

# MARKDOWN ********************

#  ## Prepare  silver schema and load data to silver layer
#  Creation of schema for the sales_silver table in the sales database using Delta Lake format

# MARKDOWN ********************

# ```py
# def init_session(catalog="dp700_dataeng_lh", schema="dbo"):
#     spark.sql(f"USE  {catalog}")
#     spark.sql(f"USE {catalog}.{schema}")
#     print("Session initialisée →",
#           spark.sql("SELECT current_catalog(), current_schema()").collect()[0])
# 
# init_session()
# ```

# CELL ********************

spark.sql("SELECT current_catalog()").show()
spark.sql("SELECT current_schema()").show()
spark.sql("SHOW SCHEMAS IN spark_catalog").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *

spark.sql("USE dp700_dataeng_lh.dbo")

sales_silver = "dp700_dataeng_lh.dbo.sales_silver"

DeltaTable.createIfNotExists(spark) \
    .tableName("dp700_dataeng_lh.dbo.sales_silver") \
    .addColumn("SalesOrderNumber", StringType()) \
    .addColumn("SalesOrderLineNumber", IntegerType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("Item", StringType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .addColumn("FileName", StringType()) \
    .addColumn("IsFlagged", BooleanType()) \
    .addColumn("CreatedTS", DateType()) \
    .addColumn("ModifiedTS", DateType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

spark.sql("USE dp700_dataeng_lh.dbo")
deltaTable = DeltaTable.forName(spark, "dp700_dataeng_lh.dbo.sales_silver")

match_condition = (
    "silver.SalesOrderNumber = updates.SalesOrderNumber AND "
    "silver.OrderDate = updates.OrderDate AND "
    "silver.CustomerName = updates.CustomerName AND "
    "silver.Item = updates.Item"
)

(
    deltaTable.alias("silver")
    .merge(df.alias("updates"), match_condition)
    .whenMatchedUpdate(set={
        "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
        "Email": "updates.Email",
        "Quantity": "updates.Quantity",
        "UnitPrice": "updates.UnitPrice",
        "Tax": "updates.Tax",
        "FileName": "updates.FileName",
        "IsFlagged": "updates.IsFlagged",
        "ModifiedTS": "current_timestamp()"
    })
    .whenNotMatchedInsert(values={
        "SalesOrderNumber": "updates.SalesOrderNumber",
        "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
        "OrderDate": "updates.OrderDate",
        "CustomerName": "updates.CustomerName",
        "Email": "updates.Email",
        "Item": "updates.Item",
        "Quantity": "updates.Quantity",
        "UnitPrice": "updates.UnitPrice",
        "Tax": "updates.Tax",
        "FileName": "updates.FileName",
        "IsFlagged": "updates.IsFlagged",
        "CreatedTS": "coalesce(updates.CreatedTS, current_timestamp())",
        "ModifiedTS": "current_timestamp()"
    })
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
