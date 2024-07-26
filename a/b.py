```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark Session
spark = SparkSession.builder.appName("Code Mapping").getOrCreate()

# Load the source tables
source_df = spark.read.format("csv").option("header", "true").load("source_path_of_src_trans.csv")

# Entry 1: Left pad the ACCOUNT_NO to make it 16 digit value
source_df = source_df.withColumn("ACCOUNT_NIUMBER", lpad(col("ACCOUNT_NO").cast("string"), 16, "0"))

# Entry 2: Direct Move
source_df = source_df.withColumn("BALANCE_AMOUNT", col("TRANS_AMT").cast("decimal"))

# Entry 3: Convert age to age group
source_df = source_df.withColumn("AGE_GROUP", when(col("AGE").between(20,30), "20-30")\
                                          .when(col("AGE").between(31,40), "31-40")\
                                          .otherwise("Unknown"))

# Entry 4: Multiply quantity by unit price to get total price
source_df = source_df.withColumn("TOTAL_PRICE", col("QUANTITY") * col("UNIT_PRICE"))

# Write the transformed data to the target table
source_df.write.format("csv").option("header", "true").mode("overwrite").save("target_path_of_dim_trans")

# Stop Spark Session
spark.stop()
```