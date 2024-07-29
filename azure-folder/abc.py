 Here is the PySpark code for the given mapping document:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Mapping').getOrCreate()

df = spark.read.format("csv").option("header", "true").load("path/to/source/data")

# Entry 1
df = df.withColumn("ACCOUNT_NIUMBER", lpad(col("ACCOUNT_NO"), 16, "0"))

# Entry 2 
df = df.withColumnRenamed("TRANS_AMT", "BALANCE_AMOUNT").cast("decimal(20,2)")

# Entry 3
df = df.withColumn("AGE_GROUP", when(col("AGE") < 20, "0-20")
                   .when((col("AGE") >= 20) & (col("AGE") < 30), "20-30") 
                   .otherwise("30+"))

# Entry 4                   
df = df.withColumn("TOTAL_PRICE", col("QUANTITY") * col("UNIT_PRICE")) \
       .cast("decimal(10,2)")
       
df.write.mode("overwrite").format("parquet").saveAsTable("DIM_TRANS")

spark.stop()
```

This code:

1. Reads the source data into a DataFrame 
2. Performs the specified transformation for each entry:
   - Entry 1: Left pads ACCOUNT_NO 
   - Entry 2: Renames and casts column
   - Entry 3: Creates AGE_GROUP based on ranges
   - Entry 4: Calculates TOTAL_PRICE by multiplying columns
3. Writes the final DataFrame to the target table

Let me know if you need any clarification or have additional entries to map!