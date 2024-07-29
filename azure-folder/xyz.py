 Here are some pytest test cases to test the PySpark code:

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col 

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_entry1(spark):
    data = [(123,), (1234,)]
    df = spark.createDataFrame(data, ["ACCOUNT_NO"])
    
    result = df.withColumn("ACCOUNT_NUMBER", lpad(col("ACCOUNT_NO"), 16, "0"))
    
    assert result.collect() == [(0000000000000000123,), (000000000000001234,)]

def test_entry2(spark):
    data = [(100.5,), (500.75,)] 
    df = spark.createDataFrame(data, ["TRANS_AMT"])
    
    result = df.withColumnRenamed("TRANS_AMT", "BALANCE_AMOUNT").cast("decimal(20,2)")
    
    assert result.collect() == [(100.50,), (500.75,)]
    
def test_entry3(spark):
    data = [(10,), (25,), (35,)]
    df = spark.createDataFrame(data, ["AGE"])  
    
    result = df.withColumn("AGE_GROUP", when(col("AGE") < 20, "0-20")  
                           .when((col("AGE") >= 20) & (col("AGE") < 30), "20-30")
                           .otherwise("30+"))
                           
    assert set(x.AGE_GROUP for x in result.collect()) == set(["0-20", "20-30", "30+"])
    
def test_entry4(spark):
    data = [(5, 10.5), (10, 2.5)]
    df = spark.createDataFrame(data, ["QUANTITY", "UNIT_PRICE"])
    
    result = df.withColumn("TOTAL_PRICE", col("QUANTITY") * col("UNIT_PRICE")) \
               .cast("decimal(10,2)")
               
    assert result.collect() == [(52.50,), (25.00,)]              
```

This tests:

- Entry 1: Account number padding 
- Entry 2: Column rename and cast
- Entry 3: Age group mapping
- Entry 4: Price calculation

Let me know if you need any other test cases!