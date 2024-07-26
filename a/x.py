```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad, col, when

# Fixture to create SparkSession
@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.appName("Test Code").master("local[2]").getOrCreate()
    yield spark
    spark.stop()

# Fixture to create source DataFrame
@pytest.fixture(scope="session")
def source_df(spark_session):
    # Sample data for testing
    data = [
        (1, "12345", 1000.00, 25, 10, 2.50),
        (2, "67890", 500.50, 35, 5, 5.00)
    ]
    # Define schema
    schema = ["ACCOUNT_NO", "TRANS_AMT", "AGE", "QUANTITY", "UNIT_PRICE"]
    # Create DataFrame
    return spark_session.createDataFrame(data, schema)

# Test for Entry 1: Left pad the ACCOUNT_NO to make it 16 digit value
def test_account_number_padding(source_df):
    result_df = source_df.withColumn("ACCOUNT_NIUMBER", lpad(col("ACCOUNT_NO").cast("string"), 16, "0"))
    assert result_df.select("ACCOUNT_NIUMBER").collect()[0][0] == "0000000000012345"

# Test for Entry 2: Direct Move
def test_balance_amount_transformation(source_df):
    result_df = source_df.withColumn("BALANCE_AMOUNT", col("TRANS_AMT").cast("decimal"))
    assert "BALANCE_AMOUNT" in result_df.columns

# Test for Entry 3: Convert age to age group
def test_age_group_transformation(source_df):
    result_df = source_df.withColumn("AGE_GROUP", when(col("AGE").between(20,30), "20-30")\
                                          .when(col("AGE").between(31,40), "31-40")\
                                          .otherwise("Unknown"))
    assert result_df.select("AGE_GROUP").collect()[0][0] == "20-30"
    assert result_df.select("AGE_GROUP").collect()[1][0] == "31-40"

# Test for Entry 4: Multiply quantity by unit price to get total price
def test_total_price_calculation(source_df):
    result_df = source_df.withColumn("TOTAL_PRICE", col("QUANTITY") * col("UNIT_PRICE"))
    assert result_df.select("TOTAL_PRICE").collect()[0][0] == 25.0
```
