from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType, Row, StructField, StructType, StringType
import pytest
from pyspark.sql import SparkSession
import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from transformater.transform import Transform

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.master("local[*]")\
        .appName("UnitTest: TestTransform")\
        .getOrCreate()
    return spark

product_schema = StructType([
    StructField("brand", StringType(), False),
    StructField("category_id", IntegerType(), False),
    StructField("comment", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("description", StringType(), True),
    StructField("image", StringType(), True),
    StructField("year_release", IntegerType(), True)
])

products_list = [
    ("Toshiba", 71, None, "NOK", "Male", "data:image/png;base64,image1", 1993),
    ("HP", 99, "Suspendisse accumsan tortor quis turpis.","PEN", "Male", "", 1988)
]

def test_split(spark: SparkSession):
    products: DataFrame = spark.createDataFrame(
        products_list,
        schema=product_schema
    )
    transform = Transform('source_path', 'target_path', 1, dict(aws_aki=None, aws_sak=None))
    valid, invalid = transform.split(products)
    assert valid.count() == 1
    assert valid.collect()[0]["brand"] == "Toshiba"
    assert invalid.count() == 1
    assert invalid.collect()[0]["brand"] == "HP"
