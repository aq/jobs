import os.path
import shutil
import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, ShortType

class Transform():

    def __init__(self, source_path, target_path, partitions_count, spark=None):
        self.source_path = source_path
        assert self.source_path, "source_path is required"
        self.target_path = target_path
        assert self.target_path, "target_path is required"
        self.partitions_count = int(partitions_count)
        assert self.partitions_count > 0, "partitions_count should be bigger than 0."
        self.spark = spark or SparkSession.builder.getOrCreate()

    def run(self):
        source = self.load()
        source.cache()
        selected, rejected = self.split(source)
        self.unload(selected, "valid")
        self.unload(rejected, "invalid")

    schema = StructType().\
        add("brand",        StringType()).\
        add("category_id",  ShortType()).\
        add("comment",      StringType()).\
        add("currency",     StringType()).\
        add("description",  StringType()).\
        add("image",        StringType()).\
        add("year_release", ShortType())
    def load(self):
        return self.spark.read.\
            option("header", "true").\
            option("delimiter", ",").\
            schema(self.schema).\
            csv(self.source_path)

    @staticmethod
    def split(source):
        condition = col("image").isNotNull() | (col("image") != '')
        return source.filter(condition), source.filter(~condition)

    def unload(self, dataframe, directory):
        path = os.path.join(self.target_path, directory)
        if os.path.isdir(path):
            shutil.rmtree(path)
        dataframe.\
            write.\
            repartition(self.partitions_count).\
            mode('overwrite').\
            option("compression", "gzip").\
            parquet(path)

@click.command()
@click.option("-i", "--source_path", default="product_catalog.csv", help="Path where the source file is stored.")
@click.option("-o", "--target_path", default="transform_output", help="Path where the results will be stored.")
@click.option("-n", "--partitions_count", default="1", help="Number of partitions of the results.")
def main(source_path, target_path, partitions_count):
    """Entry point to run this Transform job."""
    Transform(source_path, target_path, partitions_count).run()

if __name__ == '__main__':
    main()
