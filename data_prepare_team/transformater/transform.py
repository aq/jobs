import os.path
import shutil
import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, ShortType
from pyspark.conf import SparkConf

class Transform():

    def __init__(self, source_path, target_path, partitions_count, aws_credentials, spark=None):
        self.source_path = source_path
        assert self.source_path, "source_path is required"
        self.target_path = target_path
        assert self.target_path, "target_path is required"
        self.partitions_count = int(partitions_count)
        assert self.partitions_count > 0, "partitions_count should be bigger than 0."
        conf = self.spark_conf(aws_credentials)
        self.spark = spark or SparkSession.builder.config(conf=conf).getOrCreate()

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
        condition = col("image").isNotNull() & (col("image") != '')
        return source.filter(condition), source.filter(~condition)

    def unload(self, dataframe, directory):
        path = os.path.join(self.target_path, directory)
        if os.path.isdir(path):
            shutil.rmtree(path)
        dataframe.\
            repartition(self.partitions_count).\
            write.\
            mode('overwrite').\
            option("compression", "gzip").\
            parquet(path)

    @staticmethod
    def spark_conf(aws_credentials):
        return SparkConf().\
            set("spark.hadoop.fs.s3a.path.style.access", True).\
            set("spark.hadoop.fs.s3a.access.key", aws_credentials["aws_aki"]).\
            set("spark.hadoop.fs.s3a.secret.key", aws_credentials["aws_sak"]).\
            set("spark.hadoop.fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com").\
            set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").\
            set("com.amazonaws.services.s3.enableV4", True).\
            set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")

@click.command()
@click.option("-i", "--source_path", default="product_catalog.csv", help="Path where the source file is stored.")
@click.option("-o", "--target_path", default="transform_output", help="Path where the results will be stored.")
@click.option("-n", "--partitions_count", default="1", help="Number of partitions of the results.")
@click.option("--aws_aki", help="AWS access key")
@click.option("--aws_sak", help="AWS secret")
def main(source_path, target_path, partitions_count, aws_aki, aws_sak):
    """Entry point to run this Transform job."""
    aws_credentials = dict(aws_aki = aws_aki, aws_sak = aws_sak)
    Transform(source_path, target_path, partitions_count, aws_credentials).run()

if __name__ == '__main__':
    main()
