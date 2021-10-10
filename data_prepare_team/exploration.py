# wget https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv

# head product_catalog.csv

# pyspark --packages=com.amazonaws:aws-java-sdk-bundle:1.11.1034,org.apache.hadoop:hadoop-aws:3.2.2

src = "s3a://backmarket-data-jobs/data/product_catalog.csv"

df = spark.read.\
    option("header", "true").\
    option("delimiter", ",").\
    csv(src)
df.cache()

df.count()
# => 1000

df.show()
# +-------+-----------+--------------------+--------+-----------+--------------------+------------+
# |  brand|category_id|             comment|currency|description|               image|year_release|
# +-------+-----------+--------------------+--------+-----------+--------------------+------------+
# |Toshiba|         71|                null|     NOK|       Male|data:image/png;ba...|        1993|
# |     HP|         99|Suspendisse accum...|     PEN|       Male|data:image/png;ba...|        1988|
# |   Acer|         69|Donec dapibus. Du...|     IDR|     Female|data:image/png;ba...|        2010|
# |     HP|         62|                null|     CNY|     Female|data:image/png;ba...|        2008|
# |   Dell|         48|Vivamus in felis ...|     CNY|       Male|data:image/png;ba...|        2013|
# +-------+-----------+--------------------+--------+-----------+--------------------+------------+

from pyspark.sql.functions import *

df.select(col("brand")).distinct().count()
# => 6

df.select(col("category_id")).distinct().count()
# => 100

df.select("image").limit(1).collect()[0].image
# 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAALISURBVDjLpZNpMNRhHMc31ExNmjRTDdVMsxXLkntnTcmxJTKFMK6JnK1KcheKDDZXubbSoRdlNFFNGFTIbeiNsyjHnzI0ZrFk/621+21bx6wx9caLz/Pi+T3fz/Ob56AAoKwH2WAU2QyP+wNO6xKY3WgOsMseKJQvTrm6p0ohplzcuqR4/lsQ0QQmhzA0SxkUW6QPbv47xz9t3zBjd3ZeStu0g+OAFJGUESnUtYLwRqjHjyhqxxFCvVtE+dwxC84vc5ZklmV1dHnhrJUNlW9ty588ZS+wzSHiVwkMwxpAPRm/b0/kcOqF82/m5wyYpIBhwpXfyTCOyAjKLJT0Frji29sktD+xQgeX7ikrGoTVY3nhhJaJZFj/hFA+vD+YeMFOe7QwVhOF6c4yYHYUU53FaEm3Hl8UhNbKBKJdagVC1b0i0zPvyS3eRLayz7Didp+hSteb+fMT3XELwu8lGKtNg6DrNRaIRnQ9DSBlAr2QGsTo2euKlVUkC9t2JFNciUSKCwEFF0LMCi2S8LpjJWJBIwQDl8FrC8KXZ77oyXZDW7aD+qIguBrYsNEOCpv65VsPiE3Kn+y6DjHZgrl+L5AjHpj5HI2hPPPxKeNDH1cOUffKBwgpSk4iitLu5fDd9IOcsU9RS2FPkMPu4HfHoI9rSfalM4yk3aqtCA4HvcPjnTTz5XBkskZlZ0UIxIJ6kEO++D1yDtPSTnq5LJFjlh5/zTvQuVQBk3BtVWYEPc/7qjqvpzwaHRWZ+NHqjLkhD/Dar+FrrsXPqjSGJjNBe5CRSM9ZLbhYDgO2pp0+W8PqZQoLmCHQ+9AJNdFqaHpgg7oEo9E7/keOy24sWEON5q7NNg6jbV0R0APLQGeXQotdggR/HQhbciFszUJrkgWe+x0AK/AeaH6vligGzbdIxopAHmdTFZLjRRNV37YRVWVY1pVG6VD/9xv/AJGzmhVs7+fUAAAAAElFTkSuQmCC'

df2 = df.filter(col("image").isNotNull() | (col("image") != ''))

df2.count()
# => 740

df2.\
    write.\
    mode('overwrite').\
    option("compression", "gzip").\
    parquet("product_catalog_output")

df3 = df.filter(col("image").isNull() | (col("image") == ''))

df3.\
    write.\
    mode('overwrite').\
    option("compression", "gzip").\
    parquet("product_catalog_archive")

import shutil
shutil.rmtree("products_with_image") 
shutil.rmtree("products_without_image") 
