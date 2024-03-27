### Please provide your code answer for Question 2 here
## Python
# Import libraries outside the solution function
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("databricks_de").getOrCreate()
df = spark.read.csv("data/baby_names.csv", header=True, inferSchema=True)
df.createOrReplaceTempView('baby_names')
# Do not edit the code above this line

# Write your code below this line
# df = spark.createDataFrame([], schema=['Year', 'first_name'])
df=spark.sql("""
    with ranked_data as (
        SELECT
            year,
            first_name,
            count,
            row_number() over (partition by year order by count desc) as rn
        FROM baby_names
    )
    select 
        year,
        first_name
    from ranked_data
    where rn = 1
""")

# Do not edit the code below this line
with open('data/most_popular_python.txt', 'w') as f:
    f.write(df.toPandas().to_string(index=False))
spark.stop()