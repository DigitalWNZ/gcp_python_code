# Please provide your code answer for Question 1 here
# Write the Data Frame into a CSV and load it into the `data/` folder
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

# create a spark session
spark = SparkSession.builder.appName("databricks_de").getOrCreate()
schema = ["sid", "id", "position", "created_at", "created_meta", "updated_at", "updated_meta", "meta", "year", "first_name", "county", "sex", "count"]
baby_names_path = 'data/databricks_baby_names.json'
# Do not edit the code above this line

# Write your code here
from pyspark.sql import SQLContext
# df = spark.createDataFrame([], schema=schema)

df_input= spark.read.json(baby_names_path,multiLine=True) \
          .select(f.explode("data").alias("data")) \
          .createOrReplaceTempView("baby_data")

df=spark.sql("""
                select 
                    data[0] as sid, 
                    data[1] as id, 
                    data[2] as position, 
                    data[3] as created_at, 
                    data[4] as created_meta, 
                    data[5] as updated_at, 
                    data[6] as updated_meta, 
                    data[7] as meta, 
                    data[8] as year, 
                    data[9] as first_name, 
                    data[10] as county, 
                    data[11] as sex,
                    data[12] as count
                from baby_data
                """)

# Do not edit the code below this line
df.toPandas().to_csv('data/baby_names.csv', index=False)
spark.stop()

# The Please provide your brief written description of your code here.
'''
The key ideas include 
1. Levearage spark.read.json(baby_names_path,multiLine=True) to load the file and f.explode to unnest the json array to create the dataframe. 
2. Leverage sql.sql to parse the raw data into dataframe.  
'''