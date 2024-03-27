### Please provide your code answer for the question here
from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.appName("databricks_de").getOrCreate()
visitors_path = 'data/births-with-visitor-data.json'


def _run_query(query):
    return spark.sql(query).collect()


def _strip_margin(text):
    return re.sub('\n[ \t]*\|', '\n', text)


# Write your code below this line
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

strip_margin_udf = udf(_strip_margin, StringType())

df = spark.read.json(visitors_path)
df = df.withColumn("vistors_parse", strip_margin_udf(df.visitors))

id = F.expr('xpath(vistors_parse, "//@id")')
sex = F.expr('xpath(vistors_parse, "//@sex")')
age = F.expr('xpath(vistors_parse, "//@age")')
# df = df.withColumn('array_visitors', F.arrays_zip(id, sex, age))
# df = df.withColumn('num_vistors', F.size(df.array_visitors))
df = df.withColumn('visitors_exploded', F.explode(F.arrays_zip(id, sex, age)))
df = df.select(
    'county',
    'created_at',
    'first_name',
    'id',
    'meta',
    'name_count',
    'position',
    'sex',
    'sid',
    'updated_at',
    'visitors',
    'year',
    'vistors_parse',
    F.col('visitors_exploded.0').alias('visitor_id'),
    F.col('visitors_exploded.1').alias('visitor_sex'),
    F.col('visitors_exploded.2').alias('visitor_age')
).createOrReplaceTempView("baby_visitors_data")
### Part A
queryA = """
        -- Write your query here
        select 
                count(*) 
        from baby_visitors_data
        """
query_result = _run_query(queryA)
partA = f"""records={query_result[0][0]}"""

### Part B
# This algorithm leads to county=SENECA, avgVisitors=2.9
queryB = """
        -- Write your query here
        with aggregation_by_birth as (
                select 
                        county, sid, count(*) as num_visitors_by_birth  
                from baby_visitors_data
                group by 1,2
        ),
        aggregation_by_county as (
                select 
                        county,
                        avg(num_visitors_by_birth) as avg_num_visitors_by_county
                from aggregation_by_birth 
                group by 1
        )
        select 
                county,
                avg_num_visitors_by_county 
        from aggregation_by_county
        order by avg_num_visitors_by_county desc
        limit 1
        """

# This algorithm leads to county=SENECA, avgVisitors=2.9
queryB = """
        -- Write your query here
        with aggregation_by_county as (
                select 
                        county, 
                        count(*) as num_visitors_by_county,
                        count(Distinct sid) as num_birth_by_county,
                        count(*)/count(Distinct sid) as avg_num_visitors_by_county,
                        count(Distinct id) as num_birth_by_county_v1,
                        count(*)/count(Distinct sid) as avg_num_visitors_by_county_v1
                from baby_visitors_data
                group by 1
        )
        select 
                county,
                avg_num_visitors_by_county 
        from aggregation_by_county
        order by avg_num_visitors_by_county desc
        limit 1
        """
query_result = _run_query(queryB)
partB = f"""county={query_result[0][0]}, avgVisitors={query_result[0][1]}"""

### Part C
# This algorithm leads to avgVisitorAge=34.869477506556414
queryC = """
        -- Write your query here
        with aggregation_by_birth as (
                select 
                        county, sid, avg(visitor_age) as avg_visitors_age_by_birth  
                from baby_visitors_data
                where county='KINGS'
                group by 1,2
        )
        select 
                avg(avg_visitors_age_by_birth) as avg_visitors_age_by_county
        from aggregation_by_birth 
        """
# # This algorithm leads to avgVisitorAge=86.25943110752472        #
# queryC = """
#         -- Write your query here
#         with aggregation_by_county as (
#                 select
#                         county,
#                         sum(visitor_age) as sum_visitor_age_by_county,
#                         count(Distinct sid) as num_birth_by_county
#                 from baby_visitors_data
#                 where county='KINGS'
#                 group by 1
#         )
#         select
#                 sum_visitor_age_by_county/num_birth_by_county as avg_visitors_age_by_county
#         from aggregation_by_county
#         """
query_result = _run_query(queryC)
partC = f"""avgVisitorAge={query_result[0][0]}"""

### Part D
queryD = """
        -- Write your query here
        with aggregation_by_age as (
                select 
                        visitor_age,
                        count(*) as num_visitor_age
                from baby_visitors_data
                where county='KINGS'
                group by 1
        )
        select 
                visitor_age,
                num_visitor_age 
        from aggregation_by_age
        order by num_visitor_age desc
        limit 1
        """
query_result = _run_query(queryD)
partD = f"""mostCommonBirthAge={query_result[0][0]}, count={query_result[0][1]}"""

# Do not edit below this line
with open("data/output.txt", "w") as f:
    f.write(f"{partA}\n{partB}\n{partC}\n{partD}\n")

spark.stop()
