qn2_sql.py
### Please provide your SQL answer for Question 2 here
## SQL
# Import libraries outside the solution function
import pandas as pd
from sqlalchemy import create_engine, text

# Set up sqlite database object
engine = create_engine('sqlite:///data/baby_names.db', echo=False)
baby_names = pd.read_csv('data/baby_names.csv')
baby_names.to_sql('baby_names', con=engine, if_exists='replace', index=False)
# Do not edit above this line

# Write your Query here
query = text("""
        -- Write your Query here
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
with engine.connect() as connection:
    result = pd.read_sql_query(query, connection).to_string(index=False)
    with open('data/most_popular_sql.txt', 'w') as f:
        f.write(result)
