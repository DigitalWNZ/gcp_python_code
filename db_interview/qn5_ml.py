# Download bike sharing data from github and save it locally.
import pandas as pd

url = 'https://raw.githubusercontent.com/deep-learning-with-pytorch/dlwpt-code/master/data/p1ch4/bike-sharing-dataset/hour.csv'
df = pd.read_csv(url, index_col=0, parse_dates=[0])
df.to_csv('bike_share.csv', index=False)

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline

# Build dataframe directly from csv on local disk
spark = SparkSession.builder.appName("mlib_demo").getOrCreate()
df = spark.read.csv("bike_share.csv", header="true", inferSchema="true")
# unlock to acceleration execution for test purpose
# df = df.sample(0.1)
df.cache()

df = df.drop("instant").drop("dteday").drop("casual").drop("registered")
df.show(5)

train, test = df.randomSplit([.7, .3], seed=36)
print("There are %d training examples and %d test examples." % (train.count(), test.count()))

print('feature enginering start')
from pyspark.ml.feature import VectorAssembler, VectorIndexer

# Remove the target column from the input feature set.
featuresCols = df.columns
featuresCols.remove('cnt')
# combines all feature columns into a single feature vector column, "rawFeatures".
vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")
# identifies categorical features and indexes them to creates a new column "features".
vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=4)

'''
Define the model training stage of the pipeline.

GBT algorithms normally have better accuracy than other regression algorithms (such as linear regression, random forrest and etc) because the trees in GBT are trained to correct each other.

So GBT is directly adopted for this task. 
'''
print('hyper parameter tuning start')
from pyspark.ml.regression import GBTRegressor

gbt = GBTRegressor(labelCol="cnt")

# Leverage hyperparameter tuning and cross validation to identify best model
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

'''
In this programe, we leverage grid search and cross validate as hyperparameter tuning tools to search parameter spaces to find the optimal combination. 

Grid search is still the most classical approach and it is a good approach when you are fairly confident about the search space. Otherwise, you may consider Hyperopt for larger search space for the sake of performance. 

Define a grid of hyperparameters to test:
- maxDepth: maximum depth of each decision tree
- maxIter: iterations, or the total number of trees
'''
paramGrid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [2, 5]) \
    .addGrid(gbt.maxIter, [10, 100]) \
    .build()

evaluator = RegressionEvaluator(metricName="rmse", labelCol=gbt.getLabelCol(), predictionCol=gbt.getPredictionCol())

# Declare the CrossValidator, which performs the model tuning.
cv = CrossValidator(estimator=gbt, evaluator=evaluator, estimatorParamMaps=paramGrid, numFolds=3, seed=36)

# Build pipeline for prediction
print('Start to build pipeline')
from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[vectorAssembler, vectorIndexer, cv])

print('Start training')
pipelineModel = pipeline.fit(train)

print('Start prediction')
predictions = pipelineModel.transform(test)
predictions.select("cnt", "prediction", *featuresCols).show(5)

print('Start evaluation')
rmse = evaluator.evaluate(predictions)
print("RMSE on our test set: %g" % rmse)