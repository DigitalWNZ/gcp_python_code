"""
background
dataset contains 3 columns:
    - pcaVector: The PCA transformation of raw transaction data.
    For this example we'll assume that this PCA transformation occurs as part of some data pipeline
    before the data reaches us.

    - amountRange: A value between 0 and 7. The approximate amount of a transaction.
    The values correspond to 0-1, 1-5, 5-10, 10-20, 20-50, 50-100, 100-200, and 200+ in dollars.

    - label: 0 or 1. Indicates whether a transaction was fraudulent.
"""
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.GBTClassifier

val oneHot = new OneHotEncoder()
  .setInputCols(Array("amountRange"))
  .setOutputCols(Array("amountVect"))

val vectorAssembler = new VectorAssembler()
  .setInputCols(Array("amountVect", "pcaVector"))
  .setOutputCol("features")

val estimator = new GBTClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")

"""
VectorAssembler has certain limitations in a streaming context. 
Specifically, VectorAssembler only works on Vector columns of known size. This is not an issue on batch DataFrames 
because you can inspect the contents of the DataFrame to determine the size of the Vectors.

To use a pipeline to transform streaming DataFrames, you must explicitly specify the size of the pcaVector column 
by including a VectorSizeHint stage. 
The other input to our VectorAssembler stage, amountVect, is also a vector column, 
but because this column is the output of an MLlib transformer, it already contains the appropriate size information 
so you don't need to do anything additional for this column.
"""
import org.apache.spark.ml.feature.VectorSizeHint

val vectorSizeHint = new VectorSizeHint()
  .setInputCol("pcaVector")
  .setSize(28)

val pipeline = new Pipeline()
  .setStages(Array(oneHot, vectorSizeHint, vectorAssembler, estimator))
