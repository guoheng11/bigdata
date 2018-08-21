from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# create the spark session
spark = SparkSession.builder.master("local").getOrCreate()

# load the train data-set
dfTrain = spark.read.format("csv").option("header", "false") \
    .load("hdfs:///user/events/train_data_spark") \
  .withColumn("label", col("_c0").cast(FloatType())) \
  .withColumn("user", col("_c1")) \
  .withColumn("event", col("_c2")) \
  .withColumn("locale", col("_c3").cast(FloatType())) \
  .withColumn("gender", col("_c4").cast(FloatType())) \
  .withColumn("age", col("_c5").cast(FloatType())) \
  .withColumn("view_ahead_days", col("_c6").cast(FloatType())) \
  .withColumn("event_creator_is_friend", col("_c7").cast(FloatType())) \
  .withColumn("invited_friends_percentage", col("_c8").cast(FloatType())) \
  .withColumn("attended_friends_percentage", col("_c9").cast(FloatType())) \
  .withColumn("not_attended_friends_percentage", col("_c10").cast(FloatType())) \
  .withColumn("maybe_attended_friends_percentage", col("_c11").cast(FloatType())) \
  .withColumn("invited", col("_c12").cast(FloatType())) \
.select("label", \
  "user", "event", \
  "locale", "gender", "age", \
  "view_ahead_days", "event_creator_is_friend", \
  "invited_friends_percentage", "attended_friends_percentage", \
  "not_attended_friends_percentage", "maybe_attended_friends_percentage", \
  "invited")

# the VectorAssembler transformer
assembler = VectorAssembler(inputCols=["locale", "gender", "age", "view_ahead_days", "event_creator_is_friend", "invited_friends_percentage", "attended_friends_percentage", "not_attended_friends_percentage", "maybe_attended_friends_percentage", "invited"], outputCol="features")
# the logistic regression。  Estimator
lr = LogisticRegression(maxIter=10, regParam=0.001)
# the pipeline with 2 stages
pipeline = Pipeline(stages=[assembler, lr])

# build the model
model = pipeline.fit(dfTrain)

# load the test data-set
dfTest = spark.read.format("csv").option("header", "false") \
    .load("hdfs:///user/events/test_data_spark") \
  .drop("_c0") \
  .withColumn("user", col("_c1")) \
  .withColumn("event", col("_c2")) \
  .withColumn("locale", col("_c3").cast(FloatType())) \
  .withColumn("gender", col("_c4").cast(FloatType())) \
  .withColumn("age", col("_c5").cast(FloatType())) \
  .withColumn("view_ahead_days", col("_c6").cast(FloatType())) \
  .withColumn("event_creator_is_friend", col("_c7").cast(FloatType())) \
  .withColumn("invited_friends_percentage", col("_c8").cast(FloatType())) \
  .withColumn("attended_friends_percentage", col("_c9").cast(FloatType())) \
  .withColumn("not_attended_friends_percentage", col("_c10").cast(FloatType())) \
  .withColumn("maybe_attended_friends_percentage", col("_c11").cast(FloatType())) \
  .withColumn("invited", col("_c12").cast(FloatType())) \
.select("user", "event", \
  "locale", "gender", "age", \
  "view_ahead_days", "event_creator_is_friend", \
  "invited_friends_percentage", "attended_friends_percentage", \
  "not_attended_friends_percentage", "maybe_attended_friends_percentage", \
  "invited" \
)
# predict
prediction = model.transform(dfTest)
# save the prediction
prediction.select("user", "event", "prediction").repartition(1).write.format("csv").option("header", "true").mode("overwrite").save("hdfs:///user/events/predictions")

# stop the spark session
spark.stop()