from pyspark import SparkConf, SparkContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel, LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors

# create the spark-context
sc = SparkContext('local', 'pyspark')

# load and parse the data
data = sc.textFile("hdfs:///user/events/test_data_spark/")
splits = data.map(lambda line: line.split(',')).filter(lambda x: x[5] != '\\N')

# the user-event label
user_event = splits.map(lambda fields: (fields[1], fields[2]))
# extract the features
features = splits.map(lambda fields: Vectors.dense(fields[3:]))

# load the model
sameModel = LogisticRegressionModel.load(sc, "hdfs:///user/events/model/LR")
# predict the users-interest
predictions = sameModel.predict(features.map(lambda p: (p[0:])))
 
# re-organize the label-prediction
label_prediction = user_event.zip(predictions).map(lambda ((userid, eventid), interested) : (int(userid), int(eventid), interested))

# create the header
header = sc.parallelize(["user,event,interested"])
# wrap the label-prediction
lines = label_prediction.map(lambda v:(str(v[0]) + "," + str(v[1]) + "," + str(v[2])))
# save the result into HDFS
(header + lines).repartition(1).saveAsTextFile("hdfs:///user/events/predictions")