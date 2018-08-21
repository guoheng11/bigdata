from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel, LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint

# Load and parse the data
def parsePoint(fields):
	values = [float(field) for field in fields]
	return LabeledPoint(values[0], values[3:])

sc = SparkContext('local', 'pyspark')
data = sc.textFile("/user/events/train_data/").map(lambda line: line.split(",")).filter(lambda x: x[5] != '\\N')

#RDD[LabeledPoint]
parsedData = data.map(parsePoint)

(trainingData, testData) = parsedData.randomSplit([0.7, 0.3])

# Build the model
model = LogisticRegressionWithSGD.train(trainingData)

model.save(sc, "/user/events/model/LR")

# Evaluating the model on training data
labelsAndPreds = testData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(testData.count())
print("Training Error = " + str(trainErr))