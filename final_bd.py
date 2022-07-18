# Databricks notebook source
#Task1 part a
import pyspark
from pyspark.context import SparkContext
sc=SparkContext.getOrCreate()
autoData=sc.textFile("/FileStore/tables/creditcard.csv")  # creating rdd
autoData.count()

# COMMAND ----------

type(autoData)
from pyspark.sql import Row
j=autoData.map(lambda x: Row(x)).toDF() # converting to dataframe

# COMMAND ----------

type(j)

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(j.toPandas()) # converting to pandas
df.head()

# COMMAND ----------

df

# COMMAND ----------

df.nunique()

# COMMAND ----------

df.columns

# COMMAND ----------

df.index

# COMMAND ----------

df.isnull()

# COMMAND ----------

df.describe()

# COMMAND ----------

df.memory_usage(deep=True)

# COMMAND ----------

df.get('V1')

# COMMAND ----------

df.loc[2]

# COMMAND ----------

df.count()

# COMMAND ----------

df.dropna()

# COMMAND ----------

import numpy as np
arr=np.array(j)

# COMMAND ----------

print(arr)

# COMMAND ----------

j=np.arange(1, 2, 0.1)

# COMMAND ----------

np.max(arr)

# COMMAND ----------

##### Task 1 b part
#RDD- Through RDD, we can process structured as well as unstructured data. But, in RDD user need to specify the schema of ingested data, RDD cannot infer its own.It is a distributed collection of data elements. That is spread across many machines over the cluster, they are a set of Scala or Java objects representing data.RDD Supports object-oriented programming style with compile-time type safety.
#DataFrame- In data frame data is organized into named columns. Through dataframe, we can process structured and unstructured data efficiently. It also allows Spark to manage schema.As we discussed above, in a data frame data is organized into named columns. Basically, it is as same as a table in a relational database.If we try to access any column which is not present in the table, then an attribute error may occur at runtime. Dataframe will not support compile-time type safety in such case.

import pyspark
from pyspark.context import SparkContext
sc=SparkContext.getOrCreate()
autoData=sc.textFile("/FileStore/tables/creditcard.csv")  # creating rdd
autoData.count()

# dataframe
from pyspark.sql import Row
j=autoData.map(lambda x: Row(x)).toDF() # converting to dataframe
import pandas as pd
df = pd.DataFrame(j.toPandas()) # converting to pandas
df.head()

# COMMAND ----------

##### Task 1 ####### C part
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
df = spark.read.format('com.databricks.spark.csv').\
                               options(header='true', \
                               inferschema='true').\
                load("/FileStore/tables/creditcard.csv",header=True)
df.show(12)
df.printSchema()

# COMMAND ----------

import pyspark
from pyspark.context import SparkContext
sc=SparkContext.getOrCreate()
autoData=sc.textFile("/FileStore/tables/creditcard.csv")  # creating rdd
autoData.collect()

# COMMAND ----------

from pyspark import SparkContext
sc=SparkContext.getOrCreate()
collData=sc.parallelize([3,5,4,7,4,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,33,37,39,43,73.63])
collData.collect()

# COMMAND ----------

#Task 1 part d
#### transformation
autoData.take(5)

# COMMAND ----------

autoData.count()

# COMMAND ----------

autoData.distinct().count()

# COMMAND ----------

autoData.union(collData).collect()

# COMMAND ----------

words1 = sc.parallelize(["database","big data","datascience","machine learning"])
words2 = sc.parallelize(["big data","deeplearning","NLP"])
#for intersection in words1.intersection(words2).distinct().collect():
df_inter=words1.intersection(words2)
df_inter.collect()
data=autoData.intersection(collData)
data.collect()

# COMMAND ----------

#### actions
autoData.min()

# COMMAND ----------

autoData.max()

# COMMAND ----------

autoData.count()

# COMMAND ----------

autoData.collect()

# COMMAND ----------

from operator import add
autoData.fold(0,add)

# COMMAND ----------

autoData.reduce(max)

# COMMAND ----------

autoData.countByValue()

# COMMAND ----------

autoData.first()

# COMMAND ----------

autoData.top(2)

# COMMAND ----------

seqOp = (lambda x, y: (x+y))
combOp = (lambda x, y: (x+y))
collData.aggregate((0), seqOp, combOp)

# COMMAND ----------

########### Task 2 ############
###part 1

#Use a SQLContext to create a DataFrame from different data sources (S3, JSON, RDBMS, HDFS, Cassandra, etc.)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.csv("/FileStore/tables/creditcard.csv")

# Displays the content of the DataFrame to stdout
df.show()

# COMMAND ----------

# common operations
df.select("_c1").show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select(df['_c1'], df['_c2'] + 1).show()

# COMMAND ----------

df.filter(df['_c1'] > 2).show()

# COMMAND ----------

df.groupBy("_c1").count().show()

# COMMAND ----------

#####2nd part
df.cache()

# COMMAND ----------

#### 3rd part
df.rdd.getNumPartitions()

# COMMAND ----------

df.rdd.repartition(4)

# COMMAND ----------

#### 4th part
path = "/FileStore/tables/creditcard.csv"

df = spark.read.csv(path)
df.show()

# COMMAND ----------

#### 5th part
import pandas as pd

#a = pd.read_csv("/FileStore/tables/creditcard.csv")
#b = pd.read_csv("/FileStore/tables/creditcard.csv")
a=pd.DataFrame(j.toPandas())
b=pd.DataFrame(j.toPandas())
print(a)
print(b)

# COMMAND ----------

df_all_rows = pd.concat([a,b])
df_all_rows

# COMMAND ----------

df_all_cols = pd.concat([a, b], axis = 1)
df_all_cols

# COMMAND ----------

df_cd = pd.merge(a, b, how='inner')
df_cd

# COMMAND ----------

df_cd = pd.merge(a, b, how='inner', on = 'V1')

# COMMAND ----------

df_cd = pd.merge(a, b, how='inner', left_on = 'V1', right_on = 'V1')

# COMMAND ----------

empDf = sqlContext.read.csv("/FileStore/tables/creditcard.csv")
empDf.show()
empDf.printSchema()

# COMMAND ----------

empDf.select("_c2").show()

# COMMAND ----------

empDf.filter(empDf["_c3"] == 4).show()

# COMMAND ----------

empDf.groupBy("_c5").count().show()

# COMMAND ----------

##### 6th part
def factorial(x):
    if x == 1:
        return 1
    else:
        return (x * factorial(x-1))
factorial(7)

# COMMAND ----------

#### 7th part
import pandas as pd
a=pd.DataFrame(j.toPandas())
b=pd.DataFrame(j.toPandas())
print(a)
print(b)

# COMMAND ----------

df_all_rows = pd.concat([a,b])
df_all_rows

# COMMAND ----------

df_all_cols = pd.concat([a, b], axis = 1)
df_all_cols

# COMMAND ----------

#### 8th part
#The logical optimization phase applies standard rule-based optimizations to the logical plan. (Cost-based optimization is performed by generating multiple plans using rules, and then computing their costs.) These include constant folding, predicate pushdown, projection pruning, null propagation, Boolean expression simplification, and other rules. In general, we have found it extremely simple to add rules for a wide variety of situations. For example, when we added the fixed-precision DECIMAL type to Spark SQL, we wanted to optimize aggregations such as sums and averages on DECIMALs with small precisions; it took 12 lines of code to write a rule that finds such decimals in SUM and AVG expressions, and casts them to unscaled 64-bit LONGs, does the aggregation on that, then converts the result back. A simplified version of this rule that only optimizes SUM expressions is reproduced below:
object DecimalAggregates extends Rule[LogicalPlan] {
  /** Maximum number of decimal digits in a Long */
  val MAX_LONG_DIGITS = 18
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case Sum(e @ DecimalType.Expression(prec, scale))
          if prec + 10 <= MAX_LONG_DIGITS =>
        MakeDecimal(Sum(UnscaledValue(e)), prec + 10, scale) }
}
#In the physical planning phase, Spark SQL takes a logical plan and generates one or more physical plans, using physical operators that match the Spark execution engine. It then selects a plan using a cost model. At the moment, cost-based optimization is only used to select join algorithms: for relations that are known to be small, Spark SQL uses a broadcast join, using a peer-to-peer broadcast facility available in Spark. The framework supports broader use of cost-based optimization, however, as costs can be estimated recursively for a whole tree using a rule. We thus intend to implement richer cost-based optimization in the future.The physical planner also performs rule-based physical optimizations, such as pipelining projections or filters into one Spark map operation. In addition, it can push operations from the logical plan into data sources that support predicate or projection pushdown. We will describe the API for these data sources in a later section.

# COMMAND ----------

#### 9th part

import matplotlib.pyplot as plt
import pandas as pd
 
# Reading the tips.csv file
data = pd.read_csv('/FileStore/tables/creditcard.csv')
#data=pd.DataFrame(j.toPandas())
# initializing the data
x = data['_c0']
 
# plotting the data
plt.hist(x)
 
# Adding title to the plot
plt.title("_c1")
 
# Adding label on the y-axis
plt.ylabel('Y-Axis')
 
# Adding label on the x-axis
plt.xlabel('X-Axis')
 
plt.show()

# COMMAND ----------

import pandas as pd
data=pd.DataFrame(j.toPandas())
display(data)

# COMMAND ----------

from pyspark.sql import Row
scatterPlotRDD = spark.createDataFrame(sc.parallelize([
  Row(key="k1", a=0.2, b=120, c=1), Row(key="k1", a=0.4, b=140, c=1), Row(key="k1", a=0.6, b=160, c=1), Row(key="k1", a=0.8, b=180, c=1),
  Row(key="k2", a=0.2, b=220, c=1), Row(key="k2", a=0.4, b=240, c=1), Row(key="k2", a=0.6, b=260, c=1), Row(key="k2", a=0.8, b=280, c=1),
  Row(key="k1", a=1.8, b=120, c=1), Row(key="k1", a=1.4, b=140, c=1), Row(key="k1", a=1.6, b=160, c=1), Row(key="k1", a=1.8, b=180, c=1),
  Row(key="k2", a=1.8, b=220, c=2), Row(key="k2", a=1.4, b=240, c=2), Row(key="k2", a=1.6, b=260, c=2), Row(key="k2", a=1.8, b=280, c=2),
  Row(key="k1", a=2.2, b=120, c=1), Row(key="k1", a=2.4, b=140, c=1), Row(key="k1", a=2.6, b=160, c=1), Row(key="k1", a=2.8, b=180, c=1),
  Row(key="k2", a=2.2, b=220, c=3), Row(key="k2", a=2.4, b=240, c=3), Row(key="k2", a=2.6, b=260, c=3), Row(key="k2", a=2.8, b=280, c=3)
]))
display(scatterPlotRDD)

# COMMAND ----------

from pyspark.sql import Row
# Hover over the entry in the histogram to read off the exact valued plotted.
histogramRDD = spark.createDataFrame(sc.parallelize([
  Row(key1="a", key2="x", val=0.2), Row(key1="a", key2="x", val=0.4), Row(key1="a", key2="x", val=0.6), Row(key1="a", key2="x", val=0.8), Row(key1="a", key2="x", val=1.0), 
  Row(key1="b", key2="z", val=0.2), Row(key1="b", key2="x", val=0.4), Row(key1="b", key2="x", val=0.6), Row(key1="b", key2="y", val=0.8), Row(key1="b", key2="x", val=1.0), 
  Row(key1="a", key2="x", val=0.2), Row(key1="a", key2="y", val=0.4), Row(key1="a", key2="x", val=0.6), Row(key1="a", key2="x", val=0.8), Row(key1="a", key2="x", val=1.0), 
  Row(key1="b", key2="x", val=0.2), Row(key1="b", key2="x", val=0.4), Row(key1="b", key2="x", val=0.6), Row(key1="b", key2="z", val=0.8), Row(key1="b", key2="x", val=1.0)]))
display(histogramRDD)

# COMMAND ----------

##### Task 3   part 1st

#Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.

#Spark actions are executed through a set of stages, separated by distributed “shuffle” operations. Spark automatically broadcasts the common data needed by tasks within each stage. The data broadcasted this way is cached in serialized form and deserialized before running each task. This means that explicitly creating broadcast variables is only useful when tasks across multiple stages need the same data or when caching the data in deserialized form is important.

#Broadcast variables are created from a variable v by calling SparkContext.broadcast(v). The broadcast variable is a wrapper around v, and its value can be accessed by calling the value method. The code below shows this:
broadcastVar = sc.broadcast([1, 2, 3])
#<pyspark.broadcast.Broadcast object at 0x102789f10>
broadcastVar.value
[1, 2, 3]

# COMMAND ----------

# Accumulators are variables that are only “added” to through an associative and commutative operation and can therefore be efficiently supported in parallel. They can be used to implement counters (as in MapReduce) or sums. Spark natively supports accumulators of numeric types, and programmers can add support for new types. An accumulator is created from an initial value v by calling SparkContext.accumulator(v). Tasks running on a cluster can then add to it using the add method or the += operator. However, they cannot read its value. Only the driver program can read the accumulator’s value, using its value method.
# The code below shows an accumulator being used to add up the elements of an array:
accum = sc.accumulator(0)
accum
#Accumulator<id=0, value=0>

sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accum.add(x))
#...
#10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

accum.value
10

# COMMAND ----------


#### 2nd part
accum=sc.accumulator(0)
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x:accum.add(x))
print(accum.value) #Accessed by driver


# COMMAND ----------

accuSum=spark.sparkContext.accumulator(0)
def countFun(x):
    global accuSum
    accuSum+=x
rdd.foreach(countFun)
print(accuSum.value)

# COMMAND ----------


#### counters
accumCount=spark.sparkContext.accumulator(0)
rdd2=spark.sparkContext.parallelize([1,2,3,4,5])
rdd2.foreach(lambda x:accumCount.add(1))
print(accumCount.value)


# COMMAND ----------


import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("accumulator").getOrCreate()

accum=spark.sparkContext.accumulator(0)
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x:accum.add(x))
print(accum.value)

accuSum=spark.sparkContext.accumulator(0)
def countFun(x):
    global accuSum
    accuSum+=x
rdd.foreach(countFun)
print(accuSum.value)

accumCount=spark.sparkContext.accumulator(0)
rdd2=spark.sparkContext.parallelize([1,2,3,4,5])
rdd2.foreach(lambda x:accumCount.add(1))
print(accumCount.value)


# COMMAND ----------

#### 3rd part

#Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.

#Spark actions are executed through a set of stages, separated by distributed “shuffle” operations. Spark automatically broadcasts the common data needed by tasks within each stage. The data broadcasted this way is cached in serialized form and deserialized before running each task. This means that explicitly creating broadcast variables is only useful when tasks across multiple stages need the same data or when caching the data in deserialized form is important.
broadcastVar = sc.broadcast([1, 2, 3])
#<pyspark.broadcast.Broadcast object at 0x102789f10>

broadcastVar.value
[1, 2, 3]

# COMMAND ----------

#### 4th part
# closures
#One of the harder things about Spark is understanding the scope and life cycle of variables and methods when executing code across a cluster. RDD operations that modify variables outside of their scope can be a frequent source of confusion. In the example below we’ll look at code that uses foreach() to increment a counter, but similar issues can occur for other operations as well.
#Consider the naive RDD element sum below, which may behave differently depending on whether execution is happening within the same JVM. A common example of this is when running Spark in local mode (--master = local[n]) versus deploying a Spark application to a cluster (e.g. via spark-submit to YARN):
def outer(name):
    # this is the enclosing function

    def inner():
        # this is the enclosed function
        # the inner function accessing the outer function's variable 'name'
        print(name)

    return inner

# call the enclosing function
myFunction = outer('TechVidvan')
myFunction()

# COMMAND ----------

# broadcast
#Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost
broadcastVar = sc.broadcast([1, 2, 3])
#<pyspark.broadcast.Broadcast object at 0x102789f10>
broadcastVar.value
[1, 2, 3]

# COMMAND ----------

###### Task 4
#### part 1st
#Machine Learning
#Spark use cases with its machine learning capabilities.Spark comes with an integrated framework for performing advanced analytics that helps users run repeated queries on sets of data—which essentially amounts to processing machine learning algorithms. Among the components found in this framework is Spark’s scalable Machine Learning Library (MLlib). The MLlib can work in areas such as clustering, classification, and dimensionality reduction, among many others. All this enables Spark to be used for some very common big data functions, like predictive intelligence, customer segmentation for marketing purposes, and sentiment analysis. Companies that use a recommendation engine will find that Spark gets the job done fast.Network security is a good business case for Spark’s machine learning capabilities. Utilizing various components of the Spark stack, security providers can conduct real-time inspections of data packets for traces of malicious activity. At the front end, Spark Streaming allows security analysts to check against known threats prior to passing the packets on to the storage platform. Upon arrival in storage, the packets undergo further analysis via other stack components such as MLlib. Thus security providers can learn about new threats as they evolve—staying ahead of hackers while protecting their clients in real-time.
# Databricks notebook source
from pyspark.context import SparkContext

# COMMAND ----------

sc=SparkContext.getOrCreate()

# COMMAND ----------


#Load the CSV file into a RDD
irisData = sc.textFile("/FileStore/tables/MlIris.csv")
irisData.persist()


# COMMAND ----------

#Remove the first line (contains headers)
dataLines = irisData.filter(lambda x: "Sepal" not in x)
dataLines.count()

# COMMAND ----------

#Convert the RDD into a Dense Vector. As a part of this exercise
import math
from pyspark.ml.linalg import Vectors, VectorUDT
#from pyspark.ml.linalg import Vectors

# COMMAND ----------

# Change labels to numeric ones

def transformToNumeric( inputStr) :
    attList=inputStr.split(",")
    
    #Set default to setosa
    irisValue=1.0
    if attList[4] == "versicolor":
        irisValue=2.0
    if attList[4] == "virginica":
        irisValue=3.0
       
    #Filter out columns not wanted at this stage
    values= Vectors.dense([ irisValue, \
                     float(attList[0]),  \
                     float(attList[1]),  \
                     float(attList[2]),  \
                     float(attList[3])  \
                     ])
    return values

# COMMAND ----------

#Change to a Vector
irisVectors = dataLines.map(transformToNumeric)
irisVectors.collect()

# COMMAND ----------

#Transform to a Data Frame for input to Machine Learing
#Drop columns that are not required (low correlation)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def transformToLabeledPoint(inStr) :
    attList=inStr.split(",")
    lp = ( attList[4], Vectors.dense([attList[0],attList[2],attList[3]]))
    return lp

# COMMAND ----------

irisLp = dataLines.map(transformToLabeledPoint)
irisDF = sqlContext.createDataFrame(irisLp,["label", "features"])
irisDF.select("label","features").show(10)

# COMMAND ----------

#Indexing needed as pre-req for Decision Trees
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
si_model = stringIndexer.fit(irisDF)
td = si_model.transform(irisDF)
td.collect()

# COMMAND ----------


#Split into training and testing data
(trainingData, testData) = td.randomSplit([0.9, 0.1])
trainingData.collect()
#testData.count()
#testData.collect()

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT
#from pyspark.ml.linalg import Vectors as MLLibVectors
#from pyspark.ml.linalg import VectorUDT
#from pyspark.sql.functions import udf


# COMMAND ----------

#Create the model
dtClassifer = DecisionTreeClassifier(maxDepth=2, labelCol="indexed")
dtModel = dtClassifer.fit(trainingData)
dtModel.numNodes
dtModel.depth


# COMMAND ----------

#Predict on the test data
predictions = dtModel.transform(trainingData)


# COMMAND ----------

predictions.select("prediction","indexed","label","features").collect()

# COMMAND ----------

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="indexed")
evaluator.evaluate(predictions)

# COMMAND ----------

#Draw a confusion matrix
labelList=predictions.select("indexed","label").distinct().toPandas()
predictions.groupBy("indexed","prediction").count().show()

# COMMAND ----------

# part 2nd
#spark.mllib carries the original API built on top of RDDs.
#spark.ml contains higher-level API built on top of DataFrames for constructing ML pipelines.
#However, the spark.ml is considered as the recommended package because with DataFrames the API is more versatile and flexible. But users will keep supporting spark.mllib along with the development of spark.ml. Users should be comfortable using spark.mllib features as for existing algorithms not all of the functionality has been ported over to the new Spark ML API. But it is expected to have more features in the coming time. In Spark 2.0, the RDD-based APIs in the spark.mllib package have entered maintenance mode. The primary Machine Learning API for Spark has now changed to the DataFrame-based API in the spark.ml package. Now mllib is slowly getting deprecated(this already happened in case of linear regression) and most probably will be removed in the next major release.

# COMMAND ----------

### part 3rd
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression

# Prepare training data from a list of (label, features) tuples.
training = spark.createDataFrame([
    (1.0, Vectors.dense([0.0, 1.1, 0.1])),
    (0.0, Vectors.dense([2.0, 1.0, -1.0])),
    (0.0, Vectors.dense([2.0, 1.3, 1.0])),
    (1.0, Vectors.dense([0.0, 1.2, -0.5]))], ["label", "features"])

# Create a LogisticRegression instance. This instance is an Estimator.
lr = LogisticRegression(maxIter=10, regParam=0.01)
# Print out the parameters, documentation, and any default values.
print("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

# Learn a LogisticRegression model. This uses the parameters stored in lr.
model1 = lr.fit(training)

# Since model1 is a Model (i.e., a transformer produced by an Estimator),
# we can view the parameters it used during fit().
# This prints the parameter (name: value) pairs, where names are unique IDs for this
# LogisticRegression instance.
print("Model 1 was fit using parameters: ")
print(model1.extractParamMap())

# We may alternatively specify parameters using a Python dictionary as a paramMap
paramMap = {lr.maxIter: 20}
paramMap[lr.maxIter] = 30  # Specify 1 Param, overwriting the original maxIter.
# Specify multiple Params.
paramMap.update({lr.regParam: 0.1, lr.threshold: 0.55})  # type: ignore

# You can combine paramMaps, which are python dictionaries.
# Change output column name
paramMap2 = {lr.probabilityCol: "myProbability"}
paramMapCombined = paramMap.copy()
paramMapCombined.update(paramMap2)  # type: ignore

# Now learn a new model using the paramMapCombined parameters.
# paramMapCombined overrides all parameters set earlier via lr.set* methods.
model2 = lr.fit(training, paramMapCombined)
print("Model 2 was fit using parameters: ")
print(model2.extractParamMap())

# Prepare test data
test = spark.createDataFrame([
    (1.0, Vectors.dense([-1.0, 1.5, 1.3])),
    (0.0, Vectors.dense([3.0, 2.0, -0.1])),
    (1.0, Vectors.dense([0.0, 2.2, -1.5]))], ["label", "features"])

# Make predictions on test data using the Transformer.transform() method.
# LogisticRegression.transform will only use the 'features' column.
# Note that model2.transform() outputs a "myProbability" column instead of the usual
# 'probability' column since we renamed the lr.probabilityCol parameter previously.
prediction = model2.transform(test)
result = prediction.select("features", "label", "myProbability", "prediction") \
    .collect()

for row in result:
    print("features=%s, label=%s -> prob=%s, prediction=%s"
          % (row.features, row.label, row.myProbability, row.prediction))

# COMMAND ----------

# part 4th
import numpy as np

from pyspark.mllib.stat import Statistics

mat = sc.parallelize(
    [np.array([1.0, 10.0, 100.0]), np.array([2.0, 20.0, 200.0]), np.array([3.0, 30.0, 300.0])]
)  # an RDD of Vectors

# Compute column summary statistics.
summary = Statistics.colStats(mat)
print(summary.mean())  # a dense vector containing the mean value for each column
print(summary.variance())  # column-wise variance
print(summary.numNonzeros())  # number of nonzeros in each column

# COMMAND ----------

### 5th part
####LogisticRegression
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
%matplotlib inline

df = pd.read_csv('/FileStore/tables/creditcard.csv')
print(df.shape)
df.head()
df.info()
df.describe()
class_names = {0:'Not Fraud', 1:'Fraud'}
print(df.Class.value_counts().rename(index = class_names))
from sklearn.cross_validation import train_test_split
feature_names = df.iloc[:, 1:30].columns
target = df.iloc[:1, 30: ].columns
print(feature_names)
print(target)
data_features = df[feature_names]
data_target = df[target]
X_train, X_test, y_train, y_test = train_test_split(data_features, data_target, train_size=0.70, test_size=0.30, random_state=1)
print("Length of X_train is: {X_train}".format(X_train = len(X_train)))
print("Length of X_test is: {X_test}".format(X_test = len(X_test)))
print("Length of y_train is: {y_train}".format(y_train = len(y_train)))
print("Length of y_test is: {y_test}".format(y_test = len(y_test)))
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
model = LogisticRegression()
model.fit(X_train, y_train.values.ravel())
pred = model.predict(X_test)
class_names = ['not_fraud', 'fraud']
matrix = confusion_matrix(y_test, pred)
# Create pandas dataframe
dataframe = pd.DataFrame(matrix, index=class_names, columns=class_names)
# Create heatmap
sns.heatmap(dataframe, annot=True, cbar=None, cmap="Blues", fmt = 'g')
plt.title("Confusion Matrix"), plt.tight_layout()
plt.ylabel("True Class"), plt.xlabel("Predicted Class")
plt.show()
from sklearn.metrics import f1_score, recall_score
f1_score = round(f1_score(y_test, pred), 2)
recall_score = round(recall_score(y_test, pred), 2)
print("Sensitivity/Recall for Logistic Regression Model 1 : {recall_score}".format(recall_score = recall_score))
print("F1 Score for Logistic Regression Model 1 : {f1_score}".format(f1_score = f1_score))

# COMMAND ----------

### RandomForestClassifier
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
%matplotlib inline
import seaborn as sns
df = pd.read_csv('/FileStore/tables/creditcard.csv')
df.head()
df.columns
df.shape
df.info()
df.describe()
# Null check
(df.isnull().sum() * 100 / len(df)).sort_values(ascending=False)
sns.pairplot(df)
plt.show()
plt.figure(figsize = (35, 35))
cor=df.corr()
sns.heatmap(cor, annot = True, cmap="BuPu")
plt.show()
# Importing test_train_split from sklearn library
from sklearn.model_selection import train_test_split

# We specify this so that the train and test data set always have the same rows, respectively
np.random.seed(0)
X = df.iloc[:, 1:30]
y = df.iloc[:, -1]
# Splitting the data into train and test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=101)
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
X_train.head()
# Importing random forest classifier from sklearn library
from sklearn.ensemble import RandomForestClassifier

# Running the random forest with default parameters.
rfc = RandomForestClassifier()
# fit
rfc.fit(X_train,y_train)
# Making predictions
predictions = rfc.predict(X_test)
# Importing classification report and confusion matrix from sklearn metrics
from sklearn.metrics import classification_report,confusion_matrix, accuracy_score
from sklearn.metrics import roc_auc_score
# Let's check the report of our default model
print(classification_report(y_test,predictions))
# Printing confusion matrix
print(confusion_matrix(y_test,predictions))
print('Accuracy score:',accuracy_score(y_test,predictions))
# print(accuracy_score(y_test,predictions))
print('ROC Accuracy:', roc_auc_score(y_test,predictions))

# COMMAND ----------

###naive-bayes
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix,auc,roc_auc_score
from sklearn.metrics import recall_score, precision_score, accuracy_score, f1_score
# Data Handling: Load CSV
df = pd.read_csv("/FileStore/tables/creditcard.csv")

# get to know list of features, data shape, stat. description.
print(df.shape)

print("First 5 lines:")
print(df.head(5))

print("describe: ")
print(df.describe())

print("info: ")
print(df.info())

"""Since all variables are of float and int type, so this data is easy to handle for modeling"""
# Check Class variables that has 0 value for Genuine transactions and 1 for Fraud
print("Class as pie chart:")
fig, ax = plt.subplots(1, 1)
ax.pie(df.Class.value_counts(),autopct='%1.1f%%', labels=['Genuine','Fraud'], colors=['yellowgreen','r'])
plt.axis('equal')
plt.ylabel('')
#plot Time to see if there is any trend
print("Time variable")
df["Time_Hr"] = df["Time"]/3600 # convert to hours
print(df["Time_Hr"].tail(5))
fig, (ax1, ax2) = plt.subplots(2, 1, sharex = True, figsize=(6,3))
ax1.hist(df.Time_Hr[df.Class==0],bins=48,color='g',alpha=0.5)
ax1.set_title('Genuine')
ax2.hist(df.Time_Hr[df.Class==1],bins=48,color='r',alpha=0.5)
ax2.set_title('Fraud')
plt.xlabel('Time (hrs)')
plt.ylabel('# transactions')
df = df.drop(['Time'],axis=1)
#let us check another feature Amount
fig, (ax3,ax4) = plt.subplots(2,1, figsize = (6,3), sharex = True)
ax3.hist(df.Amount[df.Class==0],bins=50,color='g',alpha=0.5)
ax3.set_yscale('log') # to see the tails
ax3.set_title('Genuine') # to see the tails
ax3.set_ylabel('# transactions')
ax4.hist(df.Amount[df.Class==1],bins=50,color='r',alpha=0.5)
ax4.set_yscale('log') # to see the tails
ax4.set_title('Fraud') # to see the tails
ax4.set_xlabel('Amount ($)')
ax4.set_ylabel('# transactions')
from sklearn.preprocessing import StandardScaler
df['scaled_Amount'] = StandardScaler().fit_transform(df['Amount'].values.reshape(-1,1))
df = df.drop(['Amount'],axis=1)
#let us check correlations and shapes of those 25 principal components.
# Features V1, V2, ... V28 are the principal components obtained with PCA.
import seaborn as sns
import matplotlib.gridspec as gridspec
gs = gridspec.GridSpec(28, 1)
plt.figure(figsize=(6,28*4))
for i, col in enumerate(df[df.iloc[:,0:28].columns]):
    ax5 = plt.subplot(gs[i])
    sns.distplot(df[col][df.Class == 1], bins=50, color='r')
    sns.distplot(df[col][df.Class == 0], bins=50, color='g')
    ax5.set_xlabel('')
    ax5.set_title('feature: ' + str(col))
plt.show()
def split_data(df, drop_list):
    df = df.drop(drop_list,axis=1)
    print(df.columns)
    #test train split time
    from sklearn.model_selection import train_test_split
    y = df['Class'].values #target
    X = df.drop(['Class'],axis=1).values #features
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                    random_state=42, stratify=y)

    print("train-set size: ", len(y_train),
      "\ntest-set size: ", len(y_test))
    print("fraud cases in test-set: ", sum(y_test))
    return X_train, X_test, y_train, y_test
def get_predictions(clf, X_train, y_train, X_test):
    # create classifier
    clf = clf
    # fit it to training data
    clf.fit(X_train,y_train)
    # predict using test data
    y_pred = clf.predict(X_test)
    # Compute predicted probabilities: y_pred_prob
    y_pred_prob = clf.predict_proba(X_test)
    #for fun: train-set predictions
    train_pred = clf.predict(X_train)
    print('train-set confusion matrix:\n', confusion_matrix(y_train,train_pred)) 
    return y_pred, y_pred_prob
def print_scores(y_test,y_pred,y_pred_prob):
    print('test-set confusion matrix:\n', confusion_matrix(y_test,y_pred)) 
    print("recall score: ", recall_score(y_test,y_pred))
    print("precision score: ", precision_score(y_test,y_pred))
    print("f1 score: ", f1_score(y_test,y_pred))
    print("accuracy score: ", accuracy_score(y_test,y_pred))
    print("ROC AUC: {}".format(roc_auc_score(y_test, y_pred_prob[:,1])))
from sklearn.naive_bayes import GaussianNB
from sklearn.linear_model import LogisticRegression
# Case-NB-1 : do not drop anything
drop_list = []
X_train, X_test, y_train, y_test = split_data(df, drop_list)
y_pred, y_pred_prob = get_predictions(GaussianNB(), X_train, y_train, X_test)
print_scores(y_test,y_pred,y_pred_prob)
# Case-NB-2 : drop some of principle components that have similar distributions in above plots 
drop_list = ['V28','V27','V26','V25','V24','V23','V22','V20','V15','V13','V8']
X_train, X_test, y_train, y_test = split_data(df, drop_list)
y_pred, y_pred_prob = get_predictions(GaussianNB(), X_train, y_train, X_test)
print_scores(y_test,y_pred,y_pred_prob)
# Case-NB-3 : drop some of principle components + Time 
drop_list = ['Time_Hr','V28','V27','V26','V25','V24','V23','V22','V20','V15','V13','V8']
X_train, X_test, y_train, y_test = split_data(df, drop_list)
y_pred, y_pred_prob = get_predictions(GaussianNB(), X_train, y_train, X_test)
print_scores(y_test,y_pred,y_pred_prob)
# Case-NB-4 : drop some of principle components + Time + 'scaled_Amount'
drop_list = ['scaled_Amount','Time_Hr','V28','V27','V26','V25','V24','V23','V22','V20','V15','V13','V8']
X_train, X_test, y_train, y_test = split_data(df, drop_list)
y_pred, y_pred_prob = get_predictions(GaussianNB(), X_train, y_train, X_test)
print_scores(y_test,y_pred,y_pred_prob)
df = df.drop(drop_list,axis=1)
print(df.columns)
# let us check recall score for logistic regression
# Case-LR-1
y_pred, y_pred_prob = get_predictions(LogisticRegression(C = 0.01, penalty = 'l1')
                                      , X_train, y_train, X_test)
print_scores(y_test,y_pred,y_pred_prob)
# get indices for fraud and genuine classes 
fraud_ind = np.array(df[df.Class == 1].index)
gen_ind = df[df.Class == 0].index
n_fraud = len(df[df.Class == 1])
# random selection from genuine class
random_gen_ind = np.random.choice(gen_ind, n_fraud, replace = False)
random_gen_ind = np.array(random_gen_ind)
# merge two class indices: random genuine + original fraud
under_sample_ind = np.concatenate([fraud_ind,random_gen_ind])
# Under sample dataset
undersample_df = df.iloc[under_sample_ind,:]
y_undersample  = undersample_df['Class'].values #target
X_undersample = undersample_df.drop(['Class'],axis=1).values #features

print("# transactions in undersampled data: ", len(undersample_df))
print("% genuine transactions: ",len(undersample_df[undersample_df.Class == 0])/len(undersample_df))
print("% fraud transactions: ", sum(y_undersample)/len(undersample_df))
# let us train logistic regression with undersamples data
# Case-LR-2
# split undersampled data into 80/20 train-test datasets. 
# - Train model from this 80% fraction of undersampled data, get predictions from left over i.e. 20%.
drop_list = []
X_und_train, X_und_test, y_und_train, y_und_test = split_data(undersample_df, drop_list)
y_und_pred, y_und_pred_prob = get_predictions(LogisticRegression(C = 0.01, penalty = 'l1'), X_und_train, y_und_train, X_und_test)
print_scores(y_und_test,y_und_pred,y_und_pred_prob)
# Case-LR-3
# "train" with undersamples, "test" with full data
# call classifier
lr = LogisticRegression(C = 0.01, penalty = 'l1')
# fit it to complete undersampled data
lr.fit(X_undersample, y_undersample)
# predict on full data
y_full = df['Class'].values #target
X_full = df.drop(['Class'],axis=1).values #features
y_full_pred = lr.predict(X_full)
# Compute predicted probabilities: y_pred_prob
y_full_pred_prob = lr.predict_proba(X_full)
print("scores for Full set")   
print('test-set confusion matrix:\n', confusion_matrix(y_full,y_full_pred)) 
print("recall score: ", recall_score(y_full,y_full_pred))
print("precision score: ", precision_score(y_full,y_full_pred))
# Case-LR-4
y_p20_pred = lr.predict(X_test)
y_p20_pred_prob = lr.predict_proba(X_test)
print("scores for test (20% of full) set")   
print('test-set confusion matrix:\n', confusion_matrix(y_test,y_p20_pred)) 
print("recall score: ", recall_score(y_test,y_p20_pred))
print("precision score: ", precision_score(y_test,y_p20_pred))

# COMMAND ----------

### Boosting models
# Libraries
library(pROC, quietly=TRUE)
library(microbenchmark, quietly=TRUE)

# Set seed so the train/test split is reproducible
set.seed(42)

# Read in the data and split it into train/test subsets
credit.card.data = read.csv("/FileStore/tables/creditcard.csv")

train.test.split <- sample(2
	, nrow(credit.card.data)
	, replace = TRUE
	, prob = c(0.7, 0.3))
train = credit.card.data[train.test.split == 1,]
test = credit.card.data[train.test.split == 2,]
library(gbm, quietly=TRUE)

# Get the time to train the GBM model
system.time(
	gbm.model <- gbm(Class ~ .
		, distribution = "bernoulli"
		, data = rbind(train, test)
		, n.trees = 500
		, interaction.depth = 3
		, n.minobsinnode = 100
		, shrinkage = 0.01
		, bag.fraction = 0.5
		, train.fraction = nrow(train) / (nrow(train) + nrow(test))
		)
)
# Determine best iteration based on test data
best.iter = gbm.perf(gbm.model, method = "test")

# Get feature importance
gbm.feature.imp = summary(gbm.model, n.trees = best.iter)

# Plot and calculate AUC on test data
gbm.test = predict(gbm.model, newdata = test, n.trees = best.iter)
auc.gbm = roc(test$Class, gbm.test, plot = TRUE, col = "red")
print(auc.gbm)
library(xgboost, quietly=TRUE)
xgb.data.train <- xgb.DMatrix(as.matrix(train[, colnames(train) != "Class"]), label = train$Class)
xgb.data.test <- xgb.DMatrix(as.matrix(test[, colnames(test) != "Class"]), label = test$Class)

# Get the time to train the xgboost model
xgb.bench.speed = microbenchmark(
	xgb.model.speed <- xgb.train(data = xgb.data.train
		, params = list(objective = "binary:logistic"
			, eta = 0.1
			, max.depth = 3
			, min_child_weight = 100
			, subsample = 1
			, colsample_bytree = 1
			, nthread = 3
			, eval_metric = "auc"
			)
		, watchlist = list(test = xgb.data.test)
		, nrounds = 500
		, early_stopping_rounds = 40
		, print_every_n = 20
		)
    , times = 5L
)
print(xgb.bench.speed)
print(xgb.model.speed$bestScore)

# Make predictions on test set for ROC curve
xgb.test.speed = predict(xgb.model.speed
                   , newdata = as.matrix(test[, colnames(test) != "Class"])
                   , ntreelimit = xgb.model.speed$bestInd)
auc.xgb.speed = roc(test$Class, xgb.test.speed, plot = TRUE, col = "blue")
print(auc.xgb.speed)
# Train a deeper xgboost model to compare accuarcy.
xgb.bench.acc = microbenchmark(
	xgb.model.acc <- xgb.train(data = xgb.data.train
		, params = list(objective = "binary:logistic"
			, eta = 0.1
			, max.depth = 7
			, min_child_weight = 100
			, subsample = 1
			, colsample_bytree = 1
			, nthread = 3
			, eval_metric = "auc"
			)
		, watchlist = list(test = xgb.data.test)
		, nrounds = 500
		, early_stopping_rounds = 40
		, print_every_n = 20
		)
    , times = 5L
)
print(xgb.bench.acc)
print(xgb.model.acc$bestScore)

#Get feature importance
xgb.feature.imp = xgb.importance(model = xgb.model.acc)

# Make predictions on test set for ROC curve
xgb.test.acc = predict(xgb.model.acc
                   , newdata = as.matrix(test[, colnames(test) != "Class"])
                   , ntreelimit = xgb.model.acc$bestInd)
auc.xgb.acc = roc(test$Class, xgb.test.acc, plot = TRUE, col = "blue")
print(auc.xgb.acc)
# xgBoost with Histogram
xgb.bench.hist = microbenchmark(
	xgb.model.hist <- xgb.train(data = xgb.data.train
		, params = list(objective = "binary:logistic"
			, eta = 0.1
			, max.depth = 7
			, min_child_weight = 100
			, subsample = 1
			, colsample_bytree = 1
			, nthread = 3
			, eval_metric = "auc"
            , tree_method = "hist"
            , grow_policy = "lossguide"
			)
		, watchlist = list(test = xgb.data.test)
		, nrounds = 500
		, early_stopping_rounds = 40
		, print_every_n = 20
		)
    , times = 5L
)
print(xgb.bench.hist)
print(xgb.model.hist$bestScore)

#Get feature importance
xgb.feature.imp = xgb.importance(model = xgb.model.hist)

# Make predictions on test set for ROC curve
xgb.test.hist = predict(xgb.model.hist
                   , newdata = as.matrix(test[, colnames(test) != "Class"])
                   , ntreelimit = xgb.model.hist$bestInd)
auc.xgb.hist = roc(test$Class, xgb.test.hist, plot = TRUE, col = "blue")
print(auc.xgb.hist)
library(lightgbm, quietly=TRUE)
lgb.train = lgb.Dataset(as.matrix(train[, colnames(train) != "Class"]), label = train$Class)
lgb.test = lgb.Dataset(as.matrix(test[, colnames(test) != "Class"]), label = test$Class)

params.lgb = list(
	objective = "binary"
	, metric = "auc"
	, min_data_in_leaf = 1
	, min_sum_hessian_in_leaf = 100
	, feature_fraction = 1
	, bagging_fraction = 1
	, bagging_freq = 0
	)

# Get the time to train the lightGBM model
lgb.bench = microbenchmark(
	lgb.model <- lgb.train(
		params = params.lgb
		, data = lgb.train
		, valids = list(test = lgb.test)
		, learning_rate = 0.1
		, num_leaves = 7
		, num_threads = 2
		, nrounds = 500
		, early_stopping_rounds = 40
		, eval_freq = 20
		)
		, times = 5L
)
print(lgb.bench)
print(max(unlist(lgb.model$record_evals[["test"]][["auc"]][["eval"]])))

# get feature importance
lgb.feature.imp = lgb.importance(lgb.model, percentage = TRUE)

# make test predictions
lgb.test = predict(lgb.model, data = as.matrix(test[, colnames(test) != "Class"]), n = lgb.model$best_iter)
auc.lgb = roc(test$Class, lgb.test, plot = TRUE, col = "green")
print(auc.lgb)
print("GBM = ~243s")
print(xgb.bench.speed)
print(xgb.bench.hist)
print(lgb.bench)
print(auc.gbm)
print(auc.xgb.acc)
print(auc.xgb.hist)
print(auc.lgb)
print(gbm.feature.imp)
print(lgb.feature.imp)

# COMMAND ----------

##### part 6th
from pyspark.context import SparkContext

# COMMAND ----------

sc=SparkContext.getOrCreate()

# COMMAND ----------


#Load the CSV file into a RDD
irisData = sc.textFile("/FileStore/tables/MlIris.csv")
irisData.persist()


# COMMAND ----------

#Remove the first line (contains headers)
dataLines = irisData.filter(lambda x: "Sepal" not in x)
dataLines.count()

# COMMAND ----------

#Convert the RDD into a Dense Vector. As a part of this exercise
import math
from pyspark.ml.linalg import Vectors, VectorUDT
#from pyspark.ml.linalg import Vectors

# COMMAND ----------

# Change labels to numeric ones

def transformToNumeric( inputStr) :
    attList=inputStr.split(",")
    
    #Set default to setosa
    irisValue=1.0
    if attList[4] == "versicolor":
        irisValue=2.0
    if attList[4] == "virginica":
        irisValue=3.0
       
    #Filter out columns not wanted at this stage
    values= Vectors.dense([ irisValue, \
                     float(attList[0]),  \
                     float(attList[1]),  \
                     float(attList[2]),  \
                     float(attList[3])  \
                     ])
    return values

# COMMAND ----------

#Change to a Vector
irisVectors = dataLines.map(transformToNumeric)
irisVectors.collect()

# COMMAND ----------

#Transform to a Data Frame for input to Machine Learing
#Drop columns that are not required (low correlation)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def transformToLabeledPoint(inStr) :
    attList=inStr.split(",")
    lp = ( attList[4], Vectors.dense([attList[0],attList[2],attList[3]]))
    return lp

# COMMAND ----------

irisLp = dataLines.map(transformToLabeledPoint)
irisDF = sqlContext.createDataFrame(irisLp,["label", "features"])
irisDF.select("label","features").show(10)

# COMMAND ----------

#Indexing needed as pre-req for Decision Trees
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
si_model = stringIndexer.fit(irisDF)
td = si_model.transform(irisDF)
td.collect()

# COMMAND ----------


#Split into training and testing data
(trainingData, testData) = td.randomSplit([0.9, 0.1])
trainingData.collect()
#testData.count()
#testData.collect()

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT
#from pyspark.ml.linalg import Vectors as MLLibVectors
#from pyspark.ml.linalg import VectorUDT
#from pyspark.sql.functions import udf


# COMMAND ----------

#Create the model
dtClassifer = DecisionTreeClassifier(maxDepth=2, labelCol="indexed")
dtModel = dtClassifer.fit(trainingData)
dtModel.numNodes
dtModel.depth


# COMMAND ----------

#Predict on the test data
predictions = dtModel.transform(trainingData)


# COMMAND ----------

predictions.select("prediction","indexed","label","features").collect()

# COMMAND ----------

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="indexed")
evaluator.evaluate(predictions)

# COMMAND ----------

#Draw a confusion matrix
labelList=predictions.select("indexed","label").distinct().toPandas()
predictions.groupBy("indexed","prediction").count().show()

# COMMAND ----------


