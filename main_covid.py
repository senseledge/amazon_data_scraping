from pyspark.sql import SparkSession
from pyspark.ml.feature import RFormula
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

# Create the Spark Session context
spark = SparkSession.builder.appName("Covid_forecast").getOrCreate()

# Read the data
data = spark.read.csv('covid_dataset/train.csv', header=True, inferSchema=True)

# Create a Window partion by Id order by Date
w = Window.partitionBy('Id').orderBy('Date')

# Tracking the TargetValue of the previous day
data = data.withColumn('PreviousDay', func.lag(data.TargetValue).over(w))

# Handle null values
data = data.na.fill('na')

# Vectorize the feature with the RFormula
assemblerFormula = RFormula(formula='TargetValue ~ County + Province_State + Country_Region + Population + Weight + Date + PreviousDay + Target')
assemblerFormula.setHandleInvalid('keep')
trainingTF = assemblerFormula.fit(data)

dataR = trainingTF.transform(data).select('Id', 'Date', 'features', 'label')

# Split the training and test dataset
train = dataR.where(data.Date < '2020-04-27')
test = dataR.where(data.Date >= '2020-04-27')

# Init the Decision Tree Regressor
dt_model = DecisionTreeRegressor(featuresCol="features")

# Train the chosen model
trained_model = dt_model.fit(train)

# Make the predictions
predictions = trained_model.transform(test)

predictions.show(truncate=False)

# Plot the results
pd_predictions = predictions.orderBy('Date').toPandas()

dates = pd_predictions['Date']
actual = pd_predictions['label']
predicted = pd_predictions['prediction']

fig, ax = plt.subplots()

actual_plot = ax.plot(dates, actual, label='Actual cases')
predicted_plot = ax.plot(dates, predicted, label='Predicted cases')

ax.legend()
plt.show()

# Evaluate the model
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="mae")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)


