package app


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum



object SparkJob {
  def main(args: Array[String]): Unit = {
    this.run()
  }

  //From docs, not sure yet what to put these values
  val regressionParam = 0.3
  val trainingStream = "household"
  val testStream = ""
  val kafkaBroker = "kafka:9092"

  def run(): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("spark-application")
      .getOrCreate()

    // Load training and test data and cache it.
    this.calculateLinearRegression(spark)

    spark.stop()
  }


  //  /**
  //   * Taken from https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/DecisionTreeExample.scala
  //   *
  //   * Evaluate the given RegressionModel on data. Print the results.
  //   * @param model  Must fit RegressionModel abstraction
  //   * @param data  DataFrame with "prediction" and labelColName columns
  //   * @param labelColName  Name of the labelCol parameter for the model
  //   *
  //   */
  //  private def evaluateRegressionModel(model: Transformer, data: DataFrame, labelColName: String): Unit = {
  //    val fullPredictions = model.transform(data).cache()
  //    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
  //    val labels = fullPredictions.select(labelColName).rdd.map(_.getDouble(0))
  //    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
  //    println(s"  Root mean squared error (RMSE): $RMSE")
  //  }
  //
  /** Load a dataset from the given topic, using the given format */
  private def streamData(spark: SparkSession): Dataset[(String, String)] = {
    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", trainingStream)
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
  }

  /**
   * Calculate linear regression
   *
   * @return  (a float, b float)
   */
  private def calculateLinearRegression(spark: SparkSession): Unit = {

    //read the dataset
    var data = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("../historical_data_generator/historial_data_output/output.csv")


    import spark.implicits._
    import org.apache.spark.sql._

    val newDf = data.groupBy("device_id")
      .agg(avg("usage") as "usage_mean",
        avg("time_left") as "time_left_mean",
        avg("battery_level") as "battery_level_mean",
        sum(col("usage")*col("time_left")) as "sum1",
        sum(col("usage")*col("usage")) as "sum2",
        sum(col("battery_level")*col("time_left")) as "sum3",
        sum(col("battery_level")*col("battery_level")) as "sum4",
        sum(col("usage")*col("battery_level")) as "sum5",
        count("*") as "size"
      )

    // Show intermediate data
    //newDf.show()


    // Calculate the cross-deviation of the variables:
    // FORMULA   1) SS_x1y = sum(x1,y) - n * sum(x1_mean) * sum(y_mean)
    //           2) SS_x2y = sum(x2,y) - n * sum(x2_mean) * sum(y_mean)
    //			 3) SS_x1x2 = sum(x1,x2) - n * sum(x1_mean) * sum(x2_mean)
    // x1 = usage, x2 = battery_level, y = time_left
    val cross = newDf
      .withColumn("SS_x1y",  newDf("sum1") - newDf("size") * newDf("usage_mean") * newDf("time_left_mean"))
      .withColumn("SS_x2y",  newDf("sum3") - newDf("size") * newDf("battery_level_mean") * newDf("time_left_mean"))
      .withColumn("SS_x1x2", newDf("sum5") - newDf("size") * newDf("usage_mean") * newDf("battery_level_mean"))

    // Calculate regression coefficients:
    // FORMULA   1) b_1 = SS_xy / SS_xx
    //           2) b_2 = m_y - b_1*m_x
    //           3) a
    // a = free coefficient, b1 = usage coefficient, b2 = battery level coefficient
    val b1b2 = cross
      .withColumn("b_1", (cross("sum4")*cross("SS_x1y") - cross("SS_x1x2")*cross("SS_x2y") ) /
        (cross("sum2")*cross("sum4") - cross("SS_x1x2")*cross("SS_x1x2") )
      )
      .withColumn("b_2", (cross("sum4")*cross("SS_x2y") - cross("SS_x1x2")*cross("SS_x1y") ) /
        (cross("sum2")*cross("sum4") - cross("SS_x1x2")*cross("SS_x1x2") )
      )
    val a = b1b2
      .withColumn("a", b1b2("time_left_mean") - b1b2("b_1")*b1b2("usage_mean") - b1b2("b_2")*b1b2("battery_level_mean"))

    // Show full table
    //a.show

    // Create the table with coefficients based on the formula:
    // y_pred = a + b1*x1 + b2*x2


    val coefs = a
      .withColumn("coef_usage", col("b_1"))
      .withColumn("coef_battery", col("b_2"))
      .withColumn("free_coef", col("a"))

    //Hypothesis
    // y_pred = a + b1*x1 + b2*x2

    val Hypothesis = coefs
      .withColumn("Hypothesis", coefs("free_coef") + coefs("coef_usage")*coefs("usage_mean") + coefs("coef_battery")*coefs("battery_level_mean"))

    //Cost Function

    val ITERATION = 500
    //M = 2(calculated depending upon the features - usage and battery_level)
    //M1 for costfunction 1 / 2*M = 0.25(as per formula)
    val M1 = 0.25
    //M1 for gradient  1 / 2 = 0.25
    val M2 = 0.5
    val ALPHA = 0.01

    val device_costfunction = Hypothesis.join(Hypothesis.groupBy("device_id")
      .agg(
      sum(col("Hypothesis")*col("time_left_mean")) as "costfunction",
        count("*") as "size"

    ), "device_id")




    val costfunction = device_costfunction
      .withColumn("costfunction", device_costfunction("Hypothesis") * device_costfunction("Hypothesis") * M1)

    //Stochatic Gradient desent algorithm
    val gradient = costfunction
      .withColumn("batch_id", lit(0))
      .withColumn("theta0", costfunction("free_coef") * costfunction("costfunction") * M2 * ALPHA)
      .withColumn("theta1", costfunction("coef_usage") * costfunction("costfunction") * costfunction("usage_mean") * M2 * ALPHA )
      .withColumn("theta2", costfunction("coef_battery") * costfunction("costfunction") * costfunction("battery_level_mean") * M2 * ALPHA)



    // TODO: check the return value
    gradient.select("batch_id","theta0", "theta1", "theta2")

    // Because there might be multiple devices, average the values
    val predictor = gradient.groupBy("batch_id")
      .agg(avg("theta0") as "gradient_usage",
        avg("theta1") as "gradient_battery",
        avg("theta2") as "free_gradient"
      )
    predictor
  }


  // Where x is the energy usage and y is time till dying, n is the size of the
  //A is (SUM(y)*SUM(x^2) - SUM(X)*SUM(xy))/(n*SUM(x^2) - SUM(x)^2)-
  //B is (n*SUM(xy) - SUM(y)*SUM(x)) / (n*SUM(x^2) - SUM(x)^2)
  //Regression is of the form y = a + b*x
  //       val a = data.agg(sum("steps")).first.get(0)
  //    val b = data.agg(sum("steps")).first.get(0)
  //
  //    print("A is : ")
  //    print(a)
  //    print("B is: ")
  //    print(b)


}