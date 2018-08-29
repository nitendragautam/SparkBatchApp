package com.nitendragautam.sparkbatchapp.services


import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Spark Services
  */
class SparkServices extends Serializable{
  val accessLogsParser = new AccessLogsParser
  private val logger: Logger =
    LoggerFactory.getLogger(classOf[SparkServices])
  val tapiStart =System.currentTimeMillis()

  logger.info("Spark Batch Job Start Time: " +tapiStart)
  logger.info("Spark Batch Job Start Time: " +getTodaysDate())
  def startSparkBatchCluster(inputFile :String, outputFile :String) {
    val conf = new SparkConf().setAppName("SparkBatchApp" +getTodaysDate())


    val sc = new SparkContext(conf)

    //Read Input File and cache It
    val accessLogs = sc.textFile(inputFile)

    val accessLogsRDD =
      accessLogs.map(f =>
        accessLogsParser.parseAccessLogs(f)).cache()

    //Calculate the Client IP address which came more than 10 times
    val ipAddress = accessLogsRDD.map(_.clientAddress -> 1L)
      .reduceByKey(_ + _) //add the number of Occurens
      .cache()
    //Save the Results as singleText File
    ipAddress.coalesce(1).saveAsTextFile(outputFile)



    sc.stop() //Stopping Spark batch
    val tapiEnd =System.currentTimeMillis()

    logger.info("Spark Batch Job End Time: " +tapiEnd)
    logger.info("Spark Batch Job End Time: " +getTodaysDate())
    val elaspedTimeApi = (tapiEnd -tapiStart)/1000
    logger.info(" Spark Batch Job elapsedTime in Seconds: "+elaspedTimeApi)
    println("Spark Batch Job End Time: " + getTodaysDate)
  }
  private def getTodaysDate(): String ={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,0)
    dateFormat.format(cal.getTime())
  }
}

