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

  def startSparkStreamingCluster(inputFile :String ,outputFile :String) {
    val conf = new SparkConf().setAppName("SparkStreamingApp")


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
                   // .filter(_._2 >5 ) //Client Ip address >10 times
                     //.map(_._1) //Map the Client IP address count to
                     //.take(100)

//Save the Results as singleText File
    ipAddress.coalesce(1).saveAsTextFile(outputFile + getTodaysDate())



sc.stop() //Stopping Spark batch

  }
private def getTodaysDate(): String ={
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
  val cal = Calendar.getInstance()
  cal.add(Calendar.DATE,0)
  dateFormat.format(cal.getTime())
}
}


