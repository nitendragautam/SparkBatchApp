package com.nitendragautam.sparkbatchapp.services


import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Spark Services
  */
class SparkServices extends Serializable{
val accessLogsParser = new AccessLogsParser
  private val logger: Logger =
    LoggerFactory.getLogger(classOf[SparkServices])

  def startSparkStreamingCluster(inputFile :String ,outPutFile :String) {
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
                     .filter(_._2 >5 ) //Client Ip address >10 times
                     .map(_._1) //Map the Client IP address count to
                     .take(100)

logger.info("IP address Length" +ipAddress.length)

    ipAddress.foreach(item =>{
      logger.info(" IP address which came more than 10 times "+item)
    })

sc.stop() //Stopping Spark batch
  }

}


