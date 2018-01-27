package com.nitendragautam.sparkbatchapp.main

import com.nitendragautam.sparkbatchapp.services.SparkServices

/**
*Main Entry point to the Application 
 */
object Boot {
  def main(args: Array[String]) {
    val inputFile = args(0) //Input File
    val outputFile = args(1) //Output File Location


val sr = new SparkServices
sr.startSparkBatchCluster(inputFile ,outputFile)

  }
}
