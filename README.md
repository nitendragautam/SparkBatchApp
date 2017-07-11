#### Spark Submit in Script 

$SPARK_HOME/bin/spark-submit \
 --class com.nitendragautam.sparkbatchapp.main.Boot \
--master spark://192.168.133.128:7077 \
--deploy-mode cluster \
--supervise \
--executor-memory 4G \
--driver-memory 4G \
--total-executor-cores 1 \
/home/hduser/sparkbatchapp.jar \
/home/hduser/NDSBatchApp/input \
/home/hduser/NDSBatchApp/output/


