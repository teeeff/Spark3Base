spark-submit --master yarn --deploy-mode client --class SparkTest spark3check_2.12-0.1.jar
export SPARK_MAJOR_VERSION=3
spark-submit --master yarn --deploy-mode client --num-executors 4 --executor-cores 4 --executor-memory 1G --jars "/home/itv000118/stocktwits/mysql-connector-java-8.0.22.jar" --class SparkTest /home/itv000118/stocktwits/spark_output/spark3check_2.12-0.1.jar