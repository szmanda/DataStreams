export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH=`hadoop classpath`
flink run -m yarn-cluster -p 1 -yjm 1024m -ytm 1024m \
  -c com.example.bigdata.USFlightsApp \
  ~/USFlights.jar