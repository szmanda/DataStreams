./kafka-reset.sh

docker exec -it mysql mysql -ustreamuser -pstream streamdb -e "delete from delay_etl_image;"

flink run -m yarn-cluster -p 4 -yjm 1024m -ytm 1024m \
  -c com.example.bigdata.TestProducer \
  ~/USFlights.jar