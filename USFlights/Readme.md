## Kafka, Stream Processing - US Flights (flink)



```
┌─────────────────┐    ┌─────────────────┐
│ Flights         │    │ Airports        │
│ CSV source      │    │ CSV source      │
└────────┬────────┘    └────────┬────────┘
         │                      │         
         │                      │         
         │                      │         
┌────────▼────────┐             │         
│ flight-records  │             │         
│ Kafka topic     │             │         
└────────┬────────┘             │         
         │                      │         
         │                      │         
         │                      │         
┌────────▼────────┐             │         
│ flights         │  map state  │         
│ DS<Flight>      ◄─────────────┘         
└────────┬────────┘                       
         │ 1. Separated by state          
         │ 2. Assigned to windows by date 
         │ 3. Transformed to CombinedDelay
┌────────▼────────┐    ┌─────────────────┐
│ aggregated      │    │ delay_etl_image │
│ DS<CombineDelay>├────► MySQL table     │
└─────────────────┘    └─────────────────┘
```

# Uruchomienie

## Inicjalizacja

### Uruchomienie klastra

```sh
gcloud dataproc clusters create ${CLUSTER_NAME} \
--enable-component-gateway --region ${REGION} --subnet default \
--master-machine-type n1-standard-4 --master-boot-disk-size 50 \
--num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 2.1-debian11 --optional-components FLINK,DOCKER,ZOOKEEPER \
--project ${PROJECT_ID} --max-age=2h \
--metadata "run-on-master=true" \
--initialization-actions \
gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
```



### Pobranie/Załadowanie danych:
Jeśli dane masz gdzie indziej, zmodyfikuj `flink.properties:fileInput.dir` oraz `flink.properties:airports.input`

```sh
## Załadowanie danych do bucketa
export DATA_DIR="./flights-data"
mkdir -p DATA_DIR
wget https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/airports.csv -P $DATA_DIR
wget https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/flights-2015.zip -P $DATA_DIR
unzip ${DATA_DIR}/flights-2015.zip -d ${DATA_DIR}/flights-2015
rm ${DATA_DIR}/flights-2015.zip
gsutil mv -r $DATA_DIR gs://pbd-24-ms/labs/flights-data/
# Można użyć gsutil cp aby od razu pozostawić dane w klastrze 
# gsutil cp -r $DATA_DIR gs://pbd-24-ms/labs/flights-data/

## Załadowanie danych z bucketa
export DATA_DIR="./flights-data"
mkdir -p $DATA_DIR
gsutil cp -r gs://pbd-24-ms/labs/flights-data/* $DATA_DIR
```

Dodanie praw do wykonania skryptów `*.sh`
```sh
find ./ -type f -iname "*.sh" -exec chmod +x {} ';'
```

Skrypt ustawiający/resetujący temat kafki: `./kafka-reset.sh`
Konfiguracja bazy danych do przechowania obrazów czasu rzeczywistego `./mysql.sh`
Uruchomienie producenta kafki: `./producer.sh`

Przy *Uruchomieniu konsumenta na klastrze GCP*, mam jakiś problem z widocznością pliku `airports.csv`, nie rozumiem dlaczego. Podejrzewam to że zadanie jest delegowane na osobną maszynę, na której nie ma dostępu do tego pliku. Dla lokalnego uruchomienia, działa mi poprawnie.

## Wyświetlenie zapisu obrazu czasu rzeczywistego

```sh
sudo docker exec -it mysql mysql -ustreamuser -pstream streamdb -e "SELECT * from delay_etl_image order by date desc limit 20;"
```


# Uruchomienie lokalne

Alternatywna metoda uruchomienia, której używałem podczas pisania kodu, z wykorzystaniem WSL.

Running Kafka locally, via WSL -- following: [Install and Run Kafka 3.2.0 On WSL (kontext.tech)](https://kontext.tech/article/1047/install-and-run-kafka-320-on-wsl#:~:text=Install%20and%20Run%20Kafka%203.2.0%20On%20WSL%201,the%20topic%20...%208%20Shutdown%20Kafka%20services%20)

```sh
sudo apt update
sudo apt install default-jre openjdk-11-jre-headless openjdk-8-jre-headless -y
java -version
echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc
wget https://archive.apache.org/dist/kafka/3.1.2/kafka_2.13-3.1.2.tgz
tar -xvzf kafka_2.13-3.1.2.tgz
echo "export KAFKA_HOME=~/kafka_2.13-3.1.2/" >> ~/.bashrc
source ~/.bashrc
## Start the zookeeper environment
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
## Start kafka server
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

Reset kafka topic and run producer
```sh
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic flight-records --bootstrap-server localhost:9092
$KAFKA_HOME/bin/kafka-topics.sh --create --topic flight-records --bootstrap-server localhost:9092

$KAFKA_HOME/bin/kafka-topics.sh --delete --topic flight-records --bootstrap-server localhost:9092
$KAFKA_HOME/bin/kafka-topics.sh --create --topic flight-records --bootstrap-server localhost:9092
java -cp USFlights.jar com.example.bigdata.TestProducer
```

```sh
## requires running Docker Desktop with WSL integration enabled
sudo docker run --name mysql -v /tmp/datadir:/var/lib/mysql -p 6033:3306 -e MYSQL_ROOT_PASSWORD=admin -d mysql:debian

sudo docker exec -it mysql mysql -uroot -padmin -e \
"CREATE USER 'streamuser'@'%' IDENTIFIED BY 'stream'; \
CREATE DATABASE IF NOT EXISTS streamdb CHARACTER SET utf8; \
GRANT ALL ON streamdb.* TO 'streamuser'@'%';"

sudo docker exec -it mysql mysql -ustreamuser -pstream streamdb -e \
"CREATE TABLE delay_etl_image ( \
    state VARCHAR(255) \
  , arrival_count INT \
  , departure_count INT \
  , arrival_delay INT \
  , departure_delay INT \
  , date VARCHAR(64) \
);"
```

Run producer
```sh
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic flight-records --bootstrap-server localhost:9092
$KAFKA_HOME/bin/kafka-topics.sh --create --topic flight-records --bootstrap-server localhost:9092
java -cp USFlights.jar com.example.bigdata.TestProducer
```


Run consumer (requires docker mysql container or disabling mysql sink in flink.properties)
```sh
java -cp USFlights.jar com.example.bigdata.USFlightsApp
```

Displaying real time etl image
```sh
sudo docker exec -it mysql mysql -ustreamuser -pstream streamdb -e "SELECT * from delay_etl_image order by date desc limit 20;"
```

Removing mysql container
```sh
sudo docker stop mysql
sudo docker rm mysql
```
